/*
 * Implements ring all-reduce for IEEE-754 binary64 values.
 *
 * Reduce-scatter adds incoming chunks to local data.
 * All-gather copies completed chunks into their final positions.
 *
 * Each round sends concurrently with receiving, avoiding a ring in which
 * every process blocks while trying to send a large payload.
 *
 * Frames include operation, phase, round, chunk, and length metadata.
 * Metadata is checked before receiving the corresponding payload.
 *
 * Scratch buffers are allocated once per collective and reused across rounds.
 * Doubles are encoded as big-endian 64-bit bit patterns using memcpy.
 */

#include "collective/allreduce.hpp"
#include "collective/tcp.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <future>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <vector>

#include <endian.h>
#include <sys/socket.h>

namespace collective {
namespace {

static_assert(sizeof(double) == sizeof(std::uint64_t));
static_assert(std::numeric_limits<double>::is_iec559);

using Clock = std::chrono::steady_clock;
using Header = std::array<std::uint64_t, 7>;

constexpr std::uint64_t frame_magic = 0x43434131;
constexpr std::uint64_t reduce_scatter_phase = 1;
constexpr std::uint64_t all_gather_phase = 2;
constexpr std::size_t max_elements = 16 * 1024 * 1024;

struct Chunk {
    std::size_t offset;
    std::size_t count;
};

Chunk partition(
    std::size_t element_count,
    std::size_t world_size,
    std::size_t chunk_index) {
    const auto base = element_count / world_size;
    const auto remainder = element_count % world_size;

    return Chunk{
        chunk_index * base + std::min(chunk_index, remainder),
        base + (chunk_index < remainder ? 1U : 0U),
    };
}

std::uint64_t encode_double(double value) {
    std::uint64_t bits = 0;
    std::memcpy(&bits, &value, sizeof(bits));
    return htobe64(bits);
}

double decode_double(std::uint64_t wire_value) {
    const std::uint64_t bits = be64toh(wire_value);
    double value = 0.0;

    std::memcpy(&value, &bits, sizeof(value));
    return value;
}

void abort_ring(const Ring& ring) noexcept {
    if (ring.incoming.valid()) {
        (void)::shutdown(ring.incoming.native_handle(), SHUT_RDWR);
    }

    if (ring.outgoing.valid()) {
        (void)::shutdown(ring.outgoing.native_handle(), SHUT_RDWR);
    }
}

std::chrono::milliseconds remaining_timeout(Clock::time_point deadline) {
    const auto now = Clock::now();

    if (now >= deadline) {
        throw std::system_error(
            std::make_error_code(std::errc::timed_out),
            "allreduce round deadline expired");
    }

    return std::chrono::ceil<std::chrono::milliseconds>(deadline - now);
}

class Reduction {
public:
    Reduction(
        const Ring& ring,
        std::vector<double>& values,
        std::uint64_t operation_id,
        std::chrono::milliseconds timeout)
        : ring_(ring),
          values_(values),
          operation_id_(operation_id),
          timeout_(timeout),
          send_buffer_(
              Header{}.size() +
              partition(values.size(), ring.world_size, 0).count),
          receive_buffer_(
              partition(values.size(), ring.world_size, 0).count) {}

    void exchange(
        std::uint64_t phase,
        std::size_t step,
        std::size_t send_chunk_index,
        std::size_t receive_chunk_index) {
        const auto send_chunk =
            partition(values_.size(), ring_.world_size, send_chunk_index);

        const auto receive_chunk =
            partition(values_.size(), ring_.world_size, receive_chunk_index);

        const auto outgoing_header = make_header(
            phase,
            step,
            send_chunk_index,
            send_chunk.count);

        for (std::size_t index = 0; index < outgoing_header.size(); ++index) {
            send_buffer_[index] = htobe64(outgoing_header[index]);
        }

        const auto header_words = outgoing_header.size();

        for (std::size_t index = 0; index < send_chunk.count; ++index) {
            send_buffer_[header_words + index] =
                encode_double(values_[send_chunk.offset + index]);
        }

        const auto outgoing_bytes =
            (header_words + send_chunk.count) * sizeof(std::uint64_t);

        const auto deadline = Clock::now() + timeout_;

        // Snapshot data is already in send_buffer_, so receiving and updating
        // values_ cannot race with the sender's reads.
        auto sender = std::async(
            std::launch::async,
            [this, outgoing_bytes, deadline] {
                send_all(
                    ring_.outgoing,
                    send_buffer_.data(),
                    outgoing_bytes,
                    remaining_timeout(deadline));
            });

        try {
            Header incoming_header{};

            receive_exact(
                ring_.incoming,
                incoming_header.data(),
                sizeof(incoming_header),
                remaining_timeout(deadline));

            for (auto& field : incoming_header) {
                field = be64toh(field);
            }

            const auto expected_header = make_header(
                phase,
                step,
                receive_chunk_index,
                receive_chunk.count);

            if (incoming_header != expected_header) {
                throw std::runtime_error(
                    "allreduce frame metadata mismatch");
            }

            // The receive size comes from our validated local partition,
            // never directly from an untrusted received length.
            receive_exact(
                ring_.incoming,
                receive_buffer_.data(),
                receive_chunk.count * sizeof(std::uint64_t),
                remaining_timeout(deadline));

            // Finish sending before reusing the scratch buffer.
            sender.get();
        } catch (...) {
            const auto failure = std::current_exception();

            // Wake a sender that may be waiting for buffer space.
            abort_ring(ring_);

            if (sender.valid()) {
                try {
                    sender.get();
                } catch (...) {
                    // Preserve the original failure.
                }
            }

            std::rethrow_exception(failure);
        }

        for (std::size_t index = 0; index < receive_chunk.count; ++index) {
            const auto value = decode_double(receive_buffer_[index]);
            auto& destination = values_[receive_chunk.offset + index];

            if (phase == reduce_scatter_phase) {
                destination += value;
            } else {
                destination = value;
            }
        }
    }

private:
    Header make_header(
        std::uint64_t phase,
        std::size_t step,
        std::size_t chunk_index,
        std::size_t chunk_count) const {
        return Header{
            frame_magic,
            operation_id_,
            phase,
            static_cast<std::uint64_t>(step),
            static_cast<std::uint64_t>(chunk_index),
            static_cast<std::uint64_t>(values_.size()),
            static_cast<std::uint64_t>(chunk_count),
        };
    }

    const Ring& ring_;
    std::vector<double>& values_;
    std::uint64_t operation_id_;
    std::chrono::milliseconds timeout_;

    std::vector<std::uint64_t> send_buffer_;
    std::vector<std::uint64_t> receive_buffer_;
};

}  // namespace

void allreduce_sum(
    const Ring& ring,
    std::vector<double>& values,
    std::uint64_t operation_id,
    std::chrono::milliseconds round_timeout) {
    if (ring.world_size == 0 ||
        ring.world_size > 64 ||
        ring.rank >= ring.world_size) {
        throw std::invalid_argument("invalid ring configuration");
    }

    if (values.size() > max_elements) {
        throw std::invalid_argument("allreduce input exceeds element limit");
    }

    if (round_timeout.count() <= 0 || round_timeout.count() > 60000) {
        throw std::invalid_argument(
            "round timeout must be between 1 and 60000 milliseconds");
    }

    if (ring.world_size == 1) {
        return;
    }

    if (!ring.incoming.valid() || !ring.outgoing.valid()) {
        throw std::invalid_argument("ring connections are missing");
    }

    try {
        Reduction reduction(ring, values, operation_id, round_timeout);

        const auto rank = ring.rank;
        const auto world = ring.world_size;

        // After these rounds, rank r owns completed chunk (r + 1) % world.
        for (std::size_t step = 0; step < world - 1; ++step) {
            const auto send_chunk = (rank + world - step) % world;
            const auto receive_chunk = (rank + world - step - 1) % world;

            reduction.exchange(
                reduce_scatter_phase,
                step,
                send_chunk,
                receive_chunk);
        }

        // Circulate completed chunks until every rank has every chunk.
        for (std::size_t step = 0; step < world - 1; ++step) {
            const auto send_chunk = (rank + 1 + world - step) % world;
            const auto receive_chunk = (rank + world - step) % world;

            reduction.exchange(
                all_gather_phase,
                step,
                send_chunk,
                receive_chunk);
        }
    } catch (...) {
        abort_ring(ring);
        throw;
    }
}

}  // namespace collective