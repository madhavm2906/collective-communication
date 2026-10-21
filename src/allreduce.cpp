/*
 * Implements ring all-reduce for IEEE-754 binary64 values.
 *
 * Reduce-scatter adds incoming chunks to local data.
 * All-gather copies completed chunks into their final positions.
 *
 * A single-threaded poll loop advances sending and receiving together.
 * This avoids creating a sender task for every exchange round while
 * preserving progress when outgoing socket buffers fill.
 *
 * Frames validate operation, phase, round, chunk, and length metadata.
 * Scratch buffers are allocated once per collective and reused.
 * Floating-point bit patterns are encoded in big-endian byte order.
 */

#include "collective/allreduce.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <vector>

#include <endian.h>
#include <poll.h>
#include <sys/socket.h>

namespace collective {
namespace {

static_assert(sizeof(double) == sizeof(std::uint64_t));
static_assert(std::numeric_limits<double>::is_iec559);

using Clock = std::chrono::steady_clock;
using Header = std::array<std::uint64_t, 7>;

static_assert(sizeof(Header) == 7 * sizeof(std::uint64_t));

constexpr std::uint64_t frame_magic = 0x43434131;
constexpr std::uint64_t reduce_scatter_phase = 1;
constexpr std::uint64_t all_gather_phase = 2;

constexpr std::size_t max_elements = 16 * 1024 * 1024;
constexpr std::size_t max_syscall_bytes = 1024 * 1024;

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

int remaining_milliseconds(Clock::time_point deadline) {
    const auto now = Clock::now();

    if (now >= deadline) {
        throw std::system_error(
            std::make_error_code(std::errc::timed_out),
            "allreduce round deadline expired");
    }

    const auto remaining =
        std::chrono::ceil<std::chrono::milliseconds>(deadline - now);

    return static_cast<int>(remaining.count());
}

bool retryable(int error) {
    return error == EINTR ||
           error == EAGAIN ||
           error == EWOULDBLOCK;
}

std::size_t try_send(
    int fd,
    const unsigned char* bytes,
    std::size_t remaining) {
    const auto request = std::min(remaining, max_syscall_bytes);

    const auto result = ::send(
        fd,
        bytes,
        request,
        MSG_DONTWAIT | MSG_NOSIGNAL);

    if (result > 0) {
        return static_cast<std::size_t>(result);
    }

    if (result == 0) {
        throw std::runtime_error("send made no progress");
    }

    const int error = errno;

    if (retryable(error)) {
        return 0;
    }

    throw std::system_error(
        error,
        std::generic_category(),
        "sending allreduce frame");
}

std::size_t try_receive(
    int fd,
    unsigned char* bytes,
    std::size_t remaining) {
    const auto request = std::min(remaining, max_syscall_bytes);

    const auto result = ::recv(
        fd,
        bytes,
        request,
        MSG_DONTWAIT);

    if (result > 0) {
        return static_cast<std::size_t>(result);
    }

    if (result == 0) {
        throw std::runtime_error(
            "peer closed connection before transfer completed");
    }

    const int error = errno;

    if (retryable(error)) {
        return 0;
    }

    throw std::system_error(
        error,
        std::generic_category(),
        "receiving allreduce frame");
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

        const auto expected_header = make_header(
            phase,
            step,
            receive_chunk_index,
            receive_chunk.count);

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

        const auto incoming_payload_bytes =
            receive_chunk.count * sizeof(std::uint64_t);

        const auto* outgoing_data =
            reinterpret_cast<const unsigned char*>(send_buffer_.data());

        Header incoming_header{};

        auto* incoming_header_data =
            reinterpret_cast<unsigned char*>(incoming_header.data());

        std::size_t sent = 0;
        std::size_t header_received = 0;
        std::size_t payload_received = 0;
        bool header_checked = false;

        const auto deadline = Clock::now() + timeout_;

        for (;;) {
            const bool send_complete = sent == outgoing_bytes;

            const bool receive_complete =
                header_checked &&
                payload_received == incoming_payload_bytes;

            if (send_complete && receive_complete) {
                break;
            }

            std::array<pollfd, 2> descriptors{};

            // Completed directions are disabled rather than repeatedly
            // polling a writable socket with nothing left to send.
            descriptors[0].fd =
                send_complete ? -1 : ring_.outgoing.native_handle();
            descriptors[0].events = POLLOUT;

            descriptors[1].fd =
                receive_complete ? -1 : ring_.incoming.native_handle();
            descriptors[1].events = POLLIN;

            const int result = ::poll(
                descriptors.data(),
                static_cast<nfds_t>(descriptors.size()),
                remaining_milliseconds(deadline));

            if (result == -1) {
                const int error = errno;

                if (error == EINTR) {
                    continue;
                }

                throw std::system_error(
                    error,
                    std::generic_category(),
                    "polling allreduce connections");
            }

            if (result == 0) {
                continue;
            }

            for (const auto& descriptor : descriptors) {
                if ((descriptor.revents & POLLNVAL) != 0) {
                    throw std::system_error(
                        std::make_error_code(std::errc::bad_file_descriptor),
                        "invalid allreduce socket");
                }
            }

            // Advance each direction by at most one bounded syscall.
            // Neither direction must finish before the other progresses.
            if (!send_complete &&
                (descriptors[0].revents &
                 (POLLOUT | POLLERR | POLLHUP)) != 0) {
                sent += try_send(
                    ring_.outgoing.native_handle(),
                    outgoing_data + sent,
                    outgoing_bytes - sent);
            }

            if (!receive_complete &&
                (descriptors[1].revents &
                 (POLLIN | POLLERR | POLLHUP)) != 0) {
                if (!header_checked) {
                    header_received += try_receive(
                        ring_.incoming.native_handle(),
                        incoming_header_data + header_received,
                        sizeof(incoming_header) - header_received);

                    if (header_received == sizeof(incoming_header)) {
                        for (auto& field : incoming_header) {
                            field = be64toh(field);
                        }

                        if (incoming_header != expected_header) {
                            throw std::runtime_error(
                                "allreduce frame metadata mismatch");
                        }

                        header_checked = true;
                    }
                } else if (payload_received < incoming_payload_bytes) {
                    auto* incoming_payload_data =
                        reinterpret_cast<unsigned char*>(
                            receive_buffer_.data());

                    payload_received += try_receive(
                        ring_.incoming.native_handle(),
                        incoming_payload_data + payload_received,
                        incoming_payload_bytes - payload_received);
                }
            }
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

        for (std::size_t step = 0; step < world - 1; ++step) {
            const auto send_chunk = (rank + world - step) % world;
            const auto receive_chunk = (rank + world - step - 1) % world;

            reduction.exchange(
                reduce_scatter_phase,
                step,
                send_chunk,
                receive_chunk);
        }

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