/*
 * Runs one rank in a controlled all-reduce failure experiment.
 *
 * Normal ranks execute the real all-reduce and verify that an expected
 * network or protocol failure is reported.
 *
 * Fault modes:
 * - drop: abruptly exit after receiving the first collective header.
 * - stall: receive the header, then remain connected without participating.
 * - wrong-op: send a frame containing an incorrect operation ID.
 *
 * The Python launcher coordinates startup and releases a stalled worker.
 * Fault injection begins only after real collective traffic is observed.
 */

#include "collective/allreduce.hpp"
#include "collective/ring.hpp"
#include "collective/tcp.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <endian.h>

namespace {

using namespace std::chrono_literals;

constexpr std::size_t element_count = 4099;
constexpr std::uint64_t operation_id = 1;
constexpr std::uint64_t frame_magic = 0x43434131;

std::size_t parse_number(const std::string& text) {
    if (text.empty() ||
        text.find_first_not_of("0123456789") != std::string::npos) {
        throw std::invalid_argument("arguments must contain only digits");
    }

    const auto value = std::stoul(text);

    if (value > 64) {
        throw std::invalid_argument("numeric argument exceeds 64");
    }

    return static_cast<std::size_t>(value);
}

std::vector<std::uint16_t> read_ports(std::size_t world_size) {
    std::vector<std::uint16_t> ports;
    ports.reserve(world_size);

    for (std::size_t index = 0; index < world_size; ++index) {
        unsigned int port = 0;

        if (!(std::cin >> port) || port == 0 || port > 65535) {
            throw std::runtime_error("invalid port configuration");
        }

        ports.push_back(static_cast<std::uint16_t>(port));
    }

    return ports;
}

bool expected_network_failure(const std::system_error& error) {
    const auto code = error.code();

    return code == std::errc::timed_out ||
           code == std::errc::connection_reset ||
           code == std::errc::connection_aborted ||
           code == std::errc::broken_pipe ||
           code == std::errc::not_connected;
}

int run_normal(const collective::Ring& ring) {
    std::vector<double> values(
        element_count,
        static_cast<double>(ring.rank + 1));

    try {
        collective::allreduce_sum(
            ring,
            values,
            operation_id,
            1000ms);
    } catch (const std::system_error& error) {
        if (!expected_network_failure(error)) {
            throw;
        }

        std::cout
            << "EXPECTED_FAILURE rank=" << ring.rank
            << " message=" << error.what() << '\n';

        return 0;
    } catch (const std::runtime_error& error) {
        const std::string message = error.what();

        if (message != "peer closed connection before transfer completed" &&
            message != "allreduce frame metadata mismatch") {
            throw;
        }

        std::cout
            << "EXPECTED_FAILURE rank=" << ring.rank
            << " message=" << message << '\n';

        return 0;
    }

    throw std::runtime_error(
        "collective unexpectedly succeeded despite an injected fault");
}

int run_fault(const collective::Ring& ring, const std::string& mode) {
    // Matches the current all-reduce wire header: seven 64-bit fields.
    std::array<std::uint64_t, 7> observed_header{};

    collective::receive_exact(
        ring.incoming,
        observed_header.data(),
        sizeof(observed_header),
        5000ms);

    std::cout
        << "FAULT_OBSERVED rank=" << ring.rank
        << std::endl;

    if (mode == "drop") {
        // Simulate an abrupt process failure. The operating system closes
        // descriptors; C++ destructors intentionally do not run.
        std::_Exit(42);
    }

    if (mode == "stall") {
        // Keep connections open while normal ranks reach their deadlines.
        // The parent releases this worker after checking their results.
        std::string command;

        if (!(std::cin >> command) || command != "RELEASE") {
            throw std::runtime_error("missing stall-release command");
        }

        return 43;
    }

    const auto chunk_count =
        element_count / ring.world_size +
        (ring.rank < element_count % ring.world_size ? 1U : 0U);

    // A plausible first-round frame with a deliberately wrong operation ID.
    std::array<std::uint64_t, 7> frame{
        frame_magic,
        operation_id + 99,
        1,
        0,
        static_cast<std::uint64_t>(ring.rank),
        static_cast<std::uint64_t>(element_count),
        static_cast<std::uint64_t>(chunk_count),
    };

    for (auto& field : frame) {
        field = htobe64(field);
    }

    collective::send_all(
        ring.outgoing,
        frame.data(),
        sizeof(frame),
        5000ms);

    return 44;
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
        if (argc != 4) {
            throw std::invalid_argument(
                "usage: allreduce_failure_worker RANK WORLD_SIZE MODE");
        }

        const auto rank = parse_number(argv[1]);
        const auto world_size = parse_number(argv[2]);
        const std::string mode = argv[3];

        if (world_size < 2 || rank >= world_size) {
            throw std::invalid_argument("invalid failure-test group");
        }

        if (mode != "normal" &&
            mode != "drop" &&
            mode != "stall" &&
            mode != "wrong-op") {
            throw std::invalid_argument("unknown fault mode");
        }

        auto listener = collective::listen_loopback(0);

        std::cout
            << "READY " << rank << ' '
            << collective::local_port(listener)
            << std::endl;

        const auto ports = read_ports(world_size);

        auto ring = collective::connect_ring(
            std::move(listener),
            rank,
            ports,
            5000ms);

        std::cout << "CONNECTED " << rank << std::endl;

        std::string command;

        if (!(std::cin >> command) || command != "GO") {
            throw std::runtime_error("missing collective-start command");
        }

        if (mode == "normal") {
            return run_normal(ring);
        }

        return run_fault(ring, mode);
    } catch (const std::exception& error) {
        std::cerr << "ERROR: " << error.what() << '\n';
        return 1;
    }
}