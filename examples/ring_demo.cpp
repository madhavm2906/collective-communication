/*
 * Starts one rank, joins a TCP ring, and participates in a token check.
 *
 * Usage:
 *   ring_demo RANK WORLD_SIZE
 *
 * Startup protocol:
 *   1. Print "READY RANK PORT" after opening a listener.
 *   2. Read WORLD_SIZE listening ports from standard input.
 *   3. Connect to the next rank and validate the previous rank.
 *
 * Rank zero sends a token around the ring.
 * Every other rank increments and forwards it.
 * Rank zero validates the final value.
 */

#include "collective/ring.hpp"
#include "collective/tcp.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <netinet/in.h>

namespace {

using namespace std::chrono_literals;

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
            throw std::runtime_error("invalid listening-port configuration");
        }

        ports.push_back(static_cast<std::uint16_t>(port));
    }

    return ports;
}

void verify_token(const collective::Ring& ring) {
    if (ring.world_size == 1) {
        return;
    }

    constexpr std::uint32_t initial_token = 12345;
    std::uint32_t wire_token = 0;

    if (ring.rank == 0) {
        wire_token = htonl(initial_token);

        collective::send_all(
            ring.outgoing,
            &wire_token,
            sizeof(wire_token),
            5000ms);

        collective::receive_exact(
            ring.incoming,
            &wire_token,
            sizeof(wire_token),
            5000ms);

        const auto expected =
            initial_token +
            static_cast<std::uint32_t>(ring.world_size - 1);

        if (ntohl(wire_token) != expected) {
            throw std::runtime_error("ring token has an incorrect value");
        }
    } else {
        collective::receive_exact(
            ring.incoming,
            &wire_token,
            sizeof(wire_token),
            5000ms);

        wire_token = htonl(ntohl(wire_token) + 1);

        collective::send_all(
            ring.outgoing,
            &wire_token,
            sizeof(wire_token),
            5000ms);
    }
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
        if (argc != 3) {
            throw std::invalid_argument(
                "usage: ring_demo RANK WORLD_SIZE");
        }

        const auto rank = parse_number(argv[1]);
        const auto world_size = parse_number(argv[2]);

        if (world_size == 0 || rank >= world_size) {
            throw std::invalid_argument("invalid rank or group size");
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

        verify_token(ring);

        std::cout
            << "RANK " << rank
            << ": PASS world=" << world_size << '\n';

        return 0;
    } catch (const std::exception& error) {
        std::cerr << "ERROR: " << error.what() << '\n';
        return 1;
    }
}