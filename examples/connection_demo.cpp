/*
 * Runs as either a TCP server or a TCP client.
 *
 * Start the server first, then start the client in another terminal.
 * Both use IPv4 loopback and a ten-second connection deadline.
 * This example establishes a connection but does not exchange payloads.
 */

#include "collective/tcp.hpp"

#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {

std::uint16_t parse_port(const std::string& text) {
    if (text.empty() ||
        text.find_first_not_of("0123456789") != std::string::npos) {
        throw std::invalid_argument("port must contain only digits");
    }

    std::size_t consumed = 0;
    const unsigned long value = std::stoul(text, &consumed);

    if (consumed != text.size() || value == 0 || value > 65535) {
        throw std::invalid_argument("port must be between 1 and 65535");
    }

    return static_cast<std::uint16_t>(value);
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
        if (argc != 3) {
            throw std::invalid_argument(
                "usage: connection_demo server|client PORT");
        }

        const std::string mode = argv[1];
        const auto port = parse_port(argv[2]);
        const auto timeout = std::chrono::milliseconds(10000);

        if (mode == "server") {
            auto listener = collective::listen_loopback(port);

            // Flush immediately so the user knows when to start the client.
            std::cout
                << "LISTENING 127.0.0.1:" << port << std::endl;

            auto connection =
                collective::accept_connection(listener, timeout);

            std::cout << "SERVER: accepted connection\n";
        } else if (mode == "client") {
            auto connection =
                collective::connect_loopback(port, timeout);

            std::cout << "CLIENT: connected\n";
        } else {
            throw std::invalid_argument(
                "mode must be server or client");
        }

        return 0;
    } catch (const std::exception& error) {
        std::cerr << "ERROR: " << error.what() << '\n';
        return 1;
    }
}