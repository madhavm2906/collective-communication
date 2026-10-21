/*
 * Demonstrates a complete TCP array request and response.
 *
 * Client sends [10, 20, 30, 40, 50].
 * Server validates the message, calculates the sum, and returns 150.
 *
 * All transmitted integers use network byte order.
 * A bounded element count prevents untrusted lengths from causing
 * excessive allocations.
 *
 * Usage:
 *   connection_demo server PORT [TIMEOUT_MS]
 *   connection_demo client PORT [TIMEOUT_MS]
 *
 * Server port zero requests an available port from Linux.
 * Each connection or transfer operation has its own timeout.
 */

#include "collective/tcp.hpp"

#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <netinet/in.h>

namespace {

constexpr std::uint32_t max_elements = 1024;
constexpr std::uint32_t max_element_value = 1000000;

unsigned long parse_number(
    const std::string& text,
    unsigned long minimum,
    unsigned long maximum) {
    if (text.empty() ||
        text.find_first_not_of("0123456789") != std::string::npos) {
        throw std::invalid_argument("argument must contain only digits");
    }

    const auto value = std::stoul(text);

    if (value < minimum || value > maximum) {
        throw std::invalid_argument("numeric argument is out of range");
    }

    return value;
}

void run_server(
    std::uint16_t port,
    std::chrono::milliseconds timeout) {
    auto listener = collective::listen_loopback(port);

    std::cout
        << "LISTENING 127.0.0.1:"
        << collective::local_port(listener)
        << std::endl;

    auto connection =
        collective::accept_connection(listener, timeout);

    std::uint32_t wire_count = 0;

    collective::receive_exact(
        connection, &wire_count, sizeof(wire_count), timeout);

    const std::uint32_t count = ntohl(wire_count);

    if (count == 0 || count > max_elements) {
        throw std::runtime_error("invalid element count");
    }

    std::vector<std::uint32_t> wire_values(count);

    collective::receive_exact(
        connection,
        wire_values.data(),
        wire_values.size() * sizeof(std::uint32_t),
        timeout);

    std::uint32_t sum = 0;

    for (const auto wire_value : wire_values) {
        const auto value = ntohl(wire_value);

        if (value > max_element_value) {
            throw std::runtime_error("element exceeds allowed value");
        }

        // The count and value limits keep the total below UINT32_MAX.
        sum += value;
    }

    const std::uint32_t wire_sum = htonl(sum);

    collective::send_all(
        connection, &wire_sum, sizeof(wire_sum), timeout);

    std::cout
        << "SERVER: received " << count
        << " values; sum=" << sum << '\n';
}

void run_client(
    std::uint16_t port,
    std::chrono::milliseconds timeout) {
    auto connection = collective::connect_loopback(port, timeout);

    // One contiguous request: count followed by five values.
    std::vector<std::uint32_t> request{5, 10, 20, 30, 40, 50};

    for (auto& value : request) {
        value = htonl(value);
    }

    collective::send_all(
        connection,
        request.data(),
        request.size() * sizeof(std::uint32_t),
        timeout);

    std::uint32_t wire_sum = 0;

    collective::receive_exact(
        connection, &wire_sum, sizeof(wire_sum), timeout);

    const auto sum = ntohl(wire_sum);

    if (sum != 150) {
        throw std::runtime_error("server returned an incorrect sum");
    }

    std::cout << "CLIENT: verified sum=" << sum << '\n';
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
        if (argc != 3 && argc != 4) {
            throw std::invalid_argument(
                "usage: connection_demo server|client PORT [TIMEOUT_MS]");
        }

        const std::string mode = argv[1];

        if (mode != "server" && mode != "client") {
            throw std::invalid_argument("mode must be server or client");
        }

        const auto port = static_cast<std::uint16_t>(
            parse_number(argv[2], mode == "server" ? 0UL : 1UL, 65535));

        const auto timeout_value =
            argc == 4 ? parse_number(argv[3], 1, 60000) : 10000UL;

        const auto timeout = std::chrono::milliseconds(
            static_cast<long long>(timeout_value));

        if (mode == "server") {
            run_server(port, timeout);
        } else {
            run_client(port, timeout);
        }

        return 0;
    } catch (const std::exception& error) {
        std::cerr << "ERROR: " << error.what() << '\n';
        return 1;
    }
}