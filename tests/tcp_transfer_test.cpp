/*
 * Exercises the TCP transport beyond small request/response messages.
 *
 * Uses real TCP connections on dynamically assigned loopback ports.
 * Checks large transfers, empty transfers, peer disconnects,
 * send-side backpressure, and deadlines under slow incoming traffic.
 *
 * Background tasks have bounded network operations and are joined
 * before their sockets and buffers leave scope.
 */

#include "collective/tcp.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <sys/socket.h>

namespace {

using namespace std::chrono_literals;

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

template <typename Function>
void require_timeout(Function function, const std::string& name) {
    try {
        function();
    } catch (const std::system_error& error) {
        if (error.code() == std::errc::timed_out) {
            return;
        }

        throw std::runtime_error(
            name + ": unexpected system error: " + error.what());
    }

    throw std::runtime_error(name + ": expected a timeout");
}

// The listener stays bound until both connection ends exist.
// Port zero lets Linux select an available port without a lookup race.
std::pair<collective::Socket, collective::Socket> make_connection() {
    auto listener = collective::listen_loopback(0);
    const auto port = collective::local_port(listener);

    auto client = collective::connect_loopback(port, 5000ms);
    auto server = collective::accept_connection(listener, 5000ms);

    return {std::move(client), std::move(server)};
}

void set_buffer_size(
    const collective::Socket& socket,
    int option,
    int size) {
    const int result = ::setsockopt(
        socket.native_handle(),
        SOL_SOCKET,
        option,
        &size,
        sizeof(size));

    if (result == -1) {
        throw std::runtime_error("could not set test socket buffer size");
    }
}

void test_large_transfer() {
    auto connection = make_connection();

    // A small send buffer encourages partial writes and waiting.
    set_buffer_size(connection.first, SO_SNDBUF, 32768);

    constexpr std::size_t payload_size = 4 * 1024 * 1024 + 13;
    std::vector<std::uint8_t> payload(payload_size);

    for (std::size_t index = 0; index < payload.size(); ++index) {
        payload[index] = static_cast<std::uint8_t>(
            (index % 251 + (index / 251) % 251) % 251);
    }

    // Read concurrently so the sender can progress once buffers fill.
    auto receiver = std::async(std::launch::async, [&] {
        std::vector<std::uint8_t> received(payload.size());

        collective::receive_exact(
            connection.second,
            received.data(),
            received.size(),
            10000ms);

        require(received == payload, "large transfer: payload mismatch");
    });

    collective::send_all(
        connection.first,
        payload.data(),
        payload.size(),
        10000ms);

    // Propagate any exception from the receiving task.
    receiver.get();
}

void test_zero_length_transfers() {
    auto connection = make_connection();

    collective::send_all(
        connection.first,
        nullptr,
        0,
        1000ms);

    collective::receive_exact(
        connection.second,
        nullptr,
        0,
        1000ms);
}

void test_peer_disconnect() {
    auto connection = make_connection();

    // Closing this endpoint sends an orderly shutdown to its peer.
    connection.second.reset();

    std::uint8_t byte = 0;

    try {
        collective::receive_exact(
            connection.first,
            &byte,
            sizeof(byte),
            1000ms);
    } catch (const std::runtime_error& error) {
        require(
            std::string(error.what()).find(
                "peer closed connection before transfer completed") !=
                std::string::npos,
            "disconnect: unexpected error");

        return;
    }

    throw std::runtime_error("disconnect: expected a receive failure");
}

void test_send_backpressure_timeout() {
    auto connection = make_connection();

    set_buffer_size(connection.first, SO_SNDBUF, 4096);
    set_buffer_size(connection.second, SO_RCVBUF, 4096);

    const std::vector<std::uint8_t> payload(
        4 * 1024 * 1024,
        static_cast<std::uint8_t>(42));

    // Keep the receiving socket open, but deliberately never read.
    // The payload cannot fit in these socket buffers.
    require_timeout(
        [&] {
            collective::send_all(
                connection.first,
                payload.data(),
                payload.size(),
                200ms);
        },
        "send backpressure");
}

void test_receive_deadline_does_not_restart() {
    auto connection = make_connection();

    constexpr std::size_t byte_count = 32;

    auto sender = std::async(std::launch::async, [&] {
        for (std::size_t index = 0; index < byte_count; ++index) {
            std::this_thread::sleep_for(20ms);

            const auto byte = static_cast<std::uint8_t>(index);

            collective::send_all(
                connection.first,
                &byte,
                sizeof(byte),
                1000ms);
        }
    });

    std::vector<std::uint8_t> received(byte_count);

    // Receiving all bytes takes at least about 640 ms.
    // A 150 ms operation deadline must expire even with partial progress.
    require_timeout(
        [&] {
            collective::receive_exact(
                connection.second,
                received.data(),
                received.size(),
                150ms);
        },
        "receive deadline");

    // Keep both endpoints alive while the bounded sender finishes.
    sender.get();
}

}  // namespace

int main() {
    try {
        test_large_transfer();
        std::cout << "PASS: large transfer preserves every byte\n";

        test_zero_length_transfers();
        std::cout << "PASS: zero-length transfers\n";

        test_peer_disconnect();
        std::cout << "PASS: peer disconnect is reported\n";

        test_send_backpressure_timeout();
        std::cout << "PASS: blocked sender reaches its deadline\n";

        test_receive_deadline_does_not_restart();
        std::cout << "PASS: partial progress does not restart deadline\n";

        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}