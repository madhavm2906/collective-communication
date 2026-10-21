/*
 * Builds a ring from pre-existing loopback listeners.
 *
 * Every rank connects to its successor and accepts its predecessor.
 * A network-order handshake validates the predecessor's identity.
 *
 * TCP_NODELAY is enabled on both connected sockets to disable Nagle
 * buffering for the ring's latency-sensitive messages.
 *
 * This setting is being evaluated against the recorded baseline.
 * It does not change the ring algorithm or its wire protocol.
 */

#include "collective/ring.hpp"
#include "collective/tcp.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

namespace collective {
namespace {

constexpr std::uint32_t protocol_magic = 0x43434C31;
constexpr std::uint32_t protocol_version = 1;
constexpr std::size_t max_world_size = 64;

void validate_configuration(
    const Socket& listener,
    std::size_t rank,
    const std::vector<std::uint16_t>& ports,
    std::chrono::milliseconds timeout) {
    if (ports.empty() || ports.size() > max_world_size) {
        throw std::invalid_argument(
            "ring requires between 1 and 64 ranks");
    }

    if (rank >= ports.size()) {
        throw std::invalid_argument("rank is outside the process group");
    }

    if (timeout.count() <= 0 || timeout.count() > 60000) {
        throw std::invalid_argument(
            "timeout must be between 1 and 60000 milliseconds");
    }

    if (std::find(ports.begin(), ports.end(), 0) != ports.end()) {
        throw std::invalid_argument("ring ports must be nonzero");
    }

    auto sorted_ports = ports;
    std::sort(sorted_ports.begin(), sorted_ports.end());

    if (std::adjacent_find(
            sorted_ports.begin(),
            sorted_ports.end()) != sorted_ports.end()) {
        throw std::invalid_argument("ring ports must be unique");
    }

    if (local_port(listener) != ports[rank]) {
        throw std::invalid_argument(
            "listener port does not match this rank's configuration");
    }
}

void enable_tcp_nodelay(const Socket& socket) {
    const int enabled = 1;

    if (::setsockopt(
            socket.native_handle(),
            IPPROTO_TCP,
            TCP_NODELAY,
            &enabled,
            sizeof(enabled)) == -1) {
        const int error = errno;

        throw std::system_error(
            error,
            std::generic_category(),
            "enabling TCP_NODELAY on ring connection");
    }
}

void exchange_identity(
    const Socket& incoming,
    const Socket& outgoing,
    std::size_t rank,
    std::size_t world_size,
    std::chrono::milliseconds timeout) {
    const std::array<std::uint32_t, 4> identity{
        htonl(protocol_magic),
        htonl(protocol_version),
        htonl(static_cast<std::uint32_t>(rank)),
        htonl(static_cast<std::uint32_t>(world_size)),
    };

    // Bulk ring traffic uses concurrent sending and receiving.
    // This handshake sends only one small, fixed-size identity.
    send_all(
        outgoing,
        identity.data(),
        sizeof(identity),
        timeout);

    std::array<std::uint32_t, 4> peer_identity{};

    receive_exact(
        incoming,
        peer_identity.data(),
        sizeof(peer_identity),
        timeout);

    for (auto& field : peer_identity) {
        field = ntohl(field);
    }

    const auto previous_rank =
        (rank + world_size - 1) % world_size;

    if (peer_identity[0] != protocol_magic) {
        throw std::runtime_error("ring handshake: invalid protocol magic");
    }

    if (peer_identity[1] != protocol_version) {
        throw std::runtime_error("ring handshake: unsupported version");
    }

    if (peer_identity[2] !=
        static_cast<std::uint32_t>(previous_rank)) {
        throw std::runtime_error("ring handshake: unexpected predecessor");
    }

    if (peer_identity[3] !=
        static_cast<std::uint32_t>(world_size)) {
        throw std::runtime_error("ring handshake: group size mismatch");
    }
}

}  // namespace

Ring connect_ring(
    Socket listener,
    std::size_t rank,
    const std::vector<std::uint16_t>& ports,
    std::chrono::milliseconds timeout) {
    validate_configuration(listener, rank, ports, timeout);

    const std::size_t world_size = ports.size();

    if (world_size == 1) {
        return Ring{rank, world_size, Socket{}, Socket{}};
    }

    const auto next_rank = (rank + 1) % world_size;

    auto outgoing = connect_loopback(ports[next_rank], timeout);
    auto incoming = accept_connection(listener, timeout);

    // Configure both connections explicitly before exchanging ring traffic.
    enable_tcp_nodelay(outgoing);
    enable_tcp_nodelay(incoming);

    exchange_identity(
        incoming,
        outgoing,
        rank,
        world_size,
        timeout);

    return Ring{
        rank,
        world_size,
        std::move(incoming),
        std::move(outgoing),
    };
}

}  // namespace collective