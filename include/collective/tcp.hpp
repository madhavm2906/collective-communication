/*
 * Declares deadline-bounded IPv4 loopback connections and byte transfers.
 *
 * send_all sends the entire supplied buffer or throws.
 * receive_exact fills the entire supplied buffer or throws.
 *
 * Each operation has its own timeout, between 1 and 60000 milliseconds.
 * Transfer progress does not restart that operation's timeout.
 *
 * These functions do not provide simultaneous bidirectional transfers.
 * The caller must keep each Socket alive for the operation's duration.
 */

#pragma once

#include "collective/socket.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace collective {

[[nodiscard]] Socket listen_loopback(std::uint16_t port);

// Return the local port, including a port assigned by binding to zero.
[[nodiscard]] std::uint16_t local_port(const Socket& socket);

[[nodiscard]] Socket accept_connection(
    const Socket& listener,
    std::chrono::milliseconds timeout);

[[nodiscard]] Socket connect_loopback(
    std::uint16_t port,
    std::chrono::milliseconds timeout);

// A null buffer is allowed only when byte_count is zero.
// Successful sending means the local socket accepted the bytes;
// an application response is needed to confirm peer processing.
void send_all(
    const Socket& socket,
    const void* buffer,
    std::size_t byte_count,
    std::chrono::milliseconds timeout);

// An early peer disconnect is an error, not a successful short read.
void receive_exact(
    const Socket& socket,
    void* buffer,
    std::size_t byte_count,
    std::chrono::milliseconds timeout);

}  // namespace collective