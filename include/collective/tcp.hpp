/*
 * Declares deadline-bounded TCP connection operations for IPv4 loopback.
 *
 * listen_loopback creates a nonblocking listening socket.
 * accept_connection accepts one incoming connection.
 * connect_loopback establishes an outgoing connection.
 *
 * Returned connected sockets are nonblocking.
 * Timeouts must be between 1 and 60000 milliseconds.
 * This first version supports connections within one Linux environment.
 */

#pragma once

#include "collective/socket.hpp"

#include <chrono>
#include <cstdint>

namespace collective {

[[nodiscard]] Socket listen_loopback(std::uint16_t port);

[[nodiscard]] Socket accept_connection(
    const Socket& listener,
    std::chrono::milliseconds timeout);

[[nodiscard]] Socket connect_loopback(
    std::uint16_t port,
    std::chrono::milliseconds timeout);

}  // namespace collective