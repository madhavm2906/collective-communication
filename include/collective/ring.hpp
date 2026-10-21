/*
 * Declares a fixed-membership ring of local TCP-connected processes.
 *
 * Each rank owns:
 * - an incoming connection from the previous rank;
 * - an outgoing connection to the next rank.
 *
 * Socket ownership makes Ring movable but noncopyable.
 * A one-rank ring needs no peer connections.
 */

#pragma once

#include "collective/socket.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace collective {

struct Ring {
    std::size_t rank;
    std::size_t world_size;

    Socket incoming;
    Socket outgoing;
};

/*
 * Join a ring using an already-listening socket and every rank's port.
 *
 * ports[r] is the loopback listening port for rank r.
 * All listeners must exist before processes call this function.
 *
 * The function takes ownership of listener.
 * Each network operation has the supplied timeout.
 * Supported group size: 1 through 64 processes.
 */
[[nodiscard]] Ring connect_ring(
    Socket listener,
    std::size_t rank,
    const std::vector<std::uint16_t>& ports,
    std::chrono::milliseconds timeout);

}  // namespace collective