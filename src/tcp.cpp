/*
 * Implements nonblocking TCP listening, acceptance, and connection.
 *
 * Uses a steady-clock deadline so interrupted waits do not restart the
 * timeout. Handles pending connections and checks their final socket error.
 *
 * All sockets are owned by Socket objects, so failures release resources.
 */

#include "collective/tcp.hpp"

#include <cerrno>
#include <chrono>
#include <stdexcept>
#include <system_error>

#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>

namespace collective {
namespace {

using Clock = std::chrono::steady_clock;

[[noreturn]] void fail_errno(const char* operation) {
    const int error = errno;

    throw std::system_error(
        error,
        std::generic_category(),
        operation);
}

[[noreturn]] void fail_timeout() {
    throw std::system_error(
        std::make_error_code(std::errc::timed_out),
        "TCP connection deadline expired");
}

Clock::time_point make_deadline(std::chrono::milliseconds timeout) {
    if (timeout.count() <= 0 || timeout.count() > 60000) {
        throw std::invalid_argument(
            "timeout must be between 1 and 60000 milliseconds");
    }

    return Clock::now() + timeout;
}

void make_nonblocking(const Socket& socket) {
    const int fd = socket.native_handle();
    const int flags = ::fcntl(fd, F_GETFL);

    if (flags == -1) {
        fail_errno("reading socket flags");
    }

    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        fail_errno("setting nonblocking mode");
    }
}

sockaddr_in loopback_address(std::uint16_t port) {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    return address;
}

void wait_ready(
    int fd,
    short events,
    Clock::time_point deadline) {
    if (fd < 0) {
        throw std::invalid_argument("cannot wait on an empty socket");
    }

    for (;;) {
        const auto now = Clock::now();

        if (now >= deadline) {
            fail_timeout();
        }

        // Round upward to avoid turning a positive remaining duration
        // into a zero-millisecond busy wait.
        const auto remaining =
            std::chrono::ceil<std::chrono::milliseconds>(deadline - now);

        pollfd descriptor{};
        descriptor.fd = fd;
        descriptor.events = events;

        const int result = ::poll(
            &descriptor,
            1,
            static_cast<int>(remaining.count()));

        if (result == -1) {
            if (errno == EINTR) {
                continue;
            }

            fail_errno("waiting for socket readiness");
        }

        if (result == 0) {
            continue;
        }

        if ((descriptor.revents & POLLNVAL) != 0) {
            throw std::system_error(
                std::make_error_code(std::errc::bad_file_descriptor),
                "poll reported an invalid socket");
        }

        // Let the next socket operation report a detailed error.
        if ((descriptor.revents &
             (events | POLLERR | POLLHUP)) != 0) {
            return;
        }
    }
}

}  // namespace

Socket listen_loopback(std::uint16_t port) {
    auto listener = Socket::tcp();
    make_nonblocking(listener);

    const int enabled = 1;

    if (::setsockopt(
            listener.native_handle(),
            SOL_SOCKET,
            SO_REUSEADDR,
            &enabled,
            sizeof(enabled)) == -1) {
        fail_errno("setting SO_REUSEADDR");
    }

    const auto address = loopback_address(port);

    if (::bind(
            listener.native_handle(),
            reinterpret_cast<const sockaddr*>(&address),
            sizeof(address)) == -1) {
        fail_errno("binding listening socket");
    }

    if (::listen(listener.native_handle(), 16) == -1) {
        fail_errno("listening for connections");
    }

    return listener;
}

Socket accept_connection(
    const Socket& listener,
    std::chrono::milliseconds timeout) {
    const auto deadline = make_deadline(timeout);

    for (;;) {
        wait_ready(listener.native_handle(), POLLIN, deadline);

        const int fd = ::accept4(
            listener.native_handle(),
            nullptr,
            nullptr,
            SOCK_CLOEXEC | SOCK_NONBLOCK);

        if (fd >= 0) {
            return Socket::adopt(fd);
        }

        const int error = errno;

        if (error == EINTR ||
            error == EAGAIN ||
            error == EWOULDBLOCK ||
            error == ECONNABORTED) {
            continue;
        }

        fail_errno("accepting TCP connection");
    }
}

Socket connect_loopback(
    std::uint16_t port,
    std::chrono::milliseconds timeout) {
    const auto deadline = make_deadline(timeout);
    auto socket = Socket::tcp();
    make_nonblocking(socket);

    const auto address = loopback_address(port);

    const int result = ::connect(
        socket.native_handle(),
        reinterpret_cast<const sockaddr*>(&address),
        sizeof(address));

    if (result == 0) {
        return socket;
    }

    const int error = errno;

    if (error != EINPROGRESS && error != EINTR) {
        fail_errno("starting TCP connection");
    }

    wait_ready(socket.native_handle(), POLLOUT, deadline);

    int connection_error = 0;
    socklen_t error_length = sizeof(connection_error);

    if (::getsockopt(
            socket.native_handle(),
            SOL_SOCKET,
            SO_ERROR,
            &connection_error,
            &error_length) == -1) {
        fail_errno("reading TCP connection result");
    }

    if (connection_error != 0) {
        throw std::system_error(
            connection_error,
            std::generic_category(),
            "establishing TCP connection");
    }

    return socket;
}

}  // namespace collective