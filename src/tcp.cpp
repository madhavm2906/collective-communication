/*
 * Implements nonblocking TCP connections and complete byte transfers.
 *
 * Uses poll() and steady-clock deadlines for bounded waiting.
 * Retries interrupted operations and waits when a socket would block.
 * Handles partial reads/writes and detects early peer disconnects.
 *
 * MSG_NOSIGNAL prevents a failed send from terminating the process
 * with SIGPIPE. MSG_DONTWAIT keeps individual transfer calls nonblocking.
 */

#include "collective/tcp.hpp"

#include <algorithm>
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

constexpr std::size_t max_syscall_bytes = 1024 * 1024;

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
        "TCP deadline expired");
}

Clock::time_point make_deadline(std::chrono::milliseconds timeout) {
    if (timeout.count() <= 0 || timeout.count() > 60000) {
        throw std::invalid_argument(
            "timeout must be between 1 and 60000 milliseconds");
    }

    return Clock::now() + timeout;
}

void check_deadline(Clock::time_point deadline) {
    if (Clock::now() >= deadline) {
        fail_timeout();
    }
}

void require_socket(const Socket& socket) {
    if (!socket.valid()) {
        throw std::invalid_argument("operation requires a valid socket");
    }
}

void validate_buffer(const void* buffer, std::size_t byte_count) {
    if (buffer == nullptr && byte_count != 0) {
        throw std::invalid_argument(
            "nonempty transfer requires a nonnull buffer");
    }
}

void make_nonblocking(const Socket& socket) {
    require_socket(socket);

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

bool would_block(int error) {
    return error == EAGAIN || error == EWOULDBLOCK;
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

        // The following socket call diagnoses errors or a peer shutdown.
        // A readable socket can contain buffered data even after shutdown.
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

std::uint16_t local_port(const Socket& socket) {
    require_socket(socket);

    sockaddr_in address{};
    socklen_t length = sizeof(address);

    if (::getsockname(
            socket.native_handle(),
            reinterpret_cast<sockaddr*>(&address),
            &length) == -1) {
        fail_errno("reading local socket address");
    }

    if (address.sin_family != AF_INET) {
        throw std::runtime_error("expected an IPv4 socket");
    }

    return ntohs(address.sin_port);
}

Socket accept_connection(
    const Socket& listener,
    std::chrono::milliseconds timeout) {
    require_socket(listener);
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
            would_block(error) ||
            error == ECONNABORTED) {
            continue;
        }

        fail_errno("accepting TCP connection");
    }
}

Socket connect_loopback(
    std::uint16_t port,
    std::chrono::milliseconds timeout) {
    if (port == 0) {
        throw std::invalid_argument("destination port must be nonzero");
    }

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

void send_all(
    const Socket& socket,
    const void* buffer,
    std::size_t byte_count,
    std::chrono::milliseconds timeout) {
    require_socket(socket);
    validate_buffer(buffer, byte_count);
    const auto deadline = make_deadline(timeout);

    const auto* bytes = static_cast<const unsigned char*>(buffer);
    std::size_t completed = 0;

    while (completed < byte_count) {
        check_deadline(deadline);

        const auto request =
            std::min(byte_count - completed, max_syscall_bytes);

        const auto sent = ::send(
            socket.native_handle(),
            bytes + completed,
            request,
            MSG_DONTWAIT | MSG_NOSIGNAL);

        if (sent > 0) {
            completed += static_cast<std::size_t>(sent);
            continue;
        }

        if (sent == 0) {
            throw std::runtime_error("send made no progress");
        }

        const int error = errno;

        if (error == EINTR) {
            continue;
        }

        if (would_block(error)) {
            wait_ready(socket.native_handle(), POLLOUT, deadline);
            continue;
        }

        fail_errno("sending TCP bytes");
    }
}

void receive_exact(
    const Socket& socket,
    void* buffer,
    std::size_t byte_count,
    std::chrono::milliseconds timeout) {
    require_socket(socket);
    validate_buffer(buffer, byte_count);
    const auto deadline = make_deadline(timeout);

    auto* bytes = static_cast<unsigned char*>(buffer);
    std::size_t completed = 0;

    while (completed < byte_count) {
        check_deadline(deadline);

        const auto request =
            std::min(byte_count - completed, max_syscall_bytes);

        const auto received = ::recv(
            socket.native_handle(),
            bytes + completed,
            request,
            MSG_DONTWAIT);

        if (received > 0) {
            completed += static_cast<std::size_t>(received);
            continue;
        }

        if (received == 0) {
            throw std::runtime_error(
                "peer closed connection before transfer completed");
        }

        const int error = errno;

        if (error == EINTR) {
            continue;
        }

        if (would_block(error)) {
            wait_ready(socket.native_handle(), POLLIN, deadline);
            continue;
        }

        fail_errno("receiving TCP bytes");
    }
}

}  // namespace collective