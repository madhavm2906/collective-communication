/*
 * Owns one Linux TCP socket and closes it automatically.
 *
 * Copying is prohibited because a socket must have a single owner.
 * Moving transfers ownership and leaves the original object empty.
 *
 * This class manages resource lifetime only. Connection establishment,
 * message transfers, and deadlines will be added in subsequent steps.
 */

#pragma once

#include <cerrno>
#include <system_error>
#include <utility>

#include <sys/socket.h>
#include <unistd.h>

namespace collective {

class Socket {
public:
    // An empty Socket owns no operating-system resource.
    Socket() noexcept = default;

    ~Socket() noexcept {
        reset();
    }

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    Socket(Socket&& other) noexcept
        : fd_(std::exchange(other.fd_, -1)) {}

    Socket& operator=(Socket&& other) noexcept {
        if (this != &other) {
            reset();
            fd_ = std::exchange(other.fd_, -1);
        }

        return *this;
    }

    // Create an IPv4 TCP socket.
    // CLOEXEC prevents accidentally passing it to an executed program.
    [[nodiscard]] static Socket tcp() {
        const int fd = ::socket(
            AF_INET,
            SOCK_STREAM | SOCK_CLOEXEC,
            0);

        if (fd == -1) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "creating TCP socket");
        }

        return Socket(fd);
    }

    // Borrow the descriptor without transferring ownership.
    // Callers must not close this descriptor themselves.
    [[nodiscard]] int native_handle() const noexcept {
        return fd_;
    }

    [[nodiscard]] bool valid() const noexcept {
        return fd_ != -1;
    }

    // Release the resource. Repeated calls are harmless.
    void reset() noexcept {
        const int old_fd = std::exchange(fd_, -1);

        if (old_fd != -1) {
            // On Linux, do not retry close() after EINTR:
            // the descriptor may already have been released.
            (void)::close(old_fd);
        }
    }

private:
    explicit Socket(int fd) noexcept : fd_(fd) {}

    int fd_ = -1;
};

}  // namespace collective