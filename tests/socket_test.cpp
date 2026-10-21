/*
 * Tests ownership and lifetime of real Linux TCP sockets.
 *
 * Verifies empty objects, socket creation, close-on-exec, destruction,
 * move construction, move assignment, and repeated reset operations.
 *
 * These tests do not require a server or an external network connection.
 */

#include "collective/socket.hpp"

#include <cerrno>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

#include <fcntl.h>

namespace {

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

bool descriptor_is_open(int fd) {
    return ::fcntl(fd, F_GETFD) != -1;
}

bool descriptor_is_closed(int fd) {
    errno = 0;
    const int result = ::fcntl(fd, F_GETFD);
    return result == -1 && errno == EBADF;
}

void test_empty_socket() {
    collective::Socket socket;

    require(!socket.valid(), "default socket must be empty");
    require(
        socket.native_handle() == -1,
        "empty socket must use descriptor -1");
}

void test_creation() {
    auto socket = collective::Socket::tcp();

    require(socket.valid(), "created socket must be valid");
    require(
        descriptor_is_open(socket.native_handle()),
        "created descriptor must be open");

    const int flags = ::fcntl(socket.native_handle(), F_GETFD);

    require(
        flags != -1 && (flags & FD_CLOEXEC) != 0,
        "socket must have close-on-exec enabled");
}

void test_destruction() {
    int descriptor = -1;

    {
        auto socket = collective::Socket::tcp();
        descriptor = socket.native_handle();
    }

    require(
        descriptor_is_closed(descriptor),
        "destructor must close the descriptor");
}

void test_move_construction() {
    auto original = collective::Socket::tcp();
    const int descriptor = original.native_handle();

    auto moved = std::move(original);

    require(!original.valid(), "moved-from socket must be empty");
    require(
        moved.native_handle() == descriptor,
        "move must preserve the descriptor");
    require(
        descriptor_is_open(descriptor),
        "move must leave the owned descriptor open");
}

void test_move_assignment() {
    auto destination = collective::Socket::tcp();
    auto source = collective::Socket::tcp();

    const int old_destination = destination.native_handle();
    const int source_descriptor = source.native_handle();

    destination = std::move(source);

    require(!source.valid(), "assigned-from socket must be empty");
    require(
        descriptor_is_closed(old_destination),
        "assignment must close the destination's old socket");
    require(
        destination.native_handle() == source_descriptor,
        "assignment must transfer the source descriptor");
    require(
        descriptor_is_open(source_descriptor),
        "transferred descriptor must remain open");
}

void test_reset() {
    auto socket = collective::Socket::tcp();
    const int descriptor = socket.native_handle();

    socket.reset();

    require(!socket.valid(), "reset must empty the socket");
    require(
        descriptor_is_closed(descriptor),
        "reset must close the descriptor");

    socket.reset();

    require(!socket.valid(), "repeated reset must remain safe");
}

}  // namespace

// Verify ownership rules at compile time.
static_assert(!std::is_copy_constructible_v<collective::Socket>);
static_assert(!std::is_copy_assignable_v<collective::Socket>);
static_assert(std::is_nothrow_move_constructible_v<collective::Socket>);
static_assert(std::is_nothrow_move_assignable_v<collective::Socket>);

int main() {
    try {
        test_empty_socket();
        test_creation();
        test_destruction();
        test_move_construction();
        test_move_assignment();
        test_reset();

        std::cout << "PASS: 6 socket ownership cases\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}