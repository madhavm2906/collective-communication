# Builds the reference library and its automated tests.
# Enables strict warnings and optional runtime memory/undefined-behavior checks.

cmake_minimum_required(VERSION 3.20)

project(
    collective_communication
    VERSION 0.1.0
    LANGUAGES CXX
)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(
    COLLECTIVE_ENABLE_SANITIZERS
    "Enable AddressSanitizer and UndefinedBehaviorSanitizer"
    OFF
)

add_library(collective_options INTERFACE)

target_compile_features(collective_options INTERFACE cxx_std_17)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(
        collective_options INTERFACE
        -Wall
        -Wextra
        -Wpedantic
        -Wconversion
        -Wshadow
    )

    if(COLLECTIVE_ENABLE_SANITIZERS)
        target_compile_options(
            collective_options INTERFACE
            -fsanitize=address,undefined
            -fno-omit-frame-pointer
        )

        target_link_options(
            collective_options INTERFACE
            -fsanitize=address,undefined
        )
    endif()
endif()

# Build both the reference reduction and TCP connection implementation.
add_library(
    collective STATIC
    src/reference.cpp
    src/tcp.cpp
)

# Enable Linux declarations such as accept4 explicitly.
target_compile_definitions(collective PRIVATE _GNU_SOURCE)

target_include_directories(collective PUBLIC include)
target_link_libraries(collective PUBLIC collective_options)
# Build the executable used to exercise two-process TCP connections.
add_executable(connection_demo examples/connection_demo.cpp)
target_link_libraries(connection_demo PRIVATE collective)

include(CTest)

# Build and register the reference and Linux socket ownership tests.
if(BUILD_TESTING)
    add_executable(reference_test tests/reference_test.cpp)
    target_link_libraries(reference_test PRIVATE collective)

    add_executable(socket_test tests/socket_test.cpp)
    target_link_libraries(socket_test PRIVATE collective)

    add_test(NAME reference_sum COMMAND reference_test)
    add_test(NAME socket_ownership COMMAND socket_test)

    set_tests_properties(
        reference_sum
        socket_ownership
        PROPERTIES TIMEOUT 30
    )
endif()