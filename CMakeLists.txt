# Builds the collective communication library, examples, and tests.
# Supports strict warnings and optional runtime error detection.
# Includes process-based integration tests and threaded transport checks.

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

add_library(
    collective STATIC
    src/reference.cpp
    src/tcp.cpp
)

target_compile_definitions(collective PRIVATE _GNU_SOURCE)
target_include_directories(collective PUBLIC include)
target_link_libraries(collective PUBLIC collective_options)

add_executable(connection_demo examples/connection_demo.cpp)
target_link_libraries(connection_demo PRIVATE collective)

include(CTest)

if(BUILD_TESTING)
    find_package(Python3 REQUIRED COMPONENTS Interpreter)
    find_package(Threads REQUIRED)

    add_executable(reference_test tests/reference_test.cpp)
    target_link_libraries(reference_test PRIVATE collective)

    add_executable(socket_test tests/socket_test.cpp)
    target_link_libraries(socket_test PRIVATE collective)

    add_executable(tcp_transfer_test tests/tcp_transfer_test.cpp)
    target_link_libraries(
        tcp_transfer_test PRIVATE
        collective
        Threads::Threads
    )

    add_test(NAME reference_sum COMMAND reference_test)
    add_test(NAME socket_ownership COMMAND socket_test)
    add_test(NAME tcp_transfer COMMAND tcp_transfer_test)

    add_test(
        NAME tcp_integration
        COMMAND
            "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/tcp_integration_test.py"
            "$<TARGET_FILE:connection_demo>"
    )

    set_tests_properties(
        reference_sum
        socket_ownership
        tcp_transfer
        PROPERTIES TIMEOUT 30
    )

    set_tests_properties(
        tcp_integration
        PROPERTIES TIMEOUT 60 ENVIRONMENT "LC_ALL=C"
    )
endif()