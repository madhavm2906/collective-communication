# Builds the library, demonstrations, benchmark, and automated tests.
# The benchmark rejects unoptimized or sanitizer-enabled execution.

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

find_package(Threads REQUIRED)

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
    src/ring.cpp
    src/allreduce.cpp
)

target_compile_definitions(collective PRIVATE _GNU_SOURCE)
target_include_directories(collective PUBLIC include)

target_link_libraries(
    collective PUBLIC
    collective_options
    Threads::Threads
)

add_executable(connection_demo examples/connection_demo.cpp)
target_link_libraries(connection_demo PRIVATE collective)

add_executable(ring_demo examples/ring_demo.cpp)
target_link_libraries(ring_demo PRIVATE collective)

add_executable(allreduce_demo examples/allreduce_demo.cpp)
target_link_libraries(allreduce_demo PRIVATE collective)

add_executable(
    allreduce_benchmark
    benchmarks/allreduce_benchmark.cpp
)
target_link_libraries(allreduce_benchmark PRIVATE collective)

target_compile_definitions(
    allreduce_benchmark PRIVATE
    COLLECTIVE_BENCHMARK_SANITIZERS=$<BOOL:${COLLECTIVE_ENABLE_SANITIZERS}>
)

include(CTest)

if(BUILD_TESTING)
    find_package(Python3 REQUIRED COMPONENTS Interpreter)

    add_executable(reference_test tests/reference_test.cpp)
    target_link_libraries(reference_test PRIVATE collective)

    add_executable(socket_test tests/socket_test.cpp)
    target_link_libraries(socket_test PRIVATE collective)

    add_executable(tcp_transfer_test tests/tcp_transfer_test.cpp)
    target_link_libraries(tcp_transfer_test PRIVATE collective)

    add_executable(
        allreduce_failure_worker
        tests/allreduce_failure_worker.cpp
    )
    target_link_libraries(allreduce_failure_worker PRIVATE collective)

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

    add_test(
        NAME ring_integration
        COMMAND
            "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/ring_integration_test.py"
            "$<TARGET_FILE:ring_demo>"
    )

    add_test(
        NAME allreduce_integration
        COMMAND
            "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/allreduce_integration_test.py"
            "$<TARGET_FILE:allreduce_demo>"
    )

    add_test(
        NAME allreduce_failures
        COMMAND
            "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/allreduce_failure_test.py"
            "$<TARGET_FILE:allreduce_failure_worker>"
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

    set_tests_properties(
        ring_integration
        PROPERTIES TIMEOUT 120 ENVIRONMENT "LC_ALL=C"
    )

    set_tests_properties(
        allreduce_integration
        PROPERTIES TIMEOUT 240 ENVIRONMENT "LC_ALL=C"
    )

    set_tests_properties(
        allreduce_failures
        PROPERTIES TIMEOUT 90 ENVIRONMENT "LC_ALL=C"
    )
endif()