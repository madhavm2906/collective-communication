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

add_library(collective STATIC src/reference.cpp)

target_include_directories(collective PUBLIC include)
target_link_libraries(collective PUBLIC collective_options)

include(CTest)

if(BUILD_TESTING)
    add_executable(reference_test tests/reference_test.cpp)
    target_link_libraries(reference_test PRIVATE collective)

    add_test(NAME reference_sum COMMAND reference_test)
    set_tests_properties(reference_sum PROPERTIES TIMEOUT 30)
endif()