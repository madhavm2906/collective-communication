/*
 * Measures repeated all-reduce operations in one rank.
 *
 * Requires an optimized build with sanitizers disabled.
 * Startup, input initialization, synchronization, and verification are untimed.
 * The measured region contains the complete allreduce_sum call.
 *
 * Each rank emits its measured samples as one JSON object.
 *
 * Usage:
 *   allreduce_benchmark RANK WORLD_SIZE ELEMENT_COUNT WARMUP ITERATIONS
 */

#include "collective/allreduce.hpp"
#include "collective/ring.hpp"
#include "collective/tcp.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

unsigned long parse_number(
    const std::string& text,
    unsigned long maximum) {
    if (text.empty() ||
        text.find_first_not_of("0123456789") != std::string::npos) {
        throw std::invalid_argument("arguments must contain only digits");
    }

    const auto value = std::stoul(text);

    if (value > maximum) {
        throw std::invalid_argument("numeric argument is out of range");
    }

    return value;
}

std::vector<std::uint16_t> read_ports(std::size_t world_size) {
    std::vector<std::uint16_t> ports;
    ports.reserve(world_size);

    for (std::size_t index = 0; index < world_size; ++index) {
        unsigned int port = 0;

        if (!(std::cin >> port) || port == 0 || port > 65535) {
            throw std::runtime_error("invalid port configuration");
        }

        ports.push_back(static_cast<std::uint16_t>(port));
    }

    return ports;
}

void verify(const std::vector<double>& values, double expected) {
    // These inputs and their sums are small integers represented exactly.
    for (const auto value : values) {
        if (value != expected) {
            throw std::runtime_error("benchmark output validation failed");
        }
    }
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
#ifndef NDEBUG
        throw std::runtime_error(
            "benchmark requires an optimized build; use build/release");
#endif

#if COLLECTIVE_BENCHMARK_SANITIZERS
        throw std::runtime_error(
            "benchmark requires sanitizers to be disabled");
#endif

        if (argc != 6) {
            throw std::invalid_argument(
                "usage: allreduce_benchmark RANK WORLD_SIZE "
                "ELEMENT_COUNT WARMUP ITERATIONS");
        }

        const auto rank =
            static_cast<std::size_t>(parse_number(argv[1], 63));

        const auto world_size =
            static_cast<std::size_t>(parse_number(argv[2], 64));

        const auto count =
            static_cast<std::size_t>(parse_number(argv[3], 1048576));

        const auto warmup =
            static_cast<std::size_t>(parse_number(argv[4], 1000));

        const auto iterations =
            static_cast<std::size_t>(parse_number(argv[5], 10000));

        if (world_size == 0 ||
            rank >= world_size ||
            count == 0 ||
            warmup == 0 ||
            iterations == 0) {
            throw std::invalid_argument("invalid benchmark configuration");
        }

        auto listener = collective::listen_loopback(0);

        std::cout
            << "READY " << rank << ' '
            << collective::local_port(listener)
            << std::endl;

        const auto ports = read_ports(world_size);

        auto ring = collective::connect_ring(
            std::move(listener),
            rank,
            ports,
            10000ms);

        const double input_value = static_cast<double>(rank + 1);

        const double expected = static_cast<double>(
            world_size * (world_size + 1) / 2);

        std::vector<double> values(count);
        std::vector<double> synchronization(1, 0.0);
        std::vector<double> samples_us;
        samples_us.reserve(iterations);

        for (std::size_t cycle = 0; cycle < warmup + iterations; ++cycle) {
            // Input preparation is outside the measured region.
            std::fill(values.begin(), values.end(), input_value);

            const auto synchronization_id =
                static_cast<std::uint64_t>(2 * cycle + 1);

            const auto measured_id =
                static_cast<std::uint64_t>(2 * cycle + 2);

            // Untimed collective keeps iteration progress aligned.
            // This is not a claim of perfectly simultaneous start times.
            collective::allreduce_sum(
                ring,
                synchronization,
                synchronization_id,
                10000ms);

            const auto start = Clock::now();

            collective::allreduce_sum(
                ring,
                values,
                measured_id,
                10000ms);

            const auto finish = Clock::now();

            // Validate every iteration outside its measured interval.
            verify(values, expected);

            if (cycle >= warmup) {
                const auto elapsed =
                    std::chrono::duration<double, std::micro>(
                        finish - start);

                samples_us.push_back(elapsed.count());
            }
        }

        std::cout
            << std::setprecision(17)
            << "{\"rank\":" << rank
            << ",\"world_size\":" << world_size
            << ",\"elements\":" << count
            << ",\"samples_us\":[";

        for (std::size_t index = 0; index < samples_us.size(); ++index) {
            if (index != 0) {
                std::cout << ',';
            }

            std::cout << samples_us[index];
        }

        std::cout << "]}\n";

        return 0;
    } catch (const std::exception& error) {
        std::cerr << "ERROR: " << error.what() << '\n';
        return 1;
    }
}