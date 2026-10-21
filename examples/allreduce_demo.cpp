/*
 * Runs one all-reduce rank and validates its output against reference_sum.
 *
 * Usage:
 *   allreduce_demo RANK WORLD_SIZE ELEMENT_COUNT SEED ITERATIONS
 *
 * Announces its listener, then reads the full port list from stdin.
 * Reuses the same ring for multiple collective operations.
 *
 * Inputs include reproducible positive and negative fractional values.
 * Each rank independently checks every output element.
 */

#include "collective/allreduce.hpp"
#include "collective/reference.hpp"
#include "collective/ring.hpp"
#include "collective/tcp.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

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
            throw std::runtime_error("invalid listening-port configuration");
        }

        ports.push_back(static_cast<std::uint16_t>(port));
    }

    return ports;
}

std::vector<std::vector<double>> make_inputs(
    std::size_t world_size,
    std::size_t count,
    std::uint32_t seed) {
    std::mt19937 generator(seed);
    std::uniform_real_distribution<double> distribution(-100.0, 100.0);

    std::vector<std::vector<double>> inputs(
        world_size,
        std::vector<double>(count));

    for (auto& input : inputs) {
        for (auto& value : input) {
            value = distribution(generator);
        }
    }

    return inputs;
}

void verify(
    const std::vector<double>& actual,
    const std::vector<double>& expected) {
    if (actual.size() != expected.size()) {
        throw std::runtime_error("allreduce output size mismatch");
    }

    for (std::size_t index = 0; index < actual.size(); ++index) {
        const auto scale =
            std::max(std::abs(actual[index]), std::abs(expected[index]));

        const auto tolerance = 1e-9 + 1e-12 * scale;

        if (!std::isfinite(actual[index]) ||
            std::abs(actual[index] - expected[index]) > tolerance) {
            throw std::runtime_error(
                "allreduce result mismatch at index " +
                std::to_string(index));
        }
    }
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
        if (argc != 6) {
            throw std::invalid_argument(
                "usage: allreduce_demo RANK WORLD_SIZE "
                "ELEMENT_COUNT SEED ITERATIONS");
        }

        const auto rank =
            static_cast<std::size_t>(parse_number(argv[1], 63));

        const auto world_size =
            static_cast<std::size_t>(parse_number(argv[2], 64));

        const auto count =
            static_cast<std::size_t>(parse_number(argv[3], 1048576));

        const auto seed =
            static_cast<std::uint32_t>(parse_number(argv[4], 1000000));

        const auto iterations =
            static_cast<std::size_t>(parse_number(argv[5], 100));

        if (world_size == 0 || rank >= world_size || iterations == 0) {
            throw std::invalid_argument("invalid rank, group, or iterations");
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

        for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
            const auto iteration_seed =
                seed + static_cast<std::uint32_t>(iteration);

            const auto inputs =
                make_inputs(world_size, count, iteration_seed);

            auto actual = inputs[rank];
            const auto expected = collective::reference_sum(inputs);

            collective::allreduce_sum(
                ring,
                actual,
                static_cast<std::uint64_t>(iteration + 1),
                10000ms);

            verify(actual, expected);
        }

        std::cout
            << "RANK " << rank
            << ": PASS world=" << world_size
            << " count=" << count
            << " iterations=" << iterations
            << '\n';

        return 0;
    } catch (const std::exception& error) {
        std::cerr << "ERROR: " << error.what() << '\n';
        return 1;
    }
}