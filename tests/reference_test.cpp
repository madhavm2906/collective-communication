/*
 * Tests the reference reduction without external testing dependencies.
 *
 * Covers known sums, one process, empty vectors, fractional values,
 * invalid input structures, and 500 deterministic randomized cases.
 *
 * Explicit checks remain active in both Debug and Release builds.
 */

#include "collective/reference.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <exception>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

// Reject nonfinite values and allow small floating-point rounding differences.
bool nearly_equal(double actual, double expected) {
    if (!std::isfinite(actual) || !std::isfinite(expected)) {
        return false;
    }

    constexpr double absolute_tolerance = 1e-12;
    constexpr double relative_tolerance = 1e-12;

    const double scale =
        std::max(std::abs(actual), std::abs(expected));

    return std::abs(actual - expected) <=
           absolute_tolerance + relative_tolerance * scale;
}

void require_vector(
    const std::vector<double>& actual,
    const std::vector<double>& expected,
    const std::string& name) {
    require(actual.size() == expected.size(), name + ": wrong size");

    for (std::size_t index = 0; index < actual.size(); ++index) {
        require(
            nearly_equal(actual[index], expected[index]),
            name + ": mismatch at index " + std::to_string(index));
    }
}

template <typename Function>
void require_invalid_argument(
    Function function,
    const std::string& name) {
    try {
        function();
    } catch (const std::invalid_argument&) {
        return;
    }

    throw std::runtime_error(name + ": expected invalid_argument");
}

void test_known_cases() {
    using collective::reference_sum;

    require_vector(
        reference_sum({{1, 2}, {3, 4}, {5, 6}}),
        {9, 12},
        "three processes");

    require_vector(
        reference_sum({{-2, 0, 7}}),
        {-2, 0, 7},
        "single process");

    require_vector(
        reference_sum({{-3, 4}, {3, -4}}),
        {0, 0},
        "cancellation");

    require_vector(
        reference_sum({{0.1, 0.2}, {0.2, 0.3}}),
        {0.3, 0.5},
        "fractional values");

    require(
        reference_sum({{}, {}, {}}).empty(),
        "zero-length inputs");

    require_invalid_argument(
        [] { (void)reference_sum({}); },
        "empty process group");

    require_invalid_argument(
        [] { (void)reference_sum({{1}, {2, 3}}); },
        "mismatched input lengths");
}

void test_randomized_cases() {
    // A fixed seed makes failures reproducible within this toolchain.
    std::mt19937 generator(2027);
    std::uniform_int_distribution<int> process_counts(1, 8);
    std::uniform_int_distribution<int> element_counts(0, 257);
    std::uniform_int_distribution<int> values(-1000, 1000);

    for (int trial = 0; trial < 500; ++trial) {
        const auto process_count =
            static_cast<std::size_t>(process_counts(generator));
        const auto element_count =
            static_cast<std::size_t>(element_counts(generator));

        std::vector<std::vector<double>> inputs(
            process_count, std::vector<double>(element_count));

        // Generate quarter-integer values. These are exactly representable
        // here, allowing an independent integer-based expected result.
        std::vector<long long> expected_units(element_count, 0);

        for (auto& input : inputs) {
            for (std::size_t index = 0; index < element_count; ++index) {
                const int units = values(generator);
                input[index] = static_cast<double>(units) / 4.0;
                expected_units[index] += units;
            }
        }

        std::vector<double> expected(element_count);
        for (std::size_t index = 0; index < element_count; ++index) {
            expected[index] =
                static_cast<double>(expected_units[index]) / 4.0;
        }

        require_vector(
            collective::reference_sum(inputs),
            expected,
            "random trial " + std::to_string(trial));
    }
}

}  // namespace

int main() {
    try {
        test_known_cases();
        test_randomized_cases();

        std::cout
            << "PASS: 7 known cases and 500 randomized cases\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}