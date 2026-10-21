/*
 * Implements a straightforward elementwise sum across process inputs.
 *
 * This intentionally simple implementation serves as our correctness oracle.
 * The future TCP implementation must produce numerically equivalent results
 * on every participating process.
 */

#include "collective/reference.hpp"

#include <cstddef>
#include <stdexcept>

namespace collective {

std::vector<double> reference_sum(
    const std::vector<std::vector<double>>& inputs) {
    if (inputs.empty()) {
        throw std::invalid_argument(
            "reference_sum requires at least one process");
    }

    const std::size_t element_count = inputs.front().size();

    // Validate all lengths before performing the reduction.
    for (const auto& input : inputs) {
        if (input.size() != element_count) {
            throw std::invalid_argument(
                "all processes must provide the same number of elements");
        }
    }

    std::vector<double> result(element_count, 0.0);

    for (const auto& input : inputs) {
        for (std::size_t index = 0; index < element_count; ++index) {
            result[index] += input[index];
        }
    }

    return result;
}

}  // namespace collective