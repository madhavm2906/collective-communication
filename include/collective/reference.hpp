/*
 * Declares the local reference sum used to validate our distributed all-reduce.
 *
 * Each inner vector represents one process's input.
 * The result contains the elementwise sum across all processes.
 * This function does not perform network communication.
 */

#pragma once

#include <vector>

namespace collective {

// Requires at least one process and equal input lengths.
// Zero-length vectors are allowed.
// Throws std::invalid_argument when the input structure is invalid.
[[nodiscard]] std::vector<double> reference_sum(
    const std::vector<std::vector<double>>& inputs);

}  // namespace collective