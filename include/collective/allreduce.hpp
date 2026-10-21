/*
 * Declares an in-place floating-point sum using ring all-reduce.
 *
 * Every rank supplies an equally sized vector and receives the complete sum.
 * Uses reduce-scatter followed by all-gather.
 *
 * Supports empty vectors, uneven chunks, and single-rank groups.
 * Each exchange round has a deadline; partial progress does not reset it.
 *
 * operation_id must match across ranks and identify the current collective.
 * Calls on a ring must be sequential, not concurrent.
 *
 * On failure, abandon the ring and its partially modified output.
 * Floating-point addition order may differ from the reference reduction.
 */

#pragma once

#include "collective/ring.hpp"

#include <chrono>
#include <cstdint>
#include <vector>

namespace collective {

void allreduce_sum(
    const Ring& ring,
    std::vector<double>& values,
    std::uint64_t operation_id,
    std::chrono::milliseconds round_timeout);

}  // namespace collective