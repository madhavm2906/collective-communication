<!--
Documents the implemented library, architecture, build instructions,
automated tests, reproducible performance results, and current scope.
-->

# Collective Communication Library

[![CI](https://github.com/madhavm2906/collective-communication/actions/workflows/ci.yml/badge.svg)](https://github.com/madhavm2906/collective-communication/actions/workflows/ci.yml)

A C++17 implementation of floating-point ring all-reduce over TCP.

Independent Linux processes compute an elementwise sum using reduce-scatter
followed by all-gather. Every participating process receives the complete result.

The library uses move-only socket ownership, nonblocking communication,
validated message frames, and deadline-bounded exchanges. A single-threaded
poll loop advances outgoing and incoming traffic together.

## Example

```text
Rank 0: [1, 2]
Rank 1: [3, 4]
Rank 2: [5, 6]

Result on every rank: [9, 12]
```

A rank identifies a process within the group. All ranks supply vectors of
equal length. Chunk sizes may differ when that length is not divisible by
the group size.

## Implemented features

- Move-only socket ownership and automatic cleanup.
- Nonblocking IPv4 loopback connections.
- Complete byte-transfer loops handling partial reads and writes.
- Steady-clock deadlines that do not restart after partial progress.
- Ring startup with protocol, group size, and neighbor identity checks.
- Sum reduction over IEEE-754 binary64 values.
- Uneven partitions, empty inputs, and zero-length chunks.
- Repeated collective operations on the same connections.
- Frame validation for operation ID, phase, round, chunk, and lengths.
- Big-endian encoding of metadata and floating-point bit patterns.
- Scratch-buffer reuse across rounds within each collective.
- Concurrent communication progress without per-round sender threads.
- Controlled tests for disconnected, stalled, and misbehaving ranks.

## Requirements

- Linux, including Ubuntu under WSL 2.
- GCC or Clang with C++17 support.
- CMake 3.20 or newer.
- Ninja.
- Python 3 for test and benchmark orchestration.
- POSIX threads for the transport test executable.

On Ubuntu:

```bash
# Install the build and test dependencies.
sudo apt update
sudo apt install -y build-essential cmake ninja-build python3
```

## Build and test

### Debug with sanitizers

```bash
# Configure runtime memory and undefined-behavior checks.
cmake -S . -B build/debug -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCOLLECTIVE_ENABLE_SANITIZERS=ON

# Build and run the automated suites.
cmake --build build/debug
ctest --test-dir build/debug --output-on-failure
```

### Release

```bash
# Configure optimized code without sanitizers.
cmake -S . -B build/release -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCOLLECTIVE_ENABLE_SANITIZERS=OFF

# Build and verify the optimized implementation.
cmake --build build/release
ctest --test-dir build/release --output-on-failure
```

## Test coverage

| Suite | Coverage |
|---|---|
| `reference_sum` | Known sums, boundary cases, invalid inputs, randomized inputs |
| `socket_ownership` | Creation, destruction, moves, reset, close-on-exec |
| `tcp_transfer` | Large transfers, empty transfers, disconnects, backpressure, deadlines |
| `tcp_integration` | Independent client/server processes, fragmented writes, invalid lengths, timeouts |
| `ring_integration` | Ring formation and token circulation with 1, 2, 3, 4, and 8 ranks |
| `allreduce_integration` | Numerical validation, uneven partitions, repeated operations, large arrays, randomized configurations, mismatched lengths |
| `allreduce_failures` | Abrupt rank exit, connected but stalled rank, incorrect operation ID |

Every successful all-reduce test rank independently compares its output
against a local reference sum with floating-point tolerances.

Failure injection begins after the faulty rank observes real collective
traffic. Healthy ranks must report an expected error and exit.

```bash
# Display detailed numerical and active-failure test results.
ctest --test-dir build/debug \
  -R '^(allreduce_integration|allreduce_failures)$' \
  --verbose
```

## Architecture

```text
rank 0 -> rank 1 -> ... -> rank P-1 -> rank 0
```

Each rank owns an outgoing connection to its successor and an incoming
connection from its predecessor.

### Reduce-scatter

The input is divided into P chunks.

Across P-1 rounds, ranks send chunks and add incoming values to their local
data. Each rank finishes with one fully reduced chunk.

### All-gather

Across another P-1 rounds, ranks forward completed chunks and copy incoming
chunks into their final positions.

Every rank finishes with the complete reduced vector.

### Communication progress

The implementation encodes an outgoing frame, then polls both connections.

When the outgoing socket is writable, it advances the send position.
When the incoming socket is readable, it advances header or payload reception.
The received header is validated before its payload is accepted.

Both directions progress within one calling thread. Completed directions
are removed from the poll set. This avoids requiring an entire send to finish
before receiving and eliminates per-round sender-task creation.

TCP_NODELAY is enabled on ring connections.

## Performance experiments

Measurements were collected on:

- Intel Core i7-12650H.
- 16 logical CPUs available to WSL.
- Ubuntu 24.04 under WSL 2.
- Linux 5.15.167.4-microsoft-standard-WSL2.
- GCC 13.3.0.
- Release flags: `-O3 -DNDEBUG`.
- Sanitizers disabled.
- Local loopback communication without CPU affinity pinning.

These results describe this environment and workload.

### Method

Each configuration used three independent process groups. Each group performed
10 warmup iterations and 100 measured iterations.

Every iteration was validated. Input preparation, process startup, connection
setup, synchronization, and output verification were outside the timed region.

The measured region included the complete allreduce_sum call, including its
scratch allocations, encoding, communication, and reduction work.

For each iteration, the launcher selected the maximum elapsed time reported
by any rank. Run medians and nearest-rank p95 values were computed from those
iteration maxima.

The table below reports the median of three run medians. It is not a percentile
computed from pooled raw samples.

### Replacing sender tasks with full-duplex polling

Both versions in this comparison enable TCP_NODELAY.

| Ranks | Input per rank | Sender-task median, µs | Polling median, µs |
|---:|---:|---:|---:|
| 2 | 8 KiB | 119.193 | 35.442 |
| 2 | 64 KiB | 156.968 | 46.292 |
| 2 | 1 MiB | 612.614 | 425.554 |
| 2 | 4 MiB | 2698.958 | 3173.659 |
| 4 | 8 KiB | 539.243 | 167.784 |
| 4 | 64 KiB | 542.466 | 275.992 |
| 4 | 1 MiB | 1318.594 | 678.976 |
| 4 | 4 MiB | 5325.932 | 3797.117 |
| 8 | 8 KiB | 1776.935 | 380.325 |
| 8 | 64 KiB | 1803.103 | 366.761 |
| 8 | 1 MiB | 2852.245 | 947.170 |
| 8 | 4 MiB | 8651.077 | 7804.828 |

Polling reduced the eight-rank, 8 KiB aggregate median by approximately 78.6%,
from 1.777 ms to 0.380 ms.

The two-rank, 4 MiB aggregate median increased by approximately 17.6%.
Large-payload results showed run-to-run variation, so the change is not
presented as a universal speedup.

### Small-message buffering experiment

Before TCP_NODELAY was enabled, some small-array configurations exhibited
large latency stalls.

A confirmation experiment at eight ranks and 8 KiB per rank produced an
aggregate median of 107.542 ms for the original implementation and 1.658 ms
with TCP_NODELAY.

The repeated comparison supports using TCP_NODELAY for this workload.
The exact acknowledgment behavior was not established through packet tracing.

### Diagnostic syscall experiment

An eight-rank diagnostic run used two warmups and ten measured iterations.
Each cycle also included an untimed synchronization collective.

The sender-task implementation produced 2688 clone3 calls, matching:

```text
8 ranks × 12 cycles × 2 collectives × 14 rounds = 2688 tasks
```

The polling trace recorded no clone or clone3 calls. Futex calls decreased
from 6037 to 30.

Tracing changes execution timing. Traced latency figures are excluded from
the performance comparison above.

### Reproduce the benchmark

Commit source changes before measuring. The launcher requires a clean working
tree and records the revision, build configuration, binary hash, CPU details,
and raw samples.

```bash
# Run the same twelve-configuration experiment.
# Choose a new output directory for each experiment.
python3 benchmarks/run_benchmarks.py \
  --ranks 2 4 8 \
  --elements 1024 8192 131072 524288 \
  --warmup 10 \
  --iterations 100 \
  --runs 3 \
  --output results/reproduction-v1
```

Results include:

- `environment.json`
- `raw_samples.csv`
- `iteration_maxima.csv`
- `summary.csv`

Published experiment evidence is in [benchmarks/results](benchmarks/results).
Each directory records the revision it measured.

## Run the array-transfer example

Terminal 1:

```bash
# Accept one array request.
./build/debug/connection_demo server 47001
```

Terminal 2, within ten seconds:

```bash
# Send five integers and verify the returned sum of 150.
./build/debug/connection_demo client 47001
```

Ring and all-reduce examples expect a listening-port configuration on standard
input. Their Python integration launchers supply that configuration and manage
the complete process group.

## Failure behavior

Each all-reduce exchange round has a network deadline. Partial progress does
not restart it. The complete collective spans multiple rounds, so this is not
a single timeout for the entire collective.

Unexpected frame metadata is rejected before receiving its payload.
On an exchange failure, local ring connections are shut down.

After failure, callers must abandon the ring and its partially modified output.
The implementation does not recover failed ranks or reform the group.

## Current scope

- IPv4 loopback within one Linux environment.
- Fixed membership of 1 through 64 ranks.
- Sum over double-precision values.
- Sequential collective calls on each ring.
- Matching operation IDs and input lengths across ranks.
- At most 16,777,216 elements through the all-reduce API.
- Neighbor checks detect configuration errors; they do not authenticate peers.

Communication across physical machines has not been implemented or measured.
GPU communication, RDMA, and dynamic membership are outside this release.

## Continuous integration

GitHub Actions builds and runs the test suites using:

- GCC Debug with AddressSanitizer and UndefinedBehaviorSanitizer.
- Clang Debug with the same runtime checks.
- GCC Release.

Jobs preserve build information, diagnostic logs, and test results.

## Project layout

```text
include/collective/    Public interfaces
src/                  Library implementations
examples/             Runnable demonstrations
tests/                Correctness, transport, and failure tests
benchmarks/           Benchmark executable, launcher, and published evidence
.github/workflows/    Automated build and test configuration
```

## References

- [Open MPI all-reduce semantics](https://docs.open-mpi.org/en/main/man-openmpi/man3/MPI_Allreduce.3.html)
- [Baidu Research ring collective implementation](https://github.com/baidu-research/baidu-allreduce/blob/master/collectives.cu)
- [Linux TCP documentation](https://man7.org/linux/man-pages/man7/tcp.7.html)
- [Linux poll documentation](https://man7.org/linux/man-pages/man2/poll.2.html)