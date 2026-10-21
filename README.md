# collective-communication
C++ collective communication library implementing ring all-reduce over TCP, with correctness tests and reproducible benchmarks.
<!--
Describes the implemented library, build instructions, test coverage,
algorithm, design choices, and current limitations.
-->

# Collective Communication Library

[![CI](https://github.com/madhavm2906/collective-communication/actions/workflows/ci.yml/badge.svg)](https://github.com/madhavm2906/collective-communication/actions/workflows/ci.yml)

A C++17 library implementing floating-point ring all-reduce over TCP.

Independent Linux processes exchange array chunks using reduce-scatter
followed by all-gather. Each participating rank receives the elementwise
sum of every rank's input.

The project includes deadline-bounded socket operations, protocol validation,
multi-process correctness tests, and sanitizer-enabled builds.

## Example operation

```text
Rank 0 input: [1, 2]
Rank 1 input: [3, 4]
Rank 2 input: [5, 6]

Result on every rank: [9, 12]
```

A rank is a process's identifier within the group.

All ranks must supply the same number of elements. The library supports
uneven chunk partitions when that number is not divisible by the group size.

## Implemented features

- Move-only socket ownership with automatic cleanup.
- Nonblocking IPv4 loopback connections.
- Complete send and receive loops that handle partial transfers.
- Steady-clock deadlines that do not restart after partial progress.
- Peer-disconnect detection and bounded waiting under backpressure.
- Ring startup with protocol and neighbor identity validation.
- In-place sum all-reduce for IEEE-754 binary64 values.
- Reduce-scatter and all-gather with concurrent sending and receiving.
- Empty vectors, zero-length chunks, and single-rank groups.
- Repeated collective operations on the same ring.
- Frame checks for operation ID, phase, round, chunk, and lengths.
- Network-byte-order encoding for metadata and floating-point bit patterns.
- Scratch buffers reused across rounds within a collective.

## Requirements

- Linux, including Ubuntu under WSL 2.
- A C++17 compiler such as GCC or Clang.
- CMake 3.20 or newer.
- Ninja.
- Python 3 for integration-test orchestration.
- POSIX threads.

On Ubuntu:

```bash
# Install the dependencies needed to build and run the tests.
sudo apt update
sudo apt install -y build-essential cmake ninja-build python3
```

## Build and test

### Debug with runtime checks

```bash
# Configure a Debug build with AddressSanitizer and UndefinedBehaviorSanitizer.
cmake -S . -B build/debug -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCOLLECTIVE_ENABLE_SANITIZERS=ON

# Compile the library, examples, and tests.
cmake --build build/debug

# Run every registered test suite.
ctest --test-dir build/debug --output-on-failure
```

### Release

```bash
# Configure an optimized build without sanitizers.
cmake -S . -B build/release -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCOLLECTIVE_ENABLE_SANITIZERS=OFF

# Compile and verify the optimized build.
cmake --build build/release
ctest --test-dir build/release --output-on-failure
```

## Test suites

| Suite | Coverage |
|---|---|
| `reference_sum` | Known sums, boundary cases, invalid inputs, and deterministic randomized inputs |
| `socket_ownership` | Creation, destruction, move ownership, reset, and close-on-exec |
| `tcp_transfer` | Large byte transfers, empty transfers, disconnects, backpressure, and deadline behavior |
| `tcp_integration` | Independent client/server processes, fragmented writes, invalid lengths, and timeouts |
| `ring_integration` | Ring formation and token circulation with 1, 2, 3, 4, and 8 ranks |
| `allreduce_integration` | Numerical validation, uneven partitions, repeated operations, randomized configurations, large arrays, and mismatched lengths |

Every successful all-reduce test rank compares its output against a local
reference sum using absolute and relative floating-point tolerances.

```bash
# Display detailed multi-process all-reduce results.
ctest --test-dir build/debug -R allreduce_integration --verbose
```

## Run the array-transfer example

Open two terminals in the repository directory.

Terminal 1:

```bash
# Start a server that accepts one array request.
./build/debug/connection_demo server 47001
```

Terminal 2, within ten seconds:

```bash
# Send five integers and verify that the server returns their sum.
./build/debug/connection_demo client 47001
```

The expected sum is 150.

The ring and all-reduce integration launchers start all required processes,
collect their listening ports, and supply the complete configuration.
The corresponding C++ examples expect that configuration on standard input.

## Algorithm

Ranks connect in a directed ring:

```text
0 -> 1 -> 2 -> ... -> P-1 -> 0
```

Each rank sends to its successor and receives from its predecessor.

### Reduce-scatter

The input vector is partitioned into P chunks.

For P-1 rounds, each rank sends one chunk and adds an incoming chunk to
its local values. At completion, each rank holds one fully reduced chunk.

### All-gather

For another P-1 rounds, ranks forward completed chunks and copy received
chunks into their output vectors.

Every rank finishes with the complete sum.

### Avoiding communication deadlock

Large sends can exhaust socket buffers. If every rank finishes sending
before beginning to receive, the ring can stop making progress.

The current implementation uses a sender task while the calling thread
receives. Each round joins its sender before reusing the send buffer.

This is the initial implementation. Thread creation, encoding, allocation,
and communication overhead remain candidates for profiling.

## Failure behavior

Connection and transfer operations have bounded waiting.

Each all-reduce exchange round uses a shared deadline for its network work.
The complete collective spans multiple rounds, so the round timeout is
not a single timeout for the entire collective.

Malformed or unexpected frame metadata is rejected before its payload
is accepted. On an all-reduce exchange failure, the local connections
are shut down.

After a failed collective, callers must abandon the ring and the partially
modified output. The library does not recover failed ranks or reform groups.

## Current scope and limitations

- Connections currently use IPv4 loopback within one Linux environment.
- Current tests do not demonstrate communication across physical machines.
- Groups contain between 1 and 64 ranks.
- The reduction operation is sum over double-precision values.
- Collective calls on a ring must be sequential.
- The all-reduce API currently limits inputs to 16,777,216 elements.
- Operation IDs and input lengths must agree across ranks.
- Neighbor validation detects configuration errors; it is not authentication.
- GPU communication, RDMA, and dynamic membership are outside the current implementation.

No performance improvement or bandwidth claims are published yet.
Reproducible benchmarks and profiling are the next performance milestone.

## Continuous integration

GitHub Actions builds and runs the test suites with:

- GCC Debug and runtime sanitizers.
- Clang Debug and runtime sanitizers.
- GCC Release.

Each job retains compiler details, build logs, and test results.

## Project layout

```text
include/collective/    Public C++ interfaces
src/                  Library implementations
examples/             Executable demonstrations
tests/                Unit and process-based integration tests
.github/workflows/    Automated build and test configuration
```

## References

- [Open MPI all-reduce semantics](https://docs.open-mpi.org/en/main/man-openmpi/man3/MPI_Allreduce.3.html)
- [Baidu Research ring collective implementation](https://github.com/baidu-research/baidu-allreduce/blob/master/collectives.cu)
- [Linux socket API documentation](https://man7.org/linux/man-pages/man7/socket.7.html)