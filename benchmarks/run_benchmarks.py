"""
Run independent multi-process all-reduce benchmarks.

Requires a clean Git working tree and a Release build without sanitizers.
Each independent run creates a fresh process group and performs warmups.

Statistics use the maximum rank elapsed time for each aligned iteration.
p95 uses the nearest-rank percentile definition.
These samples are not a measurement of physical network bandwidth.
"""

import argparse
import csv
import datetime
import hashlib
import json
import math
import os
import pathlib
import platform
import select
import statistics
import subprocess
import time


ROOT = pathlib.Path(__file__).resolve().parents[1]


def command_output(arguments):
    return subprocess.check_output(
        arguments,
        cwd=ROOT,
        text=True,
        stderr=subprocess.STDOUT,
    ).strip()


def read_cache(build_dir):
    result = {}

    for line in (build_dir / "CMakeCache.txt").read_text().splitlines():
        if line.startswith(("#", "//")) or "=" not in line:
            continue

        declaration, value = line.split("=", 1)
        name = declaration.split(":", 1)[0]
        result[name] = value

    return result


def cpu_model():
    path = pathlib.Path("/proc/cpuinfo")

    if path.exists():
        for line in path.read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()

    return platform.processor()


def run_group(binary, world_size, elements, warmup, iterations):
    processes = []

    try:
        for rank in range(world_size):
            process = subprocess.Popen(
                [
                    str(binary),
                    str(rank),
                    str(world_size),
                    str(elements),
                    str(warmup),
                    str(iterations),
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            processes.append(process)

        ports = []
        startup_deadline = time.monotonic() + 15

        for rank, process in enumerate(processes):
            remaining = startup_deadline - time.monotonic()

            if remaining <= 0:
                raise RuntimeError("benchmark startup deadline expired")

            ready, _, _ = select.select(
                [process.stdout], [], [], remaining
            )

            if not ready:
                raise RuntimeError(f"rank {rank} did not become ready")

            line = process.stdout.readline().strip()
            fields = line.split()

            if (
                len(fields) != 3
                or fields[0] != "READY"
                or fields[1] != str(rank)
            ):
                raise RuntimeError(
                    f"rank {rank}: unexpected startup output {line!r}"
                )

            port = int(fields[2])

            if not 1 <= port <= 65535:
                raise RuntimeError("invalid announced port")

            ports.append(port)

        if len(set(ports)) != world_size:
            raise RuntimeError("duplicate announced ports")

        configuration = " ".join(map(str, ports)) + "\n"

        for process in processes:
            process.stdin.write(configuration)
            process.stdin.flush()
            process.stdin.close()
            process.stdin = None

        results = []
        run_deadline = time.monotonic() + max(
            60, 2 * (warmup + iterations)
        )

        for rank, process in enumerate(processes):
            remaining = run_deadline - time.monotonic()

            if remaining <= 0:
                raise RuntimeError("benchmark process-group deadline expired")

            stdout, stderr = process.communicate(timeout=remaining)

            if process.returncode != 0:
                raise RuntimeError(
                    f"rank {rank} exited with {process.returncode}\n"
                    f"stdout={stdout}\nstderr={stderr}"
                )

            record = json.loads(stdout.strip())

            if (
                record["rank"] != rank
                or record["world_size"] != world_size
                or record["elements"] != elements
            ):
                raise RuntimeError("benchmark result metadata mismatch")

            samples = record["samples_us"]

            if len(samples) != iterations or any(
                not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
                for value in samples
            ):
                raise RuntimeError("invalid benchmark samples")

            results.append(samples)

        return results

    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()

        for process in processes:
            if process.stdin is not None:
                process.stdin.close()
                process.stdin = None

            process.communicate()


def positive_integer(text):
    value = int(text)

    if value <= 0:
        raise argparse.ArgumentTypeError("must be positive")

    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("--build-dir", default="build/release")
    parser.add_argument(
        "--ranks", nargs="+", type=positive_integer, default=[2, 4, 8]
    )
    parser.add_argument(
        "--elements",
        nargs="+",
        type=positive_integer,
        default=[1024, 8192, 131072],
    )
    parser.add_argument("--warmup", type=positive_integer, default=5)
    parser.add_argument("--iterations", type=positive_integer, default=50)
    parser.add_argument("--runs", type=positive_integer, default=3)
    parser.add_argument("--output", required=True)

    args = parser.parse_args()

    if (
        max(args.ranks) > 64
        or max(args.elements) > 1048576
        or args.warmup > 1000
        or args.iterations > 10000
    ):
        parser.error("configuration exceeds benchmark worker limits")

    build_dir = (ROOT / args.build_dir).resolve()
    binary = build_dir / "allreduce_benchmark"
    cache = read_cache(build_dir)

    if cache.get("CMAKE_BUILD_TYPE") != "Release":
        raise SystemExit("Use a Release build for measurements.")

    if cache.get("COLLECTIVE_ENABLE_SANITIZERS", "").upper() not in (
        "OFF", "FALSE", "NO", "0"
    ):
        raise SystemExit("Disable sanitizers before measuring.")

    if not binary.is_file():
        raise SystemExit("Build allreduce_benchmark before running.")

    dirty = command_output(["git", "status", "--porcelain"])

    if dirty:
        raise SystemExit(
            "Commit your source changes before measuring so results "
            "identify an exact Git revision.\n" + dirty
        )

    compiler = cache["CMAKE_CXX_COMPILER"]

    metadata = {
        "created_utc": datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat(),
        "git_revision": command_output(["git", "rev-parse", "HEAD"]),
        "working_tree_clean": True,
        "platform": platform.platform(),
        "cpu_model": cpu_model(),
        "logical_cpu_count": os.cpu_count(),
        "allowed_cpus": sorted(os.sched_getaffinity(0)),
        "python_version": platform.python_version(),
        "compiler": command_output([compiler, "--version"]),
        "build_type": cache["CMAKE_BUILD_TYPE"],
        "cxx_flags": cache.get("CMAKE_CXX_FLAGS", ""),
        "release_flags": cache.get("CMAKE_CXX_FLAGS_RELEASE", ""),
        "sanitizers": cache.get("COLLECTIVE_ENABLE_SANITIZERS"),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "configuration": vars(args),
        "measurement": (
            "Maximum per-rank allreduce_sum elapsed time per iteration. "
            "Startup, input preparation, synchronization, and output "
            "verification excluded. Local loopback; no CPU pinning."
        ),
        "percentile_definition": "nearest-rank",
    }

    output = (ROOT / args.output).resolve()

    # Preserve existing experiments instead of silently overwriting them.
    output.mkdir(parents=True, exist_ok=False)

    (output / "environment.json").write_text(
        json.dumps(metadata, indent=2) + "\n"
    )

    with (
        (output / "raw_samples.csv").open("w", newline="") as raw_file,
        (output / "iteration_maxima.csv").open("w", newline="") as max_file,
        (output / "summary.csv").open("w", newline="") as summary_file,
    ):
        raw = csv.writer(raw_file)
        maxima_writer = csv.writer(max_file)
        summary = csv.writer(summary_file)

        raw.writerow(
            ["ranks", "elements", "run", "iteration", "rank", "elapsed_us"]
        )

        maxima_writer.writerow(
            ["ranks", "elements", "run", "iteration", "max_rank_us"]
        )

        summary.writerow(
            [
                "ranks", "elements", "payload_bytes", "run", "samples",
                "median_us", "p95_us", "min_us", "max_us"
            ]
        )

        for world_size in args.ranks:
            for elements in args.elements:
                for run in range(1, args.runs + 1):
                    samples = run_group(
                        binary,
                        world_size,
                        elements,
                        args.warmup,
                        args.iterations,
                    )

                    maxima = []

                    for iteration in range(args.iterations):
                        values = [
                            samples[rank][iteration]
                            for rank in range(world_size)
                        ]

                        for rank, value in enumerate(values):
                            raw.writerow(
                                [
                                    world_size, elements, run,
                                    iteration, rank, value
                                ]
                            )

                        maximum = max(values)
                        maxima.append(maximum)

                        maxima_writer.writerow(
                            [
                                world_size, elements, run,
                                iteration, maximum
                            ]
                        )

                    ordered = sorted(maxima)
                    median = statistics.median(ordered)
                    p95 = ordered[math.ceil(0.95 * len(ordered)) - 1]

                    summary.writerow(
                        [
                            world_size,
                            elements,
                            elements * 8,
                            run,
                            len(ordered),
                            median,
                            p95,
                            ordered[0],
                            ordered[-1],
                        ]
                    )

                    raw_file.flush()
                    max_file.flush()
                    summary_file.flush()

                    print(
                        f"ranks={world_size} elements={elements} "
                        f"run={run} median={median:.3f} us "
                        f"p95={p95:.3f} us",
                        flush=True,
                    )

    print(f"Saved results to {output}")


if __name__ == "__main__":
    main()