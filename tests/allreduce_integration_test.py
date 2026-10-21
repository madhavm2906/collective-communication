"""
Validate the C++ ring all-reduce using independent processes.

Checks empty vectors, zero-length chunks, uneven partitions,
repeated operations, randomized configurations, large arrays,
and rejection of inconsistent input lengths.

Every successful C++ rank compares its result against reference_sum.
"""

import pathlib
import random
import select
import subprocess
import sys
import time
import unittest


DEMO = str(pathlib.Path(sys.argv.pop(1)).resolve())


def run_group(world_size, count, seed=2027, iterations=3, expect_failure=False):
    counts = count if isinstance(count, list) else [count] * world_size

    if len(counts) != world_size:
        raise ValueError("one element count is required per rank")

    processes = []

    try:
        for rank in range(world_size):
            process = subprocess.Popen(
                [
                    DEMO,
                    str(rank),
                    str(world_size),
                    str(counts[rank]),
                    str(seed),
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
                raise AssertionError("allreduce startup deadline expired")

            ready, _, _ = select.select(
                [process.stdout], [], [], remaining
            )

            if not ready:
                raise AssertionError(f"rank {rank} did not become ready")

            line = process.stdout.readline().strip()
            fields = line.split()

            if (
                len(fields) != 3
                or fields[0] != "READY"
                or fields[1] != str(rank)
            ):
                raise AssertionError(
                    f"rank {rank}: unexpected startup output {line!r}"
                )

            port = int(fields[2])

            if not 1 <= port <= 65535:
                raise AssertionError(f"rank {rank}: invalid port")

            ports.append(port)

        if len(set(ports)) != world_size:
            raise AssertionError("duplicate listening ports")

        configuration = " ".join(str(port) for port in ports) + "\n"

        for process in processes:
            process.stdin.write(configuration)
            process.stdin.flush()
            process.stdin.close()
            process.stdin = None

        results = []

        for rank, process in enumerate(processes):
            stdout, stderr = process.communicate(timeout=30)
            results.append((rank, process.returncode, stdout, stderr))

        if expect_failure:
            if any(code != 1 for _, code, _, _ in results):
                raise AssertionError(
                    f"expected clean failures from every rank: {results}"
                )

            if not any(
                "allreduce frame metadata mismatch" in stderr
                for _, _, _, stderr in results
            ):
                raise AssertionError(
                    f"missing metadata mismatch diagnosis: {results}"
                )

            return

        for rank, code, stdout, stderr in results:
            expected = (
                f"RANK {rank}: PASS world={world_size} "
                f"count={counts[rank]} iterations={iterations}"
            )

            if code != 0 or expected not in stdout:
                raise AssertionError(
                    f"rank={rank}, exit={code}\n"
                    f"stdout={stdout}\nstderr={stderr}"
                )

    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()

        for process in processes:
            if process.stdin is not None:
                process.stdin.close()
                process.stdin = None

            process.communicate()


class AllreduceIntegrationTests(unittest.TestCase):
    def test_boundary_lengths(self):
        for world_size in (1, 2, 3, 4, 8):
            lengths = sorted(
                {0, 1, world_size - 1, world_size, world_size + 1, 17, 257}
            )

            for count in lengths:
                with self.subTest(world=world_size, count=count):
                    run_group(world_size, count)

    def test_randomized_configurations(self):
        generator = random.Random(2906)

        for case in range(20):
            world_size = generator.randint(2, 8)
            count = generator.randint(0, 4096)
            seed = generator.randint(0, 1000000)

            with self.subTest(case=case, world=world_size, count=count):
                run_group(world_size, count, seed=seed, iterations=2)

    def test_large_arrays(self):
        for world_size, count in ((2, 524301), (4, 262147)):
            with self.subTest(world=world_size, count=count):
                run_group(world_size, count, iterations=2)

    def test_mismatched_lengths_are_rejected(self):
        run_group(
            2,
            [4, 5],
            iterations=1,
            expect_failure=True,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)