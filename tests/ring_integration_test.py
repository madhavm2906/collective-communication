"""
Launch independent C++ ranks and verify complete TCP rings.

Each rank binds its own dynamically assigned port before configuration.
The launcher supplies the complete port list only after all listeners exist.

Tests group sizes 1, 2, 3, 4, and 8.
All child processes are cleaned up on success, failure, or timeout.
"""

import pathlib
import select
import subprocess
import sys
import time
import unittest


DEMO = str(pathlib.Path(sys.argv.pop(1)).resolve())


def run_ring(world_size):
    processes = []

    try:
        for rank in range(world_size):
            process = subprocess.Popen(
                [DEMO, str(rank), str(world_size)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            processes.append(process)

        ports = []
        startup_deadline = time.monotonic() + 10

        for rank, process in enumerate(processes):
            remaining = startup_deadline - time.monotonic()

            if remaining <= 0:
                raise AssertionError("ring startup deadline expired")

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
                raise AssertionError(f"rank {rank}: invalid port {port}")

            ports.append(port)

        if len(set(ports)) != world_size:
            raise AssertionError("ranks announced duplicate listening ports")

        configuration = " ".join(str(port) for port in ports) + "\n"

        for process in processes:
            process.stdin.write(configuration)
            process.stdin.flush()
            process.stdin.close()

            # communicate() must not try flushing the already-closed pipe.
            process.stdin = None

        failures = []
        results = []

        for rank, process in enumerate(processes):
            stdout, stderr = process.communicate(timeout=15)

            expected = f"RANK {rank}: PASS world={world_size}"

            if process.returncode != 0 or expected not in stdout:
                failures.append(
                    f"rank={rank}, exit={process.returncode}\n"
                    f"stdout={stdout}\nstderr={stderr}"
                )

            results.append(stdout.strip())

        if failures:
            raise AssertionError("\n".join(failures))

        return results

    finally:
        # Stop every surviving child before waiting for cleanup.
        for process in processes:
            if process.poll() is None:
                process.kill()

        for process in processes:
            if process.stdin is not None:
                process.stdin.close()
                process.stdin = None

            process.communicate()


class RingIntegrationTests(unittest.TestCase):
    def test_single_rank(self):
        run_ring(1)

    def test_two_ranks(self):
        run_ring(2)

    def test_three_ranks(self):
        run_ring(3)

    def test_four_ranks(self):
        run_ring(4)

    def test_eight_ranks(self):
        run_ring(8)


if __name__ == "__main__":
    unittest.main(verbosity=2)