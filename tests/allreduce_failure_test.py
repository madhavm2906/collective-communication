"""
Inject failures after a four-process collective begins exchanging traffic.

Coordinates startup without arbitrary sleeps.
Checks that healthy ranks report expected errors and exit.
For a stalled peer, keeps its sockets open until healthy ranks finish.

All subprocesses are cleaned up even if a test fails.
"""

import pathlib
import select
import subprocess
import sys
import unittest


WORKER = str(pathlib.Path(sys.argv.pop(1)).resolve())


def read_line(process, timeout=10):
    ready, _, _ = select.select([process.stdout], [], [], timeout)

    if not ready:
        raise AssertionError("worker did not produce expected output")

    line = process.stdout.readline().strip()

    if not line:
        raise AssertionError(
            f"worker ended before expected output; exit={process.poll()}"
        )

    return line


def close_input(process):
    if process.stdin is not None:
        process.stdin.close()
        process.stdin = None


def run_failure(mode):
    world_size = 4
    faulty_rank = 1
    expected_fault_exit = {
        "drop": 42,
        "stall": 43,
        "wrong-op": 44,
    }[mode]

    processes = []

    try:
        for rank in range(world_size):
            worker_mode = mode if rank == faulty_rank else "normal"

            process = subprocess.Popen(
                [WORKER, str(rank), str(world_size), worker_mode],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            processes.append(process)

        ports = []

        for rank, process in enumerate(processes):
            line = read_line(process)
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
                raise AssertionError("invalid announced port")

            ports.append(port)

        if len(set(ports)) != world_size:
            raise AssertionError("duplicate listening ports")

        configuration = " ".join(str(port) for port in ports) + "\n"

        for process in processes:
            process.stdin.write(configuration)
            process.stdin.flush()

        for rank, process in enumerate(processes):
            line = read_line(process)

            if line != f"CONNECTED {rank}":
                raise AssertionError(
                    f"rank {rank}: unexpected connection output {line!r}"
                )

        for rank, process in enumerate(processes):
            process.stdin.write("GO\n")
            process.stdin.flush()

            if rank != faulty_rank or mode != "stall":
                close_input(process)

        fault = processes[faulty_rank]
        observed = read_line(fault)

        if observed != f"FAULT_OBSERVED rank={faulty_rank}":
            raise AssertionError(
                f"fault was not synchronized with collective traffic: {observed}"
            )

        messages = []

        for rank, process in enumerate(processes):
            if rank == faulty_rank:
                continue

            stdout, stderr = process.communicate(timeout=10)

            if (
                process.returncode != 0
                or f"EXPECTED_FAILURE rank={rank}" not in stdout
            ):
                raise AssertionError(
                    f"healthy rank {rank} failed incorrectly\n"
                    f"exit={process.returncode}\n"
                    f"stdout={stdout}\nstderr={stderr}"
                )

            messages.append(stdout)

        if mode == "stall":
            # The faulty process is still connected: failures above must
            # have happened without waiting for it to close its sockets.
            if fault.poll() is not None:
                raise AssertionError("stalled worker exited prematurely")

            if not any("deadline expired" in text for text in messages):
                raise AssertionError(
                    f"expected a deadline diagnosis: {messages}"
                )

            fault.stdin.write("RELEASE\n")
            fault.stdin.flush()
            close_input(fault)

        if mode == "wrong-op":
            if not any(
                "allreduce frame metadata mismatch" in text
                for text in messages
            ):
                raise AssertionError(
                    f"expected operation-ID rejection: {messages}"
                )

        stdout, stderr = fault.communicate(timeout=10)

        if fault.returncode != expected_fault_exit:
            raise AssertionError(
                f"fault worker exit={fault.returncode}, "
                f"expected={expected_fault_exit}\n"
                f"stdout={stdout}\nstderr={stderr}"
            )

    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()

        for process in processes:
            close_input(process)
            process.communicate()


class AllreduceFailureTests(unittest.TestCase):
    def test_abrupt_rank_exit(self):
        run_failure("drop")

    def test_connected_rank_stalls(self):
        run_failure("stall")

    def test_wrong_operation_id(self):
        run_failure("wrong-op")


if __name__ == "__main__":
    unittest.main(verbosity=2)