"""
Exercise the C++ TCP example using independent processes.

Checks:
- C++ client/server array exchange.
- Input supplied through separate one-byte writes.
- Disconnect during an incomplete payload.
- Rejection of oversized message counts.
- Timeout when no client connects.
- Timeout when a connected client stops sending.
- Connection refusal when no server is listening.

Uses dynamically assigned ports and always cleans up child processes.
"""

import contextlib
import pathlib
import select
import socket
import struct
import subprocess
import sys
import time
import unittest


DEMO = str(pathlib.Path(sys.argv.pop(1)).resolve())


@contextlib.contextmanager
def server(timeout_ms=5000):
    process = subprocess.Popen(
        [DEMO, "server", "0", str(timeout_ms)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        ready, _, _ = select.select([process.stdout], [], [], 5)

        if not ready:
            raise AssertionError("server did not announce readiness")

        line = process.stdout.readline().strip()

        if not line.startswith("LISTENING 127.0.0.1:"):
            raise AssertionError(f"unexpected server startup: {line!r}")

        port = int(line.rsplit(":", 1)[1])
        yield process, port
    finally:
        if process.poll() is None:
            process.kill()

        process.communicate()


def finish(process, expected_code, expected_error=None):
    stdout, stderr = process.communicate(timeout=5)

    if process.returncode != expected_code:
        raise AssertionError(
            f"exit={process.returncode}, expected={expected_code}\n"
            f"stdout={stdout}\nstderr={stderr}"
        )

    if expected_error is not None and expected_error not in stderr:
        raise AssertionError(
            f"expected error {expected_error!r}, got {stderr!r}"
        )

    return stdout, stderr


def receive_exact(connection, count):
    result = bytearray()

    while len(result) < count:
        chunk = connection.recv(count - len(result))

        if not chunk:
            raise AssertionError("server closed before replying")

        result.extend(chunk)

    return bytes(result)


class TcpIntegrationTests(unittest.TestCase):
    def test_cpp_client_server(self):
        with server() as (process, port):
            client = subprocess.run(
                [DEMO, "client", str(port), "3000"],
                capture_output=True,
                text=True,
                timeout=5,
            )

            self.assertEqual(client.returncode, 0, client.stderr)
            self.assertIn("CLIENT: verified sum=150", client.stdout)

            stdout, _ = finish(process, 0)
            self.assertIn("SERVER: received 5 values; sum=150", stdout)

    def test_fragmented_input(self):
        with server() as (process, port):
            with socket.create_connection(("127.0.0.1", port), timeout=3) as peer:
                peer.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

                request = struct.pack("!6I", 5, 10, 20, 30, 40, 50)

                # Separate writes exercise incremental arrival.
                # TCP remains free to combine writes into stream segments.
                for value in request:
                    peer.sendall(bytes([value]))
                    time.sleep(0.002)

                reply = receive_exact(peer, 4)
                self.assertEqual(struct.unpack("!I", reply)[0], 150)

            finish(process, 0)

    def test_disconnect_during_payload(self):
        with server() as (process, port):
            with socket.create_connection(("127.0.0.1", port), timeout=3) as peer:
                # Declare three values, provide only one, then end sending.
                peer.sendall(struct.pack("!2I", 3, 10))
                peer.shutdown(socket.SHUT_WR)

                finish(
                    process,
                    1,
                    "peer closed connection before transfer completed",
                )

    def test_oversized_count(self):
        with server() as (process, port):
            with socket.create_connection(("127.0.0.1", port), timeout=3) as peer:
                peer.sendall(struct.pack("!I", 1025))
                finish(process, 1, "invalid element count")

    def test_missing_client_timeout(self):
        with server(timeout_ms=300) as (process, _):
            finish(process, 1, "deadline expired")

    def test_stalled_payload_timeout(self):
        with server(timeout_ms=1000) as (process, port):
            with socket.create_connection(("127.0.0.1", port), timeout=3) as peer:
                # Keep the connection open but never supply the payload.
                peer.sendall(struct.pack("!I", 5))
                finish(process, 1, "deadline expired")

    def test_connection_refused(self):
        # Reserve a port without listening, avoiding a free-port lookup race.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reserved:
            reserved.bind(("127.0.0.1", 0))
            port = reserved.getsockname()[1]

            client = subprocess.run(
                [DEMO, "client", str(port), "1000"],
                capture_output=True,
                text=True,
                timeout=5,
            )

            self.assertEqual(client.returncode, 1, client.stderr)
            self.assertIn("Connection refused", client.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)