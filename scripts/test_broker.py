"""Local broker tests; requires mosquitto and a built benchmark."""

import asyncio
import contextlib
import os
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import time
import unittest
import uuid

BIN = Path(os.environ.get(
    "MQTT_RING_BENCH_BIN",
    Path(__file__).resolve().parents[1] / "target/debug/mqtt_ring_bench",
)).resolve()


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@contextlib.contextmanager
def process(command):
    with tempfile.TemporaryFile(mode="w+") as output:
        child = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT)
        try:
            yield child, output
        finally:
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
            output.seek(0)
            if child.returncode not in (0, -15):
                print(output.read())


def wait_port(child, port):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if child.poll() is not None:
            raise RuntimeError("Service exited before opening port {}".format(port))
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError("Service did not open port {}".format(port))


class BrokerTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.services = contextlib.ExitStack()
        cls.addClassCleanup(cls.services.close)
        cls.port = free_port()
        broker, _ = cls.services.enter_context(process(["mosquitto", "-p", str(cls.port)]))
        wait_port(broker, cls.port)

    def benchmark_command(self, *extra, clients=2):
        return [str(BIN), "--host", "127.0.0.1",
            "--port", str(self.port), "--clients", str(clients), "--messages", "4",
            "--workers", "1", "--interval", "1", "--ifaddr", "127.0.0.1",
            "--shutdown-mode", "graceful", *extra]

    def assert_stat(self, output, label, expected):
        found = re.search(r"^\s*" + re.escape(label) + r":\s*(\d+)\s*$", output, re.M)
        self.assertIsNotNone(found, output)
        self.assertEqual(int(found.group(1)), expected, output)

    async def test_benchmark_publishes_all_versions_and_qos(self):
        for version in (3, 5):
            for qos in (0, 1, 2):
                with self.subTest(version=version, qos=qos), process(self.benchmark_command(
                        "--mqtt-version", str(version), "--qos", str(qos))) as (child, output):
                    await asyncio.to_thread(child.wait, timeout=20)
                    output.seek(0)
                    text = output.read()
                    self.assertEqual(child.returncode, 0, text)
                    self.assert_stat(text, "Total Sent", 8)
                    if qos:
                        self.assert_stat(text, "Total Acked", 8)
                    self.assert_stat(text, "Errors", 0)

    async def test_benchmark_receives_subscribed_messages(self):
        topic = "coverage/bench/" + uuid.uuid4().hex
        with process(self.benchmark_command("--action", "sub", "--qos", "2",
                "--topic", topic + "/0")) as (child, output):
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                # Read without moving the file offset shared with the child.
                snapshot = os.pread(output.fileno(), os.fstat(output.fileno()).st_size, 0)
                if b"Subscribed: 2/2" in snapshot:
                    break
                self.assertIsNone(child.poll(), "Subscriber exited early")
                await asyncio.sleep(0.05)
            else:
                self.fail("Benchmark did not subscribe")
            publisher = await asyncio.to_thread(subprocess.run,
                self.benchmark_command("--qos", "2", "--topic", topic,
                    "--client-prefix", "publisher-", clients=1),
                capture_output=True, text=True, timeout=20)
            published = publisher.stdout + publisher.stderr
            self.assertEqual(publisher.returncode, 0, published)
            self.assert_stat(published, "Total Sent", 4)
            self.assert_stat(published, "Total Acked", 4)
            self.assert_stat(published, "Errors", 0)
            await asyncio.to_thread(child.wait, timeout=20)
            output.seek(0)
            text = output.read()
            self.assertEqual(child.returncode, 0, text)
            self.assert_stat(text, "Total Received", 8)
            self.assert_stat(text, "Errors", 0)


if __name__ == "__main__":
    unittest.main()
