"""Local broker tests; requires mosquitto and a built benchmark."""

import asyncio
import contextlib
import os
from pathlib import Path
import re
import signal
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
            if child.returncode not in (0, -15, 130):
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


async def read_packet(reader):
    header = (await reader.readexactly(1))[0]
    remaining = 0
    multiplier = 1
    for _ in range(4):
        byte = (await reader.readexactly(1))[0]
        remaining += (byte & 127) * multiplier
        if byte < 128:
            return header >> 4, await reader.readexactly(remaining)
        multiplier *= 128
    raise ValueError("Invalid MQTT remaining length")


@contextlib.asynccontextmanager
async def mock_broker(handler):
    result = asyncio.get_running_loop().create_future()

    async def accept(reader, writer):
        try:
            value = await handler(reader, writer)
            if not result.done():
                result.set_result(value)
        except Exception as error:
            if not result.done():
                result.set_exception(error)
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()

    server = await asyncio.start_server(accept, "127.0.0.1", 0)
    async with server:
        yield server.sockets[0].getsockname()[1], result


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

    async def test_publish_intervals_wake_without_keepalive_or_incoming_packets(self):
        async def handler(reader, writer):
            kind, _ = await read_packet(reader)
            self.assertEqual(kind, 1)
            writer.write(b"\x20\x03\x00\x00\x00")
            await writer.drain()
            received_at = []
            while True:
                kind, _ = await read_packet(reader)
                if kind == 14:
                    return received_at
                self.assertEqual(kind, 3)
                received_at.append(time.monotonic())

        async with mock_broker(handler) as (port, result):
            command = self.benchmark_command(clients=1)
            command[command.index("--port") + 1] = str(port)
            command[command.index("--messages") + 1] = "6"
            command[command.index("--interval") + 1] = "50"
            with process(command + ["--keep-alive", "0"]) as (child, output):
                times = await asyncio.wait_for(result, timeout=8)
                await asyncio.to_thread(child.wait, timeout=5)
                output.seek(0)
                text = output.read()
                self.assertEqual(child.returncode, 0, text)
                self.assert_stat(text, "Errors", 0)
                self.assertEqual(len(times), 6)
                self.assertGreaterEqual(times[-1] - times[0], 0.20)
                self.assertLess(times[-1] - times[0], 1.2)

    async def test_idle_subscriber_keeps_scheduling_pings(self):
        async def handler(reader, writer):
            kind, _ = await read_packet(reader)
            self.assertEqual(kind, 1)
            writer.write(b"\x20\x03\x00\x00\x00")
            await writer.drain()
            kind, body = await read_packet(reader)
            self.assertEqual(kind, 8)
            writer.write(b"\x90\x04" + body[:2] + b"\x00\x00")
            await writer.drain()
            for _ in range(2):
                kind, _ = await read_packet(reader)
                self.assertEqual(kind, 12)
                writer.write(b"\xd0\x00")
                await writer.drain()
            # Keep the connection open until the test requests shutdown.
            pings_done.set_result(None)
            await reader.read()

        pings_done = asyncio.get_running_loop().create_future()
        async with mock_broker(handler) as (port, result):
            command = self.benchmark_command("--action", "sub", "--keep-alive", "1", clients=1)
            command[command.index("--port") + 1] = str(port)
            command[command.index("--messages") + 1] = "0"
            with process(command) as (child, output):
                await asyncio.wait_for(asyncio.shield(pings_done), timeout=6)
                self.assertIsNone(child.poll())
                child.send_signal(signal.SIGINT)
                await asyncio.to_thread(child.wait, timeout=5)
                await asyncio.wait_for(result, timeout=2)
                output.seek(0)
                text = output.read()
                self.assertEqual(child.returncode, 130, text)
                self.assert_stat(text, "Errors", 0)

    async def test_drain_timeout_wakes_without_publish_acknowledgement(self):
        async def handler(reader, writer):
            kind, _ = await read_packet(reader)
            self.assertEqual(kind, 1)
            writer.write(b"\x20\x03\x00\x00\x00")
            await writer.drain()
            kind, _ = await read_packet(reader)
            self.assertEqual(kind, 3)
            sent_at = time.monotonic()
            # Deliberately withhold PUBACK and send no other packets.
            kind, _ = await read_packet(reader)
            self.assertEqual(kind, 14)
            return time.monotonic() - sent_at

        async with mock_broker(handler) as (port, result):
            command = self.benchmark_command("--qos", "1", "--keep-alive", "0", clients=1)
            command[command.index("--port") + 1] = str(port)
            command[command.index("--messages") + 1] = "1"
            with process(command) as (child, output):
                elapsed = await asyncio.wait_for(result, timeout=15)
                await asyncio.to_thread(child.wait, timeout=5)
                output.seek(0)
                text = output.read()
                self.assertEqual(child.returncode, 0, text)
                self.assert_stat(text, "Total Sent", 1)
                self.assert_stat(text, "Total Acked", 0)
                self.assert_stat(text, "Errors", 0)
                self.assertGreaterEqual(elapsed, 9.5)
                self.assertLess(elapsed, 13)

    async def test_connection_rate_below_worker_count_makes_progress(self):
        command = self.benchmark_command("--connect-rate", "1", clients=2)
        command[command.index("--workers") + 1] = "4"
        with process(command) as (child, output):
            await asyncio.to_thread(child.wait, timeout=8)
            output.seek(0)
            text = output.read()
            self.assertEqual(child.returncode, 0, text)
            self.assert_stat(text, "Total Sent", 8)
            self.assert_stat(text, "Errors", 0)

    async def test_refused_connections_finish_the_worker(self):
        command = self.benchmark_command()
        command[command.index("--port") + 1] = str(free_port())
        with process(command) as (child, output):
            await asyncio.to_thread(child.wait, timeout=5)
            output.seek(0)
            text = output.read()
            self.assert_stat(text, "Connect", 2)
            self.assert_stat(text, "Errors", 2)


if __name__ == "__main__":
    unittest.main()
