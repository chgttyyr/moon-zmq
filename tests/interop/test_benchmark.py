"""
Cross-language benchmark test suite between moon-zmq (pure MoonBit)
and official libzmq via pyzmq.

Measures:
1. PUSH/PULL pipeline throughput (msgs/sec and MB/sec) across payload sizes (64B, 1KB, 64KB, 1MB).
2. REQ/REP round-trip latency percentiles (min, p50, p95, p99, max, avg) across payload sizes.
"""

import os
import sys
import time
import subprocess
import unittest
import zmq

if sys.platform == "win32":
    import asyncio
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass


def get_interop_bin():
    candidates = [
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "_build", "native", "debug", "build", "cmd", "interop", "interop.exe")),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "_build", "native", "debug", "build", "cmd", "interop", "interop")),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "_build", "native", "release", "build", "cmd", "interop", "interop.exe")),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "_build", "native", "release", "build", "cmd", "interop", "interop")),
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    raise FileNotFoundError(f"interop binary not found in: {candidates}")


class TestZmtpBenchmark(unittest.TestCase):
    base_port = 25000

    @classmethod
    def setUpClass(cls):
        cls.bin_path = get_interop_bin()

    def get_port(self):
        TestZmtpBenchmark.base_port += 1
        return TestZmtpBenchmark.base_port

    def test_interop_push_pull_throughput(self):
        """pyzmq PUSH -> moon-zmq PULL pipeline throughput across sizes."""
        tiers = [
            ("64B", 64, 2000),
            ("1KB", 1024, 1000),
            ("64KB", 65536, 100),
            ("1MB", 1048576, 10),
        ]

        for name, size, count in tiers:
            port = self.get_port()
            proc = subprocess.Popen(
                [self.bin_path, "bench_sink", str(port), str(count)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            time.sleep(0.3)
            try:
                ctx = zmq.Context()
                sock = ctx.socket(zmq.PUSH)
                sock.connect(f"tcp://127.0.0.1:{port}")
                payload = b"X" * size

                t_start = time.perf_counter()
                for _ in range(count):
                    sock.send(payload)

                out, _ = proc.communicate(timeout=10)
                t_end = time.perf_counter()
                self.assertIn("SINK_DONE", out)

                duration = max(t_end - t_start, 0.0001)
                msgs_per_sec = count / duration
                mb_per_sec = (size * count) / (1024 * 1024 * duration)
                print(f"[INTEROP BENCHMARK] pyzmq -> moon_zmq PUSH/PULL {name}: "
                      f"{count} msgs in {duration*1000:.1f}ms => {msgs_per_sec:.0f} msg/s, {mb_per_sec:.2f} MB/s")
                sock.close()
                ctx.term()
            finally:
                if proc.poll() is None:
                    proc.kill()

    def test_interop_req_rep_latency(self):
        """pyzmq REQ <-> moon-zmq REP round-trip latency percentiles."""
        tiers = [
            ("64B", 64, 300),
            ("1KB", 1024, 150),
            ("64KB", 65536, 40),
            ("1MB", 1048576, 10),
        ]

        for name, size, count in tiers:
            port = self.get_port()
            proc = subprocess.Popen(
                [self.bin_path, "bench_echo", str(port)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            time.sleep(0.3)
            try:
                ctx = zmq.Context()
                sock = ctx.socket(zmq.REQ)
                sock.connect(f"tcp://127.0.0.1:{port}")
                payload = b"L" * size

                latencies = []
                for _ in range(count):
                    t0 = time.perf_counter()
                    sock.send(payload)
                    rep = sock.recv()
                    t1 = time.perf_counter()
                    self.assertEqual(len(rep), size)
                    latencies.append((t1 - t0) * 1000.0)

                # Shutdown echo server cleanly
                sock.send(b"QUIT")
                _ = sock.recv()
                sock.close()
                ctx.term()
                proc.wait(timeout=5)

                latencies.sort()
                p50 = latencies[int(len(latencies) * 0.50)]
                p95 = latencies[int(len(latencies) * 0.95)]
                p99 = latencies[int(len(latencies) * 0.99)]
                min_l = latencies[0]
                max_l = latencies[-1]
                avg_l = sum(latencies) / len(latencies)

                print(f"[INTEROP BENCHMARK] pyzmq <-> moon_zmq REQ/REP {name} (n={count}): "
                      f"min={min_l:.2f}ms, p50={p50:.2f}ms, p95={p95:.2f}ms, p99={p99:.2f}ms, max={max_l:.2f}ms, avg={avg_l:.2f}ms")
            finally:
                if proc.poll() is None:
                    proc.kill()


if __name__ == "__main__":
    unittest.main()
