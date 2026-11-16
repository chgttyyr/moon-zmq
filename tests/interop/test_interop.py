"""
Bidirectional interoperability test suite between moon-zmq (pure MoonBit)
and official libzmq via pyzmq.

Covers:
- REQ/REP (both directions)
- PUB/SUB (both directions)
- PUSH/PULL (both directions)
- ROUTER/DEALER (both directions)
- PAIR (both directions)
- Multipart messages (both directions)
- Large message frames (512 KB, both directions)
- PLAIN security mechanism (RFC 24, both directions)
- Automatic reconnection with backoff
"""

import os
import sys
import time
import subprocess
import unittest
import zmq
from zmq.auth.thread import ThreadAuthenticator

if sys.platform == "win32":
    import asyncio
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass


def get_interop_bin():
    # Search in default build directory (MoonBit produces .exe on all native platforms)
    candidates = [
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "_build", "native", "debug", "build", "cmd", "interop", "interop.exe")),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "_build", "native", "debug", "build", "cmd", "interop", "interop")),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "_build", "native", "release", "build", "cmd", "interop", "interop.exe")),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "_build", "native", "release", "build", "cmd", "interop", "interop")),
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    raise FileNotFoundError(f"interop binary not found in candidate paths: {candidates}")


class TestZmtpInterop(unittest.TestCase):
    base_port = 20000

    @classmethod
    def setUpClass(cls):
        cls.bin_path = get_interop_bin()

    def get_port(self):
        TestZmtpInterop.base_port += 1
        return TestZmtpInterop.base_port

    # -------------------------------------------------------------
    # 1. REQ / REP
    # -------------------------------------------------------------
    def test_req_rep_moonbit_server(self):
        """MoonBit REP server <-> Python REQ client"""
        port = self.get_port()
        proc = subprocess.Popen([self.bin_path, "req_server", str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(0.3)
        try:
            ctx = zmq.Context()
            sock = ctx.socket(zmq.REQ)
            sock.connect(f"tcp://127.0.0.1:{port}")
            sock.send(b"ping_from_python")
            rep = sock.recv()
            self.assertEqual(rep, b"echo:ping_from_python")
            sock.close()
            ctx.term()
            proc.wait(timeout=5)
            self.assertEqual(proc.returncode, 0)
        finally:
            if proc.poll() is None:
                proc.kill()

    def test_req_rep_python_server(self):
        """Python REP server <-> MoonBit REQ client"""
        port = self.get_port()
        ctx = zmq.Context()
        sock = ctx.socket(zmq.REP)
        sock.bind(f"tcp://127.0.0.1:{port}")
        time.sleep(0.1)

        proc = subprocess.Popen([self.bin_path, "req_client", str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            req = sock.recv()
            self.assertEqual(req, b"hello_from_mbt")
            sock.send(b"rep_from_python")
            out, _ = proc.communicate(timeout=5)
            self.assertIn("REPLY:rep_from_python", out)
            self.assertEqual(proc.returncode, 0)
        finally:
            sock.close()
            ctx.term()
            if proc.poll() is None:
                proc.kill()

    # -------------------------------------------------------------
    # 2. PUB / SUB
    # -------------------------------------------------------------
    def test_pub_sub_moonbit_server(self):
        """MoonBit PUB server <-> Python SUB client"""
        port = self.get_port()
        ctx = zmq.Context()
        sub = ctx.socket(zmq.SUB)
        sub.setsockopt(zmq.SUBSCRIBE, b"alerts")
        sub.connect(f"tcp://127.0.0.1:{port}")

        proc = subprocess.Popen([self.bin_path, "pub_server", str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            # Python subscriber should receive alert but not metrics
            msg = sub.recv()
            self.assertEqual(msg, b"alerts.disk Disk full")
            sub.close()
            ctx.term()
            proc.wait(timeout=5)
            self.assertEqual(proc.returncode, 0)
        finally:
            if proc.poll() is None:
                proc.kill()

    def test_pub_sub_python_server(self):
        """Python PUB server <-> MoonBit SUB client"""
        port = self.get_port()
        ctx = zmq.Context()
        pub = ctx.socket(zmq.PUB)
        pub.bind(f"tcp://127.0.0.1:{port}")

        proc = subprocess.Popen([self.bin_path, "sub_client", str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(0.3)
        try:
            pub.send(b"alerts.fire Fire in server room")
            out, _ = proc.communicate(timeout=5)
            self.assertIn("SUB_RECV:alerts.fire Fire in server room", out)
            self.assertEqual(proc.returncode, 0)
        finally:
            pub.close()
            ctx.term()
            if proc.poll() is None:
                proc.kill()

    # -------------------------------------------------------------
    # 3. PUSH / PULL
    # -------------------------------------------------------------
    def test_push_pull_moonbit_server(self):
        """MoonBit PUSH server <-> Python PULL client"""
        port = self.get_port()
        ctx = zmq.Context()
        pull = ctx.socket(zmq.PULL)
        pull.connect(f"tcp://127.0.0.1:{port}")

        proc = subprocess.Popen([self.bin_path, "push_server", str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            msg = pull.recv()
            self.assertEqual(msg, b"task_data_42")
            pull.close()
            ctx.term()
            proc.wait(timeout=5)
            self.assertEqual(proc.returncode, 0)
        finally:
            if proc.poll() is None:
                proc.kill()

    def test_push_pull_python_server(self):
        """Python PUSH server <-> MoonBit PULL client"""
        port = self.get_port()
        ctx = zmq.Context()
        push = ctx.socket(zmq.PUSH)
        push.bind(f"tcp://127.0.0.1:{port}")

        proc = subprocess.Popen([self.bin_path, "pull_client", str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(0.3)
        try:
            push.send(b"task_from_py_push")
            out, _ = proc.communicate(timeout=5)
            self.assertIn("PULLED:task_from_py_push", out)
            self.assertEqual(proc.returncode, 0)
        finally:
            push.close()
            ctx.term()
            if proc.poll() is None:
                proc.kill()

    def test_pull_server_moonbit(self):
        """MoonBit PULL server <-> Python PUSH client"""
        port = self.get_port()
        proc = subprocess.Popen([self.bin_path, "pull_server", str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(0.3)
        try:
            ctx = zmq.Context()
            push = ctx.socket(zmq.PUSH)
            push.connect(f"tcp://127.0.0.1:{port}")
            push.send(b"push_to_mbt_server")
            push.close()
            ctx.term()
            out, _ = proc.communicate(timeout=5)
            self.assertIn("PULLED:push_to_mbt_server", out)
            self.assertEqual(proc.returncode, 0)
        finally:
            if proc.poll() is None:
                proc.kill()

    # -------------------------------------------------------------
    # 4. ROUTER / DEALER
    # -------------------------------------------------------------
    def test_router_dealer_moonbit_server(self):
        """MoonBit ROUTER server <-> Python DEALER client"""
        port = self.get_port()
        proc = subprocess.Popen([self.bin_path, "router_server", str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(0.3)
        try:
            ctx = zmq.Context()
            dealer = ctx.socket(zmq.DEALER)
            dealer.connect(f"tcp://127.0.0.1:{port}")
            dealer.send_multipart([b"", b"hello_router"])
            rep = dealer.recv_multipart()
            # Reply has empty delim + payload
            self.assertEqual(rep[1], b"router_echo:hello_router")
            dealer.close()
            ctx.term()
            proc.wait(timeout=5)
            self.assertEqual(proc.returncode, 0)
        finally:
            if proc.poll() is None:
                proc.kill()

    def test_router_dealer_python_server(self):
        """Python ROUTER server <-> MoonBit DEALER client"""
        port = self.get_port()
        ctx = zmq.Context()
        router = ctx.socket(zmq.ROUTER)
        router.bind(f"tcp://127.0.0.1:{port}")

        proc = subprocess.Popen([self.bin_path, "dealer_client", str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            req = router.recv_multipart()
            identity = req[0]
            payload = req[2] if len(req) > 2 else req[1]
            self.assertEqual(payload, b"dealer_msg")
            router.send_multipart([identity, b"", b"router_reply_ok"])
            out, _ = proc.communicate(timeout=5)
            self.assertIn("DEALER_RECV:router_reply_ok", out)
            self.assertEqual(proc.returncode, 0)
        finally:
            router.close()
            ctx.term()
            if proc.poll() is None:
                proc.kill()

    # -------------------------------------------------------------
    # 5. PAIR
    # -------------------------------------------------------------
    def test_pair_moonbit_server(self):
        """MoonBit PAIR server <-> Python PAIR client"""
        port = self.get_port()
        proc = subprocess.Popen([self.bin_path, "pair_server", str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(0.3)
        try:
            ctx = zmq.Context()
            pair = ctx.socket(zmq.PAIR)
            pair.connect(f"tcp://127.0.0.1:{port}")
            pair.send(b"exclusive_ping")
            rep = pair.recv()
            self.assertEqual(rep, b"pair_pong:exclusive_ping")
            pair.close()
            ctx.term()
            proc.wait(timeout=5)
            self.assertEqual(proc.returncode, 0)
        finally:
            if proc.poll() is None:
                proc.kill()

    def test_pair_python_server(self):
        """Python PAIR server <-> MoonBit PAIR client"""
        port = self.get_port()
        ctx = zmq.Context()
        pair = ctx.socket(zmq.PAIR)
        pair.bind(f"tcp://127.0.0.1:{port}")

        proc = subprocess.Popen([self.bin_path, "pair_client", str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            req = pair.recv()
            self.assertEqual(req, b"pair_ping")
            pair.send(b"pair_ack:pair_ping")
            out, _ = proc.communicate(timeout=5)
            self.assertIn("PAIR_REPLY:pair_ack:pair_ping", out)
            self.assertEqual(proc.returncode, 0)
        finally:
            pair.close()
            ctx.term()
            if proc.poll() is None:
                proc.kill()

    # -------------------------------------------------------------
    # 6. MULTIPART MESSAGES
    # -------------------------------------------------------------
    def test_multipart_moonbit_server(self):
        """MoonBit REP server receiving multipart message from Python"""
        port = self.get_port()
        proc = subprocess.Popen([self.bin_path, "multipart_server", str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(0.3)
        try:
            ctx = zmq.Context()
            req = ctx.socket(zmq.REQ)
            req.connect(f"tcp://127.0.0.1:{port}")
            req.send_multipart([b"frame1", b"frame2", b"frame3"])
            rep = req.recv_multipart()
            # Server reverses order
            self.assertEqual(rep, [b"frame3", b"frame2", b"frame1"])
            req.close()
            ctx.term()
            proc.wait(timeout=5)
            self.assertEqual(proc.returncode, 0)
        finally:
            if proc.poll() is None:
                proc.kill()

    def test_multipart_python_server(self):
        """Python REP server receiving multipart message from MoonBit"""
        port = self.get_port()
        ctx = zmq.Context()
        rep = ctx.socket(zmq.REP)
        rep.bind(f"tcp://127.0.0.1:{port}")

        proc = subprocess.Popen([self.bin_path, "multipart_client", str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            req = rep.recv_multipart()
            self.assertEqual(req, [b"part1", b"part2", b"part3"])
            rep.send_multipart([b"part3", b"part2", b"part1"])
            out, _ = proc.communicate(timeout=5)
            self.assertIn("MP_REPLY_FRAMES:3", out)
            self.assertIn("MP_FRAME:part3", out)
            self.assertEqual(proc.returncode, 0)
        finally:
            rep.close()
            ctx.term()
            if proc.poll() is None:
                proc.kill()

    # -------------------------------------------------------------
    # 7. LARGE MESSAGE FRAMES (512 KB)
    # -------------------------------------------------------------
    def test_big_message_moonbit_server(self):
        """MoonBit REP server receiving 512KB payload from Python"""
        port = self.get_port()
        proc = subprocess.Popen([self.bin_path, "big_server", str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(0.3)
        try:
            ctx = zmq.Context()
            req = ctx.socket(zmq.REQ)
            req.connect(f"tcp://127.0.0.1:{port}")
            payload = b"Z" * 524288
            req.send(payload)
            reply = req.recv()
            self.assertEqual(reply, b"big_ack:524288")
            req.close()
            ctx.term()
            proc.wait(timeout=5)
            self.assertEqual(proc.returncode, 0)
        finally:
            if proc.poll() is None:
                proc.kill()

    def test_big_message_python_server(self):
        """Python REP server receiving 512KB payload from MoonBit"""
        port = self.get_port()
        ctx = zmq.Context()
        rep = ctx.socket(zmq.REP)
        rep.bind(f"tcp://127.0.0.1:{port}")

        proc = subprocess.Popen([self.bin_path, "big_client", str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            data = rep.recv()
            self.assertEqual(len(data), 524288)
            rep.send(f"big_ack:{len(data)}".encode())
            out, _ = proc.communicate(timeout=5)
            self.assertIn("BIG_REPLY:big_ack:524288", out)
            self.assertEqual(proc.returncode, 0)
        finally:
            rep.close()
            ctx.term()
            if proc.poll() is None:
                proc.kill()

    # -------------------------------------------------------------
    # 8. PLAIN SECURITY MECHANISM (RFC 24)
    # -------------------------------------------------------------
    def test_plain_auth_moonbit_server(self):
        """MoonBit PLAIN Server <-> Python PLAIN Client with valid credentials"""
        port = self.get_port()
        proc = subprocess.Popen([self.bin_path, "plain_server", str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(0.3)
        try:
            ctx = zmq.Context()
            req = ctx.socket(zmq.REQ)
            req.plain_username = b"admin"
            req.plain_password = b"secret123"
            req.connect(f"tcp://127.0.0.1:{port}")
            req.send(b"hello_auth")
            rep = req.recv()
            self.assertEqual(rep, b"plain_auth_ok")
            req.close()
            ctx.term()
            proc.wait(timeout=5)
            self.assertEqual(proc.returncode, 0)
        finally:
            if proc.poll() is None:
                proc.kill()

    def test_plain_auth_python_server(self):
        """Python PLAIN Server with ThreadAuthenticator <-> MoonBit PLAIN Client"""
        port = self.get_port()
        ctx = zmq.Context()
        auth = ThreadAuthenticator(ctx)
        auth.start()
        auth.configure_plain(domain="*", passwords={"admin": "secret123"})

        rep = ctx.socket(zmq.REP)
        rep.plain_server = True
        rep.bind(f"tcp://127.0.0.1:{port}")

        proc = subprocess.Popen([self.bin_path, "plain_client", str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            data = rep.recv()
            self.assertEqual(data, b"secure_hello")
            rep.send(b"welcome_to_py_plain")
            out, _ = proc.communicate(timeout=5)
            self.assertIn("PLAIN_REPLY:welcome_to_py_plain", out)
            self.assertEqual(proc.returncode, 0)
        finally:
            rep.close()
            auth.stop()
            ctx.term()
            if proc.poll() is None:
                proc.kill()

    # -------------------------------------------------------------
    # 9. RECONNECTION
    # -------------------------------------------------------------
    def test_reconnection_moonbit_client(self):
        """MoonBit REQ client auto-reconnecting after Python server restart"""
        port = self.get_port()
        ctx = zmq.Context()
        rep = ctx.socket(zmq.REP)
        rep.bind(f"tcp://127.0.0.1:{port}")

        proc = subprocess.Popen([self.bin_path, "reconnect_client", str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            # 1. First request
            req1 = rep.recv()
            self.assertEqual(req1, b"req_1")
            rep.send(b"rep_1_ok")

            # 2. Simulate server crash and restart
            rep.close()
            time.sleep(0.3)

            rep2 = ctx.socket(zmq.REP)
            rep2.bind(f"tcp://127.0.0.1:{port}")

            # 3. Second request through auto-reconnected socket
            req2 = rep2.recv()
            self.assertEqual(req2, b"req_2")
            rep2.send(b"rep_2_ok")

            out, _ = proc.communicate(timeout=5)
            self.assertIn("REPLY1:rep_1_ok", out)
            self.assertIn("REPLY2:rep_2_ok", out)
            self.assertEqual(proc.returncode, 0)
            rep2.close()
        finally:
            ctx.term()
            if proc.poll() is None:
                proc.kill()


if __name__ == "__main__":
    unittest.main()
