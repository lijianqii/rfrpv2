#!/usr/bin/env python3
"""真实进程级端到端测试：rfrps + rfrpc + 本地 TCP/UDP 服务 + Dashboard。

只依赖 Python 标准库。默认使用 target/release/rfrp，找不到时回退
target/debug/rfrp；也可以用 RFRP_BIN 指定二进制。

运行：
    python3 scripts/e2e_env.py -v
"""

from __future__ import annotations

import base64
import http.client
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
TOKEN = "e2e-token"
DASH_USER = "admin"
DASH_PASSWORD = "secret123"
BIN: Optional[Path] = None


def find_binary() -> Path:
    env = os.environ.get("RFRP_BIN")
    if env:
        path = Path(env)
        if not path.is_file():
            raise SystemExit(f"RFRP_BIN does not point to a file: {path}")
        return path
    for candidate in (
        ROOT / "target" / "release" / "rfrp",
        ROOT / "target" / "debug" / "rfrp",
        ROOT / "target" / "release" / "rfrp.exe",
        ROOT / "target" / "debug" / "rfrp.exe",
    ):
        if candidate.is_file():
            return candidate
    raise SystemExit(
        "rfrp binary not found; run `cargo build --all` or set RFRP_BIN=/path/to/rfrp"
    )


def free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def free_shared_port() -> int:
    """返回一个 TCP 与 UDP 都空闲的端口（RDP 双传输场景）。"""
    for _ in range(100):
        port = free_tcp_port()
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp:
                udp.bind(("127.0.0.1", port))
            return port
        except OSError:
            continue
    raise RuntimeError("could not allocate a TCP+UDP free port")


def basic_auth() -> str:
    return base64.b64encode(f"{DASH_USER}:{DASH_PASSWORD}".encode()).decode()


def http_get(
    port: int,
    path: str,
    auth: bool = True,
    user: str = DASH_USER,
    password: str = DASH_PASSWORD,
) -> tuple[int, str]:
    headers = {}
    if auth:
        credentials = base64.b64encode(f"{user}:{password}".encode()).decode()
        headers["Authorization"] = "Basic " + credentials
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", headers=headers
    )
    try:
        with urllib.request.urlopen(request, timeout=3) as response:
            return response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")


def api_status(port: int) -> dict:
    status, body = http_get(port, "/api/status")
    if status != 200:
        raise AssertionError(f"/api/status returned {status}: {body}")
    return json.loads(body)


def fetch_metrics(port: int) -> str:
    status, body = http_get(port, "/metrics")
    if status != 200:
        raise AssertionError(f"/metrics returned {status}: {body}")
    return body


def http_get_host(port: int, path: str, host: str) -> tuple[int, str]:
    """向 vhost 端口发一个带指定 Host 的 HTTP 请求。"""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
    try:
        conn.request("GET", path, headers={"Host": host})
        response = conn.getresponse()
        return response.status, response.read().decode("utf-8", "replace")
    finally:
        conn.close()


def metric_value(text: str, name: str, labels: Optional[dict[str, str]] = None) -> float:
    label_text = ""
    if labels:
        label_text = "{" + ",".join(f'{k}="{v}"' for k, v in sorted(labels.items())) + "}"
    prefix = f"{name}{label_text} "
    for line in text.splitlines():
        if line.startswith(prefix):
            try:
                return float(line[len(prefix):].strip())
            except ValueError:
                return 0.0
    return 0.0


def tcp_roundtrip(port: int, payload: bytes, timeout: float = 3.0) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
            sock.settimeout(timeout)
            sock.sendall(payload)
            received = b""
            while len(received) < len(payload):
                chunk = sock.recv(len(payload) - len(received))
                if not chunk:
                    break
                received += chunk
            return received == payload
    except OSError:
        return False


def tcp_bulk_roundtrip(port: int, payload: bytes, timeout: float = 60.0) -> bool:
    """全双工大流量往返。

    不能先 `sendall` 完再接收：payload 超过 socket 缓冲区后，本端发送阻塞、
    对端回写也阻塞，会形成测试辅助函数自身的死锁。这里用独立发送线程，
    主线程同时接收，和真实 RDP/SSH 双向流量一致。
    """
    send_errors: list[OSError] = []
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
            sock.settimeout(timeout)

            def send() -> None:
                try:
                    sock.sendall(payload)
                except OSError as exc:
                    send_errors.append(exc)

            sender = threading.Thread(target=send, daemon=True)
            sender.start()
            received = bytearray()
            while len(received) < len(payload):
                chunk = sock.recv(min(65536, len(payload) - len(received)))
                if not chunk:
                    break
                received.extend(chunk)
            sender.join(timeout=timeout)
            return not send_errors and bytes(received) == payload
    except OSError:
        return False


def tcp_connectable(port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=timeout):
            return True
    except OSError:
        return False


def udp_roundtrip(sock: socket.socket, payload: bytes, timeout: float = 2.0) -> bool:
    try:
        sock.settimeout(timeout)
        sock.send(payload)
        data, _ = sock.recvfrom(65535)
        return data == payload
    except OSError:
        return False


def udp_probe(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(0.5)
        try:
            sock.sendto(b"ready", ("127.0.0.1", port))
            data, _ = sock.recvfrom(64)
            return data == b"ready"
        except OSError:
            return False


def wait_until(predicate, timeout: float, interval: float = 0.5) -> bool:
    """轮询等待。

    interval 默认 0.5s：Dashboard 对每个来源 IP 限速 100 次/分钟，
    `/healthz` 也受该限速约束，高频轮询会把自己挡在 429 上。
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


def process_fd_count(pid: int) -> Optional[int]:
    """Linux 下统计进程打开的 fd 数；其他平台返回 None。"""
    try:
        return len(os.listdir(f"/proc/{pid}/fd"))
    except OSError:
        return None


def process_rss_kib(pid: int) -> Optional[int]:
    """Linux 下读取进程 VmRSS（KiB）；其他平台返回 None。"""
    try:
        with open(f"/proc/{pid}/status", encoding="utf-8") as status:
            for line in status:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except (OSError, ValueError):
        return None
    return None


class TcpEchoServer:
    def __init__(self) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(64)
        self.port = self.sock.getsockname()[1]
        self._stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    @staticmethod
    def _handle(conn: socket.socket) -> None:
        with conn:
            while True:
                try:
                    data = conn.recv(65536)
                except OSError:
                    return
                if not data:
                    try:
                        conn.shutdown(socket.SHUT_WR)
                    except OSError:
                        pass
                    return
                try:
                    conn.sendall(data)
                except OSError:
                    return

    def close(self) -> None:
        self._stop.set()
        try:
            self.sock.close()
        except OSError:
            pass


class UdpEchoServer:
    def __init__(self) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", 0))
        self.port = self.sock.getsockname()[1]
        self._stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                data, peer = self.sock.recvfrom(65535)
            except OSError:
                return
            try:
                self.sock.sendto(data, peer)
            except OSError:
                return

    def close(self) -> None:
        self._stop.set()
        try:
            self.sock.close()
        except OSError:
            pass


class HttpEchoServer:
    """本地 HTTP 后端：固定返回 `vhost-ok`，用于验证 vhost 域名路由。"""

    def __init__(self) -> None:
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                body = b"vhost-ok"
                try:
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(body)))
                    self.send_header("Connection", "close")
                    self.end_headers()
                    self.wfile.write(body)
                except OSError:
                    pass

            def log_message(self, *args) -> None:
                pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def close(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


class Stack:
    """一套真实 rfrps + rfrpc 进程。"""

    def __init__(
        self,
        case: "E2E",
        *,
        tcp_local: Optional[int] = None,
        udp_local: Optional[int] = None,
        vhost_local: Optional[int] = None,
        vhost_domain: str = "web.example",
        same_port: bool = False,
        tls: bool = False,
        udp_timeout: int = 300,
        log_level: str = "debug",
        status: bool = False,
    ) -> None:
        self.case = case
        self.temp = case.temp
        self.tls = tls
        self.log_level = log_level
        self.tcp_local = tcp_local
        self.udp_local = udp_local
        self.vhost_local = vhost_local
        self.vhost_domain = vhost_domain
        self.remote_tcp: Optional[int] = None
        self.remote_udp: Optional[int] = None
        self.vhost_port: Optional[int] = None
        self.status_port: Optional[int] = None

        used: set[int] = set()
        self.control_port = self._port(used, free_tcp_port)
        self.dashboard_port = self._port(used, free_tcp_port)
        if tcp_local is not None:
            allocator = free_shared_port if same_port and udp_local is not None else free_tcp_port
            self.remote_tcp = self._port(used, allocator)
        if udp_local is not None:
            if same_port and self.remote_tcp is not None:
                self.remote_udp = self.remote_tcp
            else:
                self.remote_udp = self._port(used, free_udp_port)
        if vhost_local is not None:
            self.vhost_port = self._port(used, free_tcp_port)
        if status:
            self.status_port = self._port(used, free_tcp_port)

        self.server_config = self.temp / "server.toml"
        self.client_config = self.temp / "client.toml"
        self._write_server_config(udp_timeout)
        self._write_client_config()

        self.server_proc: Optional[subprocess.Popen] = None
        self.client_proc: Optional[subprocess.Popen] = None
        self._log_handles: list[object] = []
        try:
            self.server_proc = self._spawn("server", self.server_config)
            if not wait_until(lambda: self._health_ok(), 10.0):
                raise AssertionError("rfrps dashboard did not become healthy")
            self.client_proc = self._spawn("client", self.client_config)
            if not self._wait_proxies_ready(15.0):
                raise AssertionError("proxies did not become ready")
        except Exception:
            self.stop()
            raise

    @staticmethod
    def _port(used: set[int], allocator) -> int:
        for _ in range(100):
            port = allocator()
            if port not in used:
                used.add(port)
                return port
        raise RuntimeError("could not allocate a unique port")

    def _write_server_config(self, udp_timeout: int) -> None:
        lines = [
            "[server]",
            'bind_addr = "127.0.0.1"',
            f"bind_port = {self.control_port}",
            f'token = "{TOKEN}"',
            f"tls_enable = {str(self.tls).lower()}",
            f"work_conn_tls = {str(self.tls).lower()}",
            "heartbeat_interval_secs = 5",
            "heartbeat_timeout_secs = 3",
            f"udp_session_timeout_secs = {udp_timeout}",
            "grace_secs = 1",
        ]
        if self.tls:
            lines += [
                f'tls_cert = "{(ROOT / "examples" / "cert.pem").as_posix()}"',
                f'tls_key = "{(ROOT / "examples" / "key.pem").as_posix()}"',
            ]
        lines += [
            "",
            "[dashboard]",
            f'addr = "127.0.0.1:{self.dashboard_port}"',
            f'user = "{DASH_USER}"',
            f'password = "{DASH_PASSWORD}"',
            "",
            "[proxy]",
            'allow_ports = ""',
        ]
        if self.vhost_port is not None:
            lines.append(f"vhost_http_port = {self.vhost_port}")
        lines += [
            "",
            "[log]",
            f'level = "{self.log_level}"',
            f'output = "file:{(self.temp / "rfrps.log").as_posix()}"',
        ]
        self.server_config.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _write_client_config(self) -> None:
        lines = [
            "[client]",
            'server_addr = "127.0.0.1"',
            f"server_port = {self.control_port}",
            f'token = "{TOKEN}"',
            f"tls_enable = {str(self.tls).lower()}",
            f"work_conn_tls = {str(self.tls).lower()}",
            "heartbeat_interval_secs = 5",
            "heartbeat_timeout_secs = 3",
            f'run_id_file = "{(self.temp / "run_id").as_posix()}"',
        ]
        if self.tls:
            lines += [
                'tls_server_name = "localhost"',
                f'tls_ca = "{(ROOT / "examples" / "ca.pem").as_posix()}"',
            ]
        if self.status_port is not None:
            lines.append(f'status_addr = "127.0.0.1:{self.status_port}"')
        if self.tcp_local is not None:
            lines += [
                "",
                "[[proxy]]",
                'name = "tcp"',
                'type = "tcp"',
                'local_ip = "127.0.0.1"',
                f"local_port = {self.tcp_local}",
                f"remote_port = {self.remote_tcp}",
                "pool_size = 1",
            ]
        if self.udp_local is not None:
            lines += [
                "",
                "[[proxy]]",
                'name = "udp"',
                'type = "udp"',
                'local_ip = "127.0.0.1"',
                f"local_port = {self.udp_local}",
                f"remote_port = {self.remote_udp}",
                "pool_size = 0",
            ]
        if self.vhost_local is not None:
            lines += [
                "",
                "[[proxy]]",
                'name = "web"',
                'type = "http"',
                'local_ip = "127.0.0.1"',
                f"local_port = {self.vhost_local}",
                f'custom_domains = ["{self.vhost_domain}"]',
            ]
        lines += [
            "",
            "[log]",
            f'level = "{self.log_level}"',
            f'output = "file:{(self.temp / "rfrpc.log").as_posix()}"',
        ]
        self.client_config.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _spawn(self, role: str, config: Path):
        log_path = self.temp / f"{role}.log"
        log = open(log_path, "ab", buffering=0)
        self._log_handles.append(log)
        proc = subprocess.Popen(
            [str(BIN), role, "-c", str(config)],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        self.case.processes.append((proc, log))
        return proc

    def _health_ok(self) -> bool:
        try:
            status, _ = http_get(self.dashboard_port, "/healthz", auth=False)
            return status == 200
        except OSError:
            return False

    def _wait_proxies_ready(self, timeout: float) -> bool:
        if self.tcp_local is not None:
            if not wait_until(
                lambda: tcp_roundtrip(self.remote_tcp, b"ready", timeout=0.5), timeout
            ):
                return False
        if self.udp_local is not None:
            if not wait_until(lambda: udp_probe(self.remote_udp), timeout):
                return False
        if self.vhost_local is not None:
            if not wait_until(lambda: self._vhost_ready(), timeout):
                return False
        return True

    def _vhost_ready(self) -> bool:
        status, body = http_get_host(self.vhost_port, "/", self.vhost_domain)
        return status == 200 and body == "vhost-ok"

    def status(self) -> dict:
        return api_status(self.dashboard_port)

    def wait_status(self, predicate, timeout: float = 10.0) -> dict:
        deadline = time.monotonic() + timeout
        last: Optional[dict] = None
        while time.monotonic() < deadline:
            try:
                last = self.status()
                if predicate(last):
                    return last
            except Exception:
                pass
            time.sleep(0.5)
        raise AssertionError(
            f"dashboard condition not met; last status: {last}\n{self.log_tail()}"
        )

    def restart_server(self) -> None:
        self._terminate(self.server_proc)
        self.server_proc = self._spawn("server", self.server_config)
        if not wait_until(self._health_ok, 10.0):
            raise AssertionError("rfrps did not restart")
        if not self._wait_proxies_ready(20.0):
            raise AssertionError("proxies did not recover after server restart")

    def restart_client(self) -> None:
        self._terminate(self.client_proc)
        self.client_proc = self._spawn("client", self.client_config)
        if not self._wait_proxies_ready(20.0):
            raise AssertionError("proxies did not recover after client restart")

    def pause_client(self) -> None:
        if self.client_proc is None:
            raise AssertionError("client is not running")
        os.kill(self.client_proc.pid, signal.SIGSTOP)

    def resume_client(self) -> None:
        if self.client_proc is None:
            raise AssertionError("client is not running")
        os.kill(self.client_proc.pid, signal.SIGCONT)

    @staticmethod
    def _terminate(proc: Optional[subprocess.Popen]) -> None:
        if proc is None or proc.poll() is not None:
            return
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass

    def stop(self) -> None:
        self._terminate(self.client_proc)
        self._terminate(self.server_proc)
        for handle in self._log_handles:
            try:
                handle.close()
            except OSError:
                pass
        self._log_handles.clear()

    def log_tail(self, lines: int = 60) -> str:
        return self.case.logs_tail(lines)


class E2E(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory(prefix="rfrp-e2e-")
        self.temp = Path(self.tempdir.name)
        self.processes: list[tuple[subprocess.Popen, object]] = []
        self.stacks: list[Stack] = []
        self.echoes: list[object] = []

    def _test_failed(self) -> bool:
        outcome = getattr(self, "_outcome", None)
        result = getattr(outcome, "result", None)
        if result is None:
            return False
        for test, _ in list(result.failures) + list(result.errors):
            if test is self:
                return True
        return False

    def tearDown(self) -> None:
        failed = self._test_failed()
        for stack in reversed(self.stacks):
            stack.stop()
        for echo in self.echoes:
            echo.close()
        # 兜底：Stack 构造中途失败时，进程可能已启动但未加入 stacks。
        for proc, handle in reversed(self.processes):
            if proc.poll() is None:
                proc.kill()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
            try:
                handle.close()
            except OSError:
                pass
        if failed:
            tail = self.logs_tail()
            if tail:
                print(tail, file=sys.stderr)
            for name in ("server.toml", "client.toml"):
                path = self.temp / name
                if path.is_file():
                    print(
                        f"--- {name} ---\n{path.read_text(encoding='utf-8')}",
                        file=sys.stderr,
                    )
        if failed and os.environ.get("E2E_KEEP_TEMP") == "1":
            self.tempdir._finalizer.detach()
            print(f"E2E temp dir preserved: {self.temp}", file=sys.stderr)
        else:
            self.tempdir.cleanup()

    def logs_tail(self, lines: int = 60) -> str:
        parts = []
        for name in ("rfrps.log", "rfrpc.log"):
            path = self.temp / name
            if path.is_file():
                text = path.read_text(encoding="utf-8", errors="replace").splitlines()
                parts.append(f"--- {name} ---\n" + "\n".join(text[-lines:]))
        return "\n".join(parts)

    def tcp_echo(self) -> TcpEchoServer:
        echo = TcpEchoServer()
        self.echoes.append(echo)
        return echo

    def udp_echo(self) -> UdpEchoServer:
        echo = UdpEchoServer()
        self.echoes.append(echo)
        return echo

    def start_extra_client(
        self,
        *,
        token: str,
        local_port: int,
        remote_port: int,
        name: str,
        server_port: Optional[int] = None,
        tls: bool = False,
    ) -> subprocess.Popen:
        binary = BIN
        if binary is None:
            raise AssertionError("rfrp binary is not initialized")
        config = self.temp / f"client-{name}.toml"
        log_path = self.temp / f"client-{name}.log"
        config.write_text(
            "\n".join(
                [
                    "[client]",
                    'server_addr = "127.0.0.1"',
                    f"server_port = {server_port or self._stack_control_port}",
                    f'token = "{token}"',
                    "work_conn_tls = false",
                    "heartbeat_interval_secs = 5",
                    "heartbeat_timeout_secs = 3",
                    f'run_id_file = "{(self.temp / f"run-id-{name}").as_posix()}"',
                    *((
                        [
                            "tls_enable = true",
                            'tls_server_name = "localhost"',
                            f'tls_ca = "{(ROOT / "examples" / "ca.pem").as_posix()}"',
                        ]
                    ) if tls else []),
                    "",
                    "[[proxy]]",
                    'name = "extra"',
                    'type = "tcp"',
                    'local_ip = "127.0.0.1"',
                    f"local_port = {local_port}",
                    f"remote_port = {remote_port}",
                    "pool_size = 0",
                    "",
                    "[log]",
                    'level = "warn"',
                    f'output = "file:{(self.temp / f"client-{name}.rfrpc.log").as_posix()}"',
                    "",
                ]
            ),
            encoding="utf-8",
        )
        log = open(log_path, "ab", buffering=0)
        proc = subprocess.Popen(
            [str(binary), "client", "-c", str(config)],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        self.processes.append((proc, log))
        return proc

    def http_echo(self) -> HttpEchoServer:
        echo = HttpEchoServer()
        self.echoes.append(echo)
        return echo

    def make_stack(self, **kwargs) -> Stack:
        try:
            stack = Stack(self, **kwargs)
        except Exception as exc:
            raise AssertionError(f"stack setup failed: {exc}\n{self.logs_tail()}") from exc
        self.stacks.append(stack)
        self._stack_control_port = stack.control_port
        return stack

    def test_config_hot_reload(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(tcp_local=echo.port)
        original_client = stack.client_config.read_text(encoding="utf-8")
        original_server = stack.server_config.read_text(encoding="utf-8")
        second_echo = self.tcp_echo()
        second_remote = free_tcp_port()
        while second_remote in {
            stack.remote_tcp,
            stack.control_port,
            stack.dashboard_port,
        }:
            second_remote = free_tcp_port()

        # 1) 新增代理：客户端收到文件事件后重连并注册新代理。
        stack.client_config.write_text(
            original_client
            + "\n[[proxy]]\n"
            + 'name = "tcp2"\n'
            + 'type = "tcp"\n'
            + 'local_ip = "127.0.0.1"\n'
            + f"local_port = {second_echo.port}\n"
            + f"remote_port = {second_remote}\n"
            + "pool_size = 0\n",
            encoding="utf-8",
        )
        self.assertTrue(
            wait_until(
                lambda: tcp_roundtrip(second_remote, b"hot-added", timeout=0.5),
                20.0,
            ),
            "new proxy from hot-reloaded config did not become ready",
        )
        self.assertTrue(
            tcp_roundtrip(stack.remote_tcp, b"original-still-works"),
            "existing proxy broke after client config reload",
        )

        # 2) 非法配置：保留旧配置且进程继续服务。
        stack.client_config.write_text("invalid = [\n", encoding="utf-8")
        time.sleep(2)
        self.assertIsNone(stack.client_proc.poll())
        self.assertTrue(
            tcp_roundtrip(stack.remote_tcp, b"after-invalid-config"),
            "client did not keep serving after invalid config",
        )

        # 3) 删除代理：恢复原配置后新代理端口应释放。
        stack.client_config.write_text(original_client, encoding="utf-8")
        self.assertTrue(
            wait_until(
                lambda: not tcp_roundtrip(second_remote, b"gone", timeout=0.5),
                20.0,
            ),
            "removed proxy port was not released",
        )
        self.assertTrue(
            tcp_roundtrip(stack.remote_tcp, b"after-proxy-removal"),
            "original proxy broke after proxy removal",
        )

        # 4) Dashboard 凭据热更新：旧密码失效，新密码生效。
        new_password = "new-secret-456"
        stack.server_config.write_text(
            original_server.replace(
                'password = "secret123"', f'password = "{new_password}"'
            ),
            encoding="utf-8",
        )
        self.assertTrue(
            wait_until(
                lambda: http_get(
                    stack.dashboard_port,
                    "/api/status",
                    password=new_password,
                )[0]
                == 200,
                15.0,
            ),
            "new dashboard password did not take effect",
        )
        old_status, _ = http_get(
            stack.dashboard_port,
            "/api/status",
            password=DASH_PASSWORD,
        )
        self.assertEqual(old_status, 401, "old dashboard password must be rejected")
        self.assertTrue(
            tcp_roundtrip(stack.remote_tcp, b"after-dashboard-reload"),
            "proxy broke after server config reload",
        )

        # 收尾：恢复原始配置，确保后续事件不会影响其他测试。
        stack.server_config.write_text(original_server, encoding="utf-8")
        stack.client_config.write_text(original_client, encoding="utf-8")

    def test_server_vhost_and_dashboard_listener_hot_reload(self) -> None:
        backend = self.http_echo()
        stack = self.make_stack(
            vhost_local=backend.port,
            vhost_domain="web.example",
            log_level="info",
        )
        original = stack.server_config.read_text(encoding="utf-8")
        old_vhost = stack.vhost_port
        new_vhost = free_tcp_port()
        while new_vhost in {
            stack.control_port,
            stack.dashboard_port,
            old_vhost,
        }:
            new_vhost = free_tcp_port()
        stack.server_config.write_text(
            original.replace(
                f"vhost_http_port = {old_vhost}",
                f"vhost_http_port = {new_vhost}",
            ),
            encoding="utf-8",
        )
        self.assertTrue(
            wait_until(
                lambda: http_get_host(new_vhost, "/", "web.example")[0] == 200,
                10.0,
            ),
            "new vhost listener did not become ready",
        )
        self.assertTrue(
            wait_until(lambda: not tcp_connectable(old_vhost), 10.0),
            "old vhost listener was not released",
        )

        new_dashboard = free_tcp_port()
        while new_dashboard in {
            stack.control_port,
            stack.dashboard_port,
            old_vhost,
            new_vhost,
        }:
            new_dashboard = free_tcp_port()
        step = stack.server_config.read_text(encoding="utf-8")
        stack.server_config.write_text(
            step.replace(
                f'addr = "127.0.0.1:{stack.dashboard_port}"',
                f'addr = "127.0.0.1:{new_dashboard}"',
            ),
            encoding="utf-8",
        )
        self.assertTrue(
            wait_until(
                lambda: http_get(new_dashboard, "/api/status")[0] == 200,
                10.0,
            ),
            "new dashboard listener did not become ready",
        )
        self.assertTrue(
            wait_until(lambda: not tcp_connectable(stack.dashboard_port), 10.0),
            "old dashboard listener was not released",
        )

        stack.server_config.write_text(original, encoding="utf-8")
        self.assertTrue(
            wait_until(
                lambda: http_get(stack.dashboard_port, "/api/status")[0] == 200,
                10.0,
            ),
            "original dashboard listener was not restored",
        )
        self.assertTrue(
            wait_until(
                lambda: http_get_host(old_vhost, "/", "web.example")[0] == 200,
                10.0,
            ),
            "original vhost listener was not restored",
        )

    def test_client_status_addr_hot_reload(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(tcp_local=echo.port, status=True, log_level="info")
        old_status = stack.status_port
        self.assertIsNotNone(old_status)
        self.assertEqual(
            http_get(old_status, "/api/status", auth=False)[0],
            200,
            "initial client status endpoint is not available",
        )
        original = stack.client_config.read_text(encoding="utf-8")
        new_status = free_tcp_port()
        while new_status in {
            old_status,
            stack.control_port,
            stack.dashboard_port,
            stack.remote_tcp,
        }:
            new_status = free_tcp_port()
        stack.client_config.write_text(
            original.replace(
                f'status_addr = "127.0.0.1:{old_status}"',
                f'status_addr = "127.0.0.1:{new_status}"',
            ),
            encoding="utf-8",
        )
        self.assertTrue(
            wait_until(
                lambda: http_get(new_status, "/api/status", auth=False)[0] == 200,
                10.0,
            ),
            "new client status endpoint did not become ready",
        )
        self.assertTrue(
            wait_until(lambda: not tcp_connectable(old_status), 10.0),
            "old client status endpoint was not released",
        )
        self.assertTrue(
            tcp_roundtrip(stack.remote_tcp, b"status-reload-proxy"),
            "proxy broke while moving client status endpoint",
        )

        stack.client_config.write_text(original, encoding="utf-8")
        self.assertTrue(
            wait_until(
                lambda: http_get(old_status, "/api/status", auth=False)[0] == 200,
                10.0,
            ),
            "original client status endpoint was not restored",
        )

    def test_udp_session_timeout_hot_reload(self) -> None:
        udp_echo = self.udp_echo()
        stack = self.make_stack(udp_local=udp_echo.port, udp_timeout=300)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addCleanup(sock.close)
        sock.connect(("127.0.0.1", stack.remote_udp))
        self.assertTrue(udp_roundtrip(sock, b"timeout-hot"), "initial UDP roundtrip failed")
        stack.wait_status(lambda s: s["udp_sessions"] >= 1)

        original = stack.server_config.read_text(encoding="utf-8")
        stack.server_config.write_text(
            original.replace(
                "udp_session_timeout_secs = 300",
                "udp_session_timeout_secs = 1",
            ),
            encoding="utf-8",
        )
        self.assertTrue(
            wait_until(lambda: stack.status()["udp_sessions"] == 0, 12.0),
            "existing UDP session did not expire after timeout hot reload",
        )
        stack.server_config.write_text(original, encoding="utf-8")

    def test_server_listener_and_tls_hot_reload(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(tcp_local=echo.port, log_level="info")
        original = stack.server_config.read_text(encoding="utf-8")

        new_control = free_tcp_port()
        while new_control in {
            stack.control_port,
            stack.dashboard_port,
            stack.remote_tcp,
        }:
            new_control = free_tcp_port()
        stack.server_config.write_text(
            original.replace(
                f"bind_port = {stack.control_port}",
                f"bind_port = {new_control}",
            ),
            encoding="utf-8",
        )
        self.assertTrue(
            wait_until(lambda: tcp_connectable(new_control), 10.0),
            "new control listener did not become ready",
        )
        self.assertTrue(
            tcp_roundtrip(stack.remote_tcp, b"existing-control-session"),
            "existing control session broke after listener replacement",
        )

        echo2 = self.tcp_echo()
        remote2 = free_tcp_port()
        while remote2 in {
            stack.remote_tcp,
            stack.control_port,
            new_control,
            stack.dashboard_port,
        }:
            remote2 = free_tcp_port()
        extra = self.start_extra_client(
            token="e2e-token",
            local_port=echo2.port,
            remote_port=remote2,
            name="new-control",
            server_port=new_control,
        )
        self.assertTrue(
            wait_until(
                lambda: tcp_roundtrip(remote2, b"new-control-login", timeout=0.5),
                20.0,
            ),
            "client could not connect to the replaced control listener",
        )
        extra.terminate()
        extra.wait(timeout=5)

        # 恢复原控制端口，确认新监听可以被替换回来。
        stack.server_config.write_text(original, encoding="utf-8")
        self.assertTrue(
            wait_until(lambda: tcp_connectable(stack.control_port), 10.0),
            "original control listener was not restored",
        )
        self.assertTrue(
            tcp_roundtrip(stack.remote_tcp, b"after-control-restore"),
            "existing proxy broke after control listener restore",
        )

        # 开启控制链路 TLS：新 TLS 客户端应能连接，旧明文会话保持。
        tls_config = original.replace("tls_enable = false", "tls_enable = true", 1)
        tls_config = tls_config.replace(
            "tls_enable = true\n",
            "tls_enable = true\n"
            f'tls_cert = "{(ROOT / "examples" / "cert.pem").as_posix()}"\n'
            f'tls_key = "{(ROOT / "examples" / "key.pem").as_posix()}"\n',
            1,
        )
        stack.server_config.write_text(tls_config, encoding="utf-8")
        echo3 = self.tcp_echo()
        remote3 = free_tcp_port()
        while remote3 in {
            stack.remote_tcp,
            stack.control_port,
            stack.dashboard_port,
            remote2,
        }:
            remote3 = free_tcp_port()
        tls_client = self.start_extra_client(
            token="e2e-token",
            local_port=echo3.port,
            remote_port=remote3,
            name="tls-control",
            tls=True,
        )
        self.assertTrue(
            wait_until(
                lambda: tcp_roundtrip(remote3, b"tls-control-login", timeout=0.5),
                20.0,
            ),
            "TLS client could not connect after control TLS hot reload",
        )
        self.assertTrue(
            tcp_roundtrip(stack.remote_tcp, b"plain-session-still-live"),
            "existing plaintext session broke after enabling control TLS",
        )
        tls_client.terminate()
        tls_client.wait(timeout=5)
        stack.server_config.write_text(original, encoding="utf-8")

    def test_log_level_hot_reload(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(tcp_local=echo.port, log_level="info")
        original = stack.server_config.read_text(encoding="utf-8")
        log_path = self.temp / "rfrps.log"
        self.assertNotIn(
            "DEBUG",
            log_path.read_text(encoding="utf-8", errors="replace")
            if log_path.is_file()
            else "",
            "info-level server log must not contain DEBUG before reload",
        )
        stack.server_config.write_text(
            original.replace('level = "info"', 'level = "debug"'),
            encoding="utf-8",
        )
        time.sleep(1.0)
        self.assertTrue(tcp_roundtrip(stack.remote_tcp, b"generate-debug-log"))
        self.assertTrue(
            wait_until(
                lambda: "DEBUG"
                in log_path.read_text(encoding="utf-8", errors="replace"),
                15.0,
            ),
            "log.level hot reload to debug did not take effect",
        )
        stack.server_config.write_text(original, encoding="utf-8")

    def test_server_token_change_only_affects_new_login(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(tcp_local=echo.port, log_level="info")
        original = stack.server_config.read_text(encoding="utf-8")
        rotated = "rotated-token-123"
        stack.server_config.write_text(
            original.replace('token = "e2e-token"', f'token = "{rotated}"'),
            encoding="utf-8",
        )
        time.sleep(1.0)
        # 已建立的控制会话继续服务，不因 token 变更被踢掉。
        self.assertTrue(
            tcp_roundtrip(stack.remote_tcp, b"existing-session-token"),
            "existing session was affected by token reload",
        )
        self.assertIsNone(stack.client_proc.poll())

        echo2 = self.tcp_echo()
        remote2 = free_tcp_port()
        while remote2 in {
            stack.remote_tcp,
            stack.control_port,
            stack.dashboard_port,
        }:
            remote2 = free_tcp_port()
        new_client = self.start_extra_client(
            token=rotated,
            local_port=echo2.port,
            remote_port=remote2,
            name="rotated-token",
        )
        if not wait_until(
            lambda: tcp_roundtrip(remote2, b"new-token-login", timeout=0.5),
            20.0,
        ):
            for name in (
                "client-rotated-token.log",
                "client-rotated-token.rfrpc.log",
            ):
                path = self.temp / name
                if path.is_file():
                    print(
                        f"--- {name} ---\n"
                        f"{path.read_text(encoding='utf-8', errors='replace')[-4000:]}",
                        file=sys.stderr,
                    )
            self.fail("client with rotated token could not log in")

        echo3 = self.tcp_echo()
        remote3 = free_tcp_port()
        while remote3 in {
            stack.remote_tcp,
            stack.control_port,
            stack.dashboard_port,
            remote2,
        }:
            remote3 = free_tcp_port()
        old_client = self.start_extra_client(
            token="e2e-token",
            local_port=echo3.port,
            remote_port=remote3,
            name="old-token",
        )
        self.assertIsNotNone(
            old_client.wait(timeout=8),
            "client with old token should exit after auth failure",
        )
        self.assertFalse(
            tcp_roundtrip(remote3, b"old-token-rejected", timeout=0.5),
            "old token must not register a proxy",
        )
        self.assertTrue(
            tcp_roundtrip(stack.remote_tcp, b"existing-after-token"),
            "existing session stopped after token reload",
        )

        for proc in (new_client, old_client):
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
        stack.server_config.write_text(original, encoding="utf-8")

    def test_tcp_proxy_concurrent_and_large_transfer(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(tcp_local=echo.port)
        payload = b"x" * 65536

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: tcp_roundtrip(stack.remote_tcp, payload), range(8)))
        self.assertTrue(all(results), "concurrent TCP roundtrips failed")

        big = b"y" * (1 << 20)
        self.assertTrue(tcp_roundtrip(stack.remote_tcp, big), "1 MiB TCP transfer failed")

        status = stack.wait_status(
            lambda s: any(
                p.get("name") == "tcp"
                and p.get("connections_total", 0) >= 8
                and p.get("bytes_up", 0) >= 8 * len(payload)
                for p in s.get("proxy_stats", [])
            )
        )
        tcp_stats = next(p for p in status["proxy_stats"] if p["name"] == "tcp")
        self.assertGreaterEqual(tcp_stats["bytes_down"], 8 * len(payload))
        self.assertGreaterEqual(status["metrics"]["total_connections"], 8)

    def test_http_vhost_routing(self) -> None:
        backend = self.http_echo()
        stack = self.make_stack(vhost_local=backend.port, vhost_domain="web.example")
        self.assertIsNotNone(stack.vhost_port)

        status, body = http_get_host(stack.vhost_port, "/", "web.example")
        self.assertEqual(status, 200)
        self.assertEqual(body, "vhost-ok")

        status, _ = http_get_host(stack.vhost_port, "/", "unknown.example")
        self.assertEqual(status, 404, "unmatched Host must return 404")

        status_json = stack.wait_status(lambda s: s["proxies"] >= 1)
        web = next(p for p in status_json["proxy_stats"] if p["name"] == "web")
        self.assertGreaterEqual(web["connections_total"], 2)

    def test_udp_multiple_clients_and_large_packet(self) -> None:
        udp_echo = self.udp_echo()
        stack = self.make_stack(udp_local=udp_echo.port)
        socks = []
        for _ in range(4):
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.connect(("127.0.0.1", stack.remote_udp))
            socks.append(sock)
            self.addCleanup(sock.close)

        payload = b"u" * 60000
        with ThreadPoolExecutor(max_workers=len(socks)) as pool:
            results = list(
                pool.map(lambda s: udp_roundtrip(s, payload, timeout=5.0), socks)
            )
        self.assertTrue(all(results), "concurrent UDP large-packet roundtrips failed")

        status = stack.wait_status(
            lambda s: s["udp_sessions"] >= 4
            and any(
                p.get("name") == "udp" and p.get("connections_total", 0) >= 4
                for p in s.get("proxy_stats", [])
            )
        )
        self.assertGreaterEqual(status["metrics"]["udp_sessions_total"], 4)

    def test_rdp_tcp_udp_same_port_and_dashboard_stats(self) -> None:
        tcp_echo = self.tcp_echo()
        udp_echo = self.udp_echo()
        stack = self.make_stack(
            tcp_local=tcp_echo.port,
            udp_local=udp_echo.port,
            same_port=True,
        )
        self.assertEqual(stack.remote_tcp, stack.remote_udp)
        self.assertTrue(tcp_roundtrip(stack.remote_tcp, b"rdp-tcp"))

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addCleanup(sock.close)
        sock.connect(("127.0.0.1", stack.remote_udp))
        payload = b"udp-stats"
        for _ in range(5):
            self.assertTrue(udp_roundtrip(sock, payload), "UDP roundtrip failed")

        status = stack.wait_status(
            lambda s: s["udp_sessions"] >= 1
            and any(
                p.get("name") == "udp"
                and p.get("bytes_up", 0) >= 5 * len(payload)
                and p.get("bytes_down", 0) >= 5 * len(payload)
                for p in s.get("proxy_stats", [])
            )
        )
        udp_stats = next(p for p in status["proxy_stats"] if p["name"] == "udp")
        self.assertGreaterEqual(udp_stats["active_connections"], 1)
        self.assertGreaterEqual(udp_stats["connections_total"], 1)
        self.assertGreaterEqual(status["active_sessions"], status["udp_sessions"])
        self.assertGreaterEqual(
            status["total_sessions"], status["metrics"]["udp_sessions_total"]
        )

        status_code, _ = http_get(stack.dashboard_port, "/api/status", auth=False)
        self.assertEqual(status_code, 401, "dashboard must require auth")
        metrics = fetch_metrics(stack.dashboard_port)
        self.assertGreater(
            metric_value(metrics, "rfrp_proxy_bytes_up_total", {"proxy": "udp"}),
            0,
            "per-proxy UDP bytes missing from /metrics",
        )
        self.assertGreaterEqual(metric_value(metrics, "rfrp_active_sessions"), 1)
        self.assertGreaterEqual(metric_value(metrics, "rfrp_udp_sessions_total"), 1)
        self.assertGreaterEqual(metric_value(metrics, "rfrp_data_sessions_total"), 1)

    def test_udp_session_timeout_and_recreate(self) -> None:
        udp_echo = self.udp_echo()
        stack = self.make_stack(udp_local=udp_echo.port, udp_timeout=1)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addCleanup(sock.close)
        sock.connect(("127.0.0.1", stack.remote_udp))
        payload = b"timeout"
        self.assertTrue(udp_roundtrip(sock, payload), "initial UDP roundtrip failed")

        first = stack.wait_status(lambda s: s["udp_sessions"] >= 1)
        before = first["metrics"]["udp_sessions_total"]
        self.assertTrue(
            wait_until(lambda: stack.status()["udp_sessions"] == 0, 8.0),
            "idle UDP session was not swept",
        )
        self.assertTrue(udp_roundtrip(sock, payload, timeout=4.0), "UDP session recreation failed")
        after = stack.wait_status(
            lambda s: s["metrics"]["udp_sessions_total"] > before, timeout=8.0
        )
        self.assertGreater(after["metrics"]["udp_sessions_total"], before)

    def test_tls_control_and_work_conn(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(tcp_local=echo.port, tls=True)
        self.assertTrue(tcp_roundtrip(stack.remote_tcp, b"tls-data"))
        status = stack.wait_status(lambda s: s["metrics"]["accepting"] is True)
        self.assertGreaterEqual(status["metrics"]["total_connections"], 1)

    def test_server_restart_reconnects(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(tcp_local=echo.port)
        self.assertTrue(tcp_roundtrip(stack.remote_tcp, b"before-restart"))
        stack.restart_server()
        self.assertTrue(
            wait_until(
                lambda: tcp_roundtrip(stack.remote_tcp, b"after-restart", timeout=1.0),
                15.0,
            ),
            "TCP proxy did not recover after rfrps restart",
        )

    def test_udp_burst_remains_usable(self) -> None:
        udp_echo = self.udp_echo()
        stack = self.make_stack(udp_local=udp_echo.port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addCleanup(sock.close)
        sock.connect(("127.0.0.1", stack.remote_udp))
        self.assertTrue(udp_roundtrip(sock, b"warm"), "warm-up UDP failed")

        received: list[bytes] = []
        stop = threading.Event()

        def receive() -> None:
            sock.settimeout(0.5)
            while not stop.is_set():
                try:
                    received.append(sock.recv(65535))
                except socket.timeout:
                    continue
                except OSError:
                    return

        receiver = threading.Thread(target=receive, daemon=True)
        receiver.start()
        burst = b"z" * 512
        for _ in range(200):
            sock.send(burst)
        time.sleep(1.5)
        stop.set()
        receiver.join(timeout=2)
        self.assertTrue(received, "UDP burst produced no echoes")
        self.assertTrue(udp_roundtrip(sock, b"after-burst"), "UDP proxy unusable after burst")

        status = stack.wait_status(lambda s: s["udp_sessions"] >= 1)
        self.assertGreaterEqual(status["metrics"]["udp_sessions_total"], 1)

    def test_tcp_throughput_and_short_connection_latency(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(tcp_local=echo.port)

        payload = b"p" * (16 << 20)
        started = time.perf_counter()
        self.assertTrue(
            tcp_bulk_roundtrip(stack.remote_tcp, payload, timeout=60.0),
            "16 MiB TCP transfer failed",
        )
        elapsed = time.perf_counter() - started
        mibps = (len(payload) / (1 << 20)) / max(elapsed, 1e-9)
        print(
            f"\n[perf] TCP throughput: {mibps:.2f} MiB/s "
            f"({len(payload) / (1 << 20):.0f} MiB in {elapsed:.3f}s)"
        )
        self.assertGreater(mibps, 0.2, "TCP throughput below liveness floor (0.2 MiB/s)")

        payload = b"l" * 32
        latencies = []
        for _ in range(100):
            started = time.perf_counter()
            self.assertTrue(
                tcp_roundtrip(stack.remote_tcp, payload, timeout=10.0),
                "short TCP roundtrip failed",
            )
            latencies.append(time.perf_counter() - started)
        latencies.sort()
        p50 = latencies[len(latencies) // 2]
        p95 = latencies[min(len(latencies) - 1, int(len(latencies) * 0.95))]
        print(
            f"[perf] TCP short-connect: p50={p50 * 1000:.2f}ms "
            f"p95={p95 * 1000:.2f}ms max={latencies[-1] * 1000:.2f}ms"
        )
        self.assertLess(p95, 5.0, "short-connect p95 exceeded 5s")

    def test_repeated_tcp_connections_release_resources(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(tcp_local=echo.port)
        server_pid = stack.server_proc.pid
        client_pid = stack.client_proc.pid
        server_fds_before = process_fd_count(server_pid)
        client_fds_before = process_fd_count(client_pid)
        server_rss_before = process_rss_kib(server_pid)

        rounds = 300
        payload = b"r" * 128
        for _ in range(rounds):
            self.assertTrue(
                tcp_roundtrip(stack.remote_tcp, payload, timeout=5.0),
                "repeated TCP roundtrip failed",
            )

        status = stack.wait_status(
            lambda s: s["metrics"]["total_connections"] >= rounds
            and s["metrics"]["active_connections"] == 0
        )
        self.assertGreaterEqual(status["metrics"]["total_connections"], rounds)
        self.assertEqual(
            status["metrics"]["active_connections"], 0, "TCP connections leaked"
        )
        self.assertIsNone(stack.server_proc.poll())
        self.assertIsNone(stack.client_proc.poll())

        server_fds_after = process_fd_count(server_pid)
        client_fds_after = process_fd_count(client_pid)
        server_rss_after = process_rss_kib(server_pid)
        if server_fds_before is not None and server_fds_after is not None:
            growth = server_fds_after - server_fds_before
            print(
                f"\n[stab] rfrps fds: {server_fds_before} -> {server_fds_after} "
                f"(growth {growth})"
            )
            self.assertLess(growth, 64, "rfrps file descriptors leaked")
        if client_fds_before is not None and client_fds_after is not None:
            growth = client_fds_after - client_fds_before
            print(
                f"[stab] rfrpc fds: {client_fds_before} -> {client_fds_after} "
                f"(growth {growth})"
            )
            self.assertLess(growth, 64, "rfrpc file descriptors leaked")
        if server_rss_before is not None and server_rss_after is not None:
            growth_mib = (server_rss_after - server_rss_before) / 1024
            print(
                f"[stab] rfrps RSS: {server_rss_before / 1024:.1f} -> "
                f"{server_rss_after / 1024:.1f} MiB (growth {growth_mib:.1f})"
            )
            self.assertLess(growth_mib, 256, "rfrps RSS grew unexpectedly")

        self.assertTrue(tcp_roundtrip(stack.remote_tcp, b"still-alive"))

    def test_repeated_udp_sessions_release_resources(self) -> None:
        echo = self.udp_echo()
        stack = self.make_stack(udp_local=echo.port, udp_timeout=1)
        server_pid = stack.server_proc.pid
        fds_before = process_fd_count(server_pid)
        sessions = 20
        payload = b"udp-churn"
        for _ in range(sessions):
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                sock.connect(("127.0.0.1", stack.remote_udp))
                self.assertTrue(
                    udp_roundtrip(sock, payload, timeout=3.0),
                    "UDP churn roundtrip failed",
                )
            finally:
                sock.close()

        status = stack.wait_status(
            lambda s: s["metrics"]["udp_sessions_total"] >= sessions
        )
        self.assertGreaterEqual(status["metrics"]["udp_sessions_total"], sessions)
        self.assertTrue(
            wait_until(lambda: stack.status()["udp_sessions"] == 0, 10.0),
            "idle UDP sessions were not swept",
        )
        self.assertIsNone(stack.server_proc.poll())
        self.assertIsNone(stack.client_proc.poll())

        fds_after = process_fd_count(server_pid)
        if fds_before is not None and fds_after is not None:
            growth = fds_after - fds_before
            print(
                f"\n[stab] rfrps fds after UDP churn: {fds_before} -> {fds_after} "
                f"(growth {growth})"
            )
            self.assertLess(growth, 64, "UDP sessions leaked file descriptors")

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addCleanup(sock.close)
        sock.connect(("127.0.0.1", stack.remote_udp))
        self.assertTrue(
            udp_roundtrip(sock, b"after-churn"), "UDP proxy unusable after churn"
        )

    def test_sustained_mixed_load(self) -> None:
        tcp_echo = self.tcp_echo()
        udp_echo = self.udp_echo()
        stack = self.make_stack(tcp_local=tcp_echo.port, udp_local=udp_echo.port)

        stop = threading.Event()
        lock = threading.Lock()
        counters = {"tcp_ok": 0, "tcp_fail": 0, "udp_ok": 0, "udp_fail": 0}

        def tcp_worker() -> None:
            payload = b"mixed-tcp" * 8
            while not stop.is_set():
                ok = tcp_roundtrip(stack.remote_tcp, payload, timeout=3.0)
                with lock:
                    counters["tcp_ok" if ok else "tcp_fail"] += 1

        def udp_worker() -> None:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.connect(("127.0.0.1", stack.remote_udp))
            payload = b"mixed-udp"
            try:
                while not stop.is_set():
                    ok = udp_roundtrip(sock, payload, timeout=2.0)
                    with lock:
                        counters["udp_ok" if ok else "udp_fail"] += 1
            finally:
                sock.close()

        threads = [
            threading.Thread(target=tcp_worker, daemon=True),
            threading.Thread(target=udp_worker, daemon=True),
        ]
        for thread in threads:
            thread.start()

        deadline = time.monotonic() + 5.0
        checks = 0
        while time.monotonic() < deadline:
            status = stack.status()
            self.assertTrue(status["metrics"]["accepting"], "accept loop unhealthy")
            self.assertIsNone(stack.server_proc.poll())
            self.assertIsNone(stack.client_proc.poll())
            checks += 1
            time.sleep(0.5)

        stop.set()
        for thread in threads:
            thread.join(timeout=5)

        with lock:
            snapshot = dict(counters)
        print(f"\n[stab] mixed load 5s ({checks} dashboard checks): {snapshot}")
        self.assertGreater(snapshot["tcp_ok"], 0)
        self.assertGreater(snapshot["udp_ok"], 0)
        self.assertEqual(snapshot["tcp_fail"], 0, "TCP failed during mixed load")
        udp_total = snapshot["udp_ok"] + snapshot["udp_fail"]
        if udp_total:
            success = snapshot["udp_ok"] / udp_total
            self.assertGreaterEqual(success, 0.8, "UDP success rate below 80%")
        self.assertTrue(tcp_roundtrip(stack.remote_tcp, b"after-mixed"))

    def test_repeated_server_restarts(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(tcp_local=echo.port)
        for round_no in range(1, 4):
            self.assertTrue(
                tcp_roundtrip(stack.remote_tcp, f"before-{round_no}".encode()),
                f"TCP proxy failed before restart {round_no}",
            )
            stack.restart_server()
            self.assertTrue(
                tcp_roundtrip(stack.remote_tcp, f"after-{round_no}".encode()),
                f"TCP proxy failed after restart {round_no}",
            )
            status = stack.wait_status(lambda s: len(s["sessions"]) == 1)
            self.assertEqual(len(status["sessions"]), 1)
            self.assertIsNone(stack.server_proc.poll())
            self.assertIsNone(stack.client_proc.poll())


if __name__ == "__main__":
    BIN = find_binary()
    unittest.main(verbosity=2)
