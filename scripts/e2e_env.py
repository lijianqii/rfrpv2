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


def child_pids(pid: int) -> list[int]:
    """Linux 下返回直接子进程 PID；其他平台返回空。"""
    if os.name != "posix":
        return []
    try:
        out = subprocess.check_output(
            ["pgrep", "-P", str(pid)], text=True, stderr=subprocess.DEVNULL
        )
    except (OSError, subprocess.CalledProcessError):
        return []
    return [int(line) for line in out.splitlines() if line.strip().isdigit()]


def server_child_pids(pid: int) -> list[int]:
    """supervisor 的直接子进程中排除 `--check` 校验进程。"""
    result = []
    for child in child_pids(pid):
        try:
            cmdline = Path(f"/proc/{child}/cmdline").read_bytes()
        except OSError:
            continue
        if b"--check" not in cmdline:
            result.append(child)
    return result


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
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
        server_extra_args: Optional[list[str]] = None,
        client_extra_args: Optional[list[str]] = None,
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
        self.server_extra_args = list(server_extra_args or [])
        self.client_extra_args = list(client_extra_args or [])

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
        extra = self.server_extra_args if role == "server" else self.client_extra_args
        proc = subprocess.Popen(
            [str(BIN), role, "-c", str(config), *extra],
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
        return stack

    def test_watch_config_exit_mode(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(
            tcp_local=echo.port,
            log_level="info",
            server_extra_args=["--watch-config=exit"],
        )
        original = stack.server_config.read_text(encoding="utf-8")

        stack.server_config.write_text("invalid = [\n", encoding="utf-8")
        time.sleep(2)
        self.assertIsNone(
            stack.server_proc.poll(),
            "invalid config must not exit watch-config=exit process",
        )
        self.assertTrue(
            tcp_roundtrip(stack.remote_tcp, b"exit-invalid"),
            "proxy broke after invalid config in exit mode",
        )

        stack.server_config.write_text(
            original.replace(
                'password = "secret123"', 'password = "exit-new-pass"'
            ),
            encoding="utf-8",
        )
        code = stack.server_proc.wait(timeout=15)
        self.assertEqual(
            code,
            75,
            "valid config change should exit with code 75 for supervisor restart",
        )

    @unittest.skipUnless(os.name == "posix", "supervisor child inspection requires pgrep")
    def test_watch_config_restart_mode(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(
            tcp_local=echo.port,
            log_level="info",
            server_extra_args=["--watch-config=restart"],
        )
        parent = stack.server_proc
        original = stack.server_config.read_text(encoding="utf-8")
        self.assertTrue(
            wait_until(lambda: len(server_child_pids(parent.pid)) == 1, 10.0),
            "supervisor did not start exactly one child",
        )
        old_child = server_child_pids(parent.pid)[0]

        stack.server_config.write_text("invalid = [\n", encoding="utf-8")
        time.sleep(2)
        self.assertIsNone(parent.poll())
        self.assertEqual(
            server_child_pids(parent.pid),
            [old_child],
            "invalid config must not replace the child",
        )
        self.assertTrue(
            tcp_roundtrip(stack.remote_tcp, b"restart-invalid"),
            "proxy broke after invalid config in restart mode",
        )

        stack.server_config.write_text(
            original.replace(
                'password = "secret123"', 'password = "restart-new-pass"'
            ),
            encoding="utf-8",
        )
        self.assertTrue(
            wait_until(
                lambda: len(server_child_pids(parent.pid)) == 1
                and server_child_pids(parent.pid)[0] != old_child,
                15.0,
            ),
            "supervisor did not replace the child after valid config change",
        )
        new_child = server_child_pids(parent.pid)[0]
        self.assertIsNone(parent.poll(), "supervisor parent exited unexpectedly")
        last_dashboard = []

        def dashboard_ok() -> bool:
            try:
                code, body = http_get(
                    stack.dashboard_port,
                    "/api/status",
                    password="restart-new-pass",
                )
                last_dashboard.append((code, body[:120]))
                return code == 200
            except Exception as exc:
                last_dashboard.append(("exception", repr(exc)))
                return False

        if not wait_until(dashboard_ok, 15.0):
            print(
                f"dashboard port={stack.dashboard_port} probe tail={last_dashboard[-8:]} new_child={new_child}",
                file=sys.stderr,
            )
            self.fail("child did not start with the new config")
        self.assertTrue(
            wait_until(
                lambda: tcp_roundtrip(
                    stack.remote_tcp, b"restart-valid", timeout=1.0
                ),
                15.0,
            ),
            "proxy did not recover after supervised child restart",
        )

        parent.terminate()
        parent.wait(timeout=15)
        self.assertTrue(
            wait_until(lambda: not pid_alive(new_child), 10.0),
            "supervisor left an orphan child process",
        )

    def test_watch_config_exit_mode_client(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(
            tcp_local=echo.port,
            log_level="info",
            client_extra_args=["--watch-config=exit"],
        )
        original = stack.client_config.read_text(encoding="utf-8")
        stack.client_config.write_text("invalid = [\n", encoding="utf-8")
        time.sleep(2)
        self.assertIsNone(
            stack.client_proc.poll(),
            "invalid config must not exit client watch-config=exit process",
        )
        self.assertTrue(
            tcp_roundtrip(stack.remote_tcp, b"client-exit-invalid"),
            "proxy broke after invalid client config",
        )
        stack.client_config.write_text(
            original.replace("pool_size = 1", "pool_size = 0"),
            encoding="utf-8",
        )
        code = stack.client_proc.wait(timeout=15)
        self.assertEqual(code, 75, "client config change should exit with 75")

    def test_watch_config_check_bypasses_supervisor(self) -> None:
        binary = BIN
        self.assertIsNotNone(binary)
        result = subprocess.run(
            [
                str(binary),
                "server",
                "-c",
                str(ROOT / "examples" / "rfrp-server.toml"),
                "--watch-config=restart",
                "--check",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(
            result.returncode,
            0,
            f"--check with --watch-config must not supervise: {result.stderr}",
        )
        self.assertIn("server config OK", result.stdout)

    @unittest.skipUnless(os.name == "posix", "supervisor child inspection requires pgrep")
    def test_watch_config_restart_rollback_on_startup_failure(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(
            tcp_local=echo.port,
            log_level="info",
            server_extra_args=["--watch-config=restart"],
        )
        parent = stack.server_proc
        original = stack.server_config.read_text(encoding="utf-8")
        self.assertTrue(
            wait_until(lambda: len(server_child_pids(parent.pid)) == 1, 10.0),
            "supervisor did not start a child",
        )
        old_child = server_child_pids(parent.pid)[0]

        blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        blocker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        self.addCleanup(blocker.close)
        blocked = blocker.getsockname()[1]
        stack.server_config.write_text(
            original.replace(
                f"bind_port = {stack.control_port}",
                f"bind_port = {blocked}",
            ),
            encoding="utf-8",
        )
        self.assertTrue(
            wait_until(
                lambda: len(server_child_pids(parent.pid)) == 1
                and server_child_pids(parent.pid)[0] != old_child,
                20.0,
            ),
            "supervisor did not start a rollback child",
        )
        self.assertIsNone(parent.poll())
        self.assertTrue(
            wait_until(
                lambda: tcp_roundtrip(
                    stack.remote_tcp, b"rollback-old-config", timeout=1.0
                ),
                15.0,
            ),
            "rollback to old config did not restore the proxy",
        )
        self.assertTrue(
            tcp_connectable(stack.control_port),
            "rollback child did not bind the old control port",
        )

        blocker.close()
        stack.server_config.write_text(original, encoding="utf-8")
        self.assertTrue(
            wait_until(
                lambda: tcp_roundtrip(
                    stack.remote_tcp, b"after-rollback-recovery", timeout=1.0
                ),
                20.0,
            ),
            "proxy did not recover after restoring config",
        )

    @unittest.skipUnless(os.name == "posix", "SIGKILL test requires POSIX")
    def test_watch_config_parent_sigkill_cleans_child(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(
            tcp_local=echo.port,
            server_extra_args=["--watch-config=restart"],
        )
        parent = stack.server_proc
        self.assertTrue(
            wait_until(lambda: len(server_child_pids(parent.pid)) == 1, 10.0)
        )
        child = server_child_pids(parent.pid)[0]
        os.kill(parent.pid, signal.SIGKILL)
        parent.wait(timeout=5)
        self.assertTrue(
            wait_until(lambda: not pid_alive(child), 10.0),
            "child survived after supervisor was SIGKILLed",
        )

    @unittest.skipUnless(os.name == "posix", "supervisor child inspection requires pgrep")
    def test_watch_config_restart_mode_client(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(
            tcp_local=echo.port,
            log_level="info",
            client_extra_args=["--watch-config=restart"],
        )
        parent = stack.client_proc
        original = stack.client_config.read_text(encoding="utf-8")
        self.assertTrue(
            wait_until(lambda: len(server_child_pids(parent.pid)) == 1, 10.0)
        )
        old_child = server_child_pids(parent.pid)[0]

        stack.client_config.write_text("invalid = [\n", encoding="utf-8")
        time.sleep(2)
        self.assertIsNone(parent.poll())
        self.assertEqual(
            server_child_pids(parent.pid),
            [old_child],
            "invalid config must not replace client child",
        )
        self.assertTrue(
            tcp_roundtrip(stack.remote_tcp, b"client-restart-invalid"),
            "proxy broke after invalid client config",
        )

        stack.client_config.write_text(
            original.replace(
                "heartbeat_interval_secs = 5", "heartbeat_interval_secs = 6"
            ),
            encoding="utf-8",
        )
        self.assertTrue(
            wait_until(
                lambda: len(server_child_pids(parent.pid)) == 1
                and server_child_pids(parent.pid)[0] != old_child,
                15.0,
            ),
            "client supervisor did not replace the child",
        )
        self.assertIsNone(parent.poll())
        self.assertTrue(
            wait_until(
                lambda: tcp_roundtrip(
                    stack.remote_tcp, b"client-restart-valid", timeout=1.0
                ),
                15.0,
            ),
            "proxy did not recover after client child restart",
        )

        new_child = server_child_pids(parent.pid)[0]
        parent.terminate()
        parent.wait(timeout=15)
        self.assertTrue(
            wait_until(lambda: not pid_alive(new_child), 10.0),
            "client supervisor left an orphan child",
        )

    @unittest.skipUnless(os.name == "posix", "supervisor child inspection requires pgrep")
    def test_watch_config_restart_child_crash(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(
            tcp_local=echo.port,
            log_level="info",
            server_extra_args=["--watch-config=restart"],
        )
        parent = stack.server_proc
        self.assertTrue(
            wait_until(lambda: len(server_child_pids(parent.pid)) == 1, 10.0)
        )
        old_child = server_child_pids(parent.pid)[0]
        os.kill(old_child, signal.SIGKILL)
        self.assertTrue(
            wait_until(
                lambda: len(server_child_pids(parent.pid)) == 1
                and server_child_pids(parent.pid)[0] != old_child,
                10.0,
            ),
            "supervisor did not restart a crashed child",
        )
        self.assertIsNone(parent.poll())
        self.assertTrue(
            wait_until(
                lambda: tcp_roundtrip(
                    stack.remote_tcp, b"after-child-crash", timeout=1.0
                ),
                15.0,
            ),
            "proxy did not recover after child crash",
        )

    def test_watch_config_rapid_saves(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(
            tcp_local=echo.port,
            log_level="info",
            server_extra_args=["--watch-config=restart"],
        )
        parent = stack.server_proc
        original = stack.server_config.read_text(encoding="utf-8")
        log_path = self.temp / "server.log"
        before = (
            log_path.read_text(encoding="utf-8", errors="replace").count(
                "config valid; restarting child"
            )
            if log_path.is_file()
            else 0
        )
        stack.server_config.write_text(
            original.replace(
                'password = "secret123"', 'password = "rapid-first"'
            ),
            encoding="utf-8",
        )
        stack.server_config.write_text(
            original.replace(
                'password = "secret123"', 'password = "rapid-final"'
            ),
            encoding="utf-8",
        )
        self.assertTrue(
            wait_until(
                lambda: http_get(
                    stack.dashboard_port,
                    "/api/status",
                    password="rapid-final",
                )[0]
                == 200,
                15.0,
            ),
            "final rapid-save config was not applied",
        )
        after = (
            log_path.read_text(encoding="utf-8", errors="replace").count(
                "config valid; restarting child"
            )
            if log_path.is_file()
            else 0
        )
        self.assertEqual(
            after - before,
            1,
            "rapid saves should coalesce into one supervisor restart",
        )
        self.assertIsNone(parent.poll())
        self.assertTrue(
            wait_until(
                lambda: tcp_roundtrip(
                    stack.remote_tcp, b"after-rapid-saves", timeout=1.0
                ),
                15.0,
            ),
            "proxy did not recover after rapid saves",
        )

    def test_watch_config_delete_and_recreate(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(
            tcp_local=echo.port,
            log_level="info",
            server_extra_args=["--watch-config=restart"],
        )
        parent = stack.server_proc
        original = stack.server_config.read_text(encoding="utf-8")
        self.assertTrue(
            wait_until(lambda: len(server_child_pids(parent.pid)) == 1, 10.0)
        )
        old_child = server_child_pids(parent.pid)[0]
        stack.server_config.unlink()
        time.sleep(2)
        self.assertIsNone(parent.poll())
        self.assertEqual(
            server_child_pids(parent.pid),
            [old_child],
            "deleting config must not replace the child",
        )
        self.assertTrue(
            tcp_roundtrip(stack.remote_tcp, b"after-config-delete"),
            "proxy broke after config deletion",
        )
        stack.server_config.write_text(
            original.replace(
                'password = "secret123"', 'password = "recreated-config"'
            ),
            encoding="utf-8",
        )
        self.assertTrue(
            wait_until(
                lambda: len(server_child_pids(parent.pid)) == 1
                and server_child_pids(parent.pid)[0] != old_child,
                15.0,
            ),
            "recreated config did not trigger a restart",
        )
        self.assertTrue(
            wait_until(
                lambda: http_get(
                    stack.dashboard_port,
                    "/api/status",
                    password="recreated-config",
                )[0]
                == 200,
                15.0,
            ),
            "recreated config was not applied",
        )

    @unittest.skipUnless(os.name == "posix", "supervisor child inspection requires pgrep")
    def test_watch_config_cli_overrides_survive_restart_and_rollback(self) -> None:
        echo = self.tcp_echo()
        stack = self.make_stack(
            tcp_local=echo.port,
            log_level="info",
            server_extra_args=[
                "--watch-config=restart",
                "--token",
                "cli-token",
            ],
            client_extra_args=["--token", "cli-token"],
        )
        parent = stack.server_proc
        original = stack.server_config.read_text(encoding="utf-8")
        self.assertTrue(
            wait_until(lambda: len(server_child_pids(parent.pid)) == 1, 10.0)
        )
        old_child = server_child_pids(parent.pid)[0]

        # 合法配置重启：--token 覆盖必须继续生效，客户端才能重连。
        changed = original.replace(
            'password = "secret123"', 'password = "cli-override-pass"'
        )
        stack.server_config.write_text(changed, encoding="utf-8")
        self.assertTrue(
            wait_until(
                lambda: len(server_child_pids(parent.pid)) == 1
                and server_child_pids(parent.pid)[0] != old_child,
                15.0,
            ),
            "CLI override test did not restart child",
        )
        self.assertTrue(
            wait_until(
                lambda: http_get(
                    stack.dashboard_port,
                    "/api/status",
                    password="cli-override-pass",
                )[0]
                == 200,
                15.0,
            ),
            "config with CLI override was not applied",
        )
        self.assertTrue(
            wait_until(
                lambda: tcp_roundtrip(
                    stack.remote_tcp, b"cli-override-restart", timeout=1.0
                ),
                15.0,
            ),
            "CLI token override was lost after restart",
        )

        # 启动失败回滚：仍有 --token 覆盖，回滚子进程也应继续服务。
        current = stack.server_config.read_text(encoding="utf-8")
        old_child = server_child_pids(parent.pid)[0]
        blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        blocker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        self.addCleanup(blocker.close)
        blocked = blocker.getsockname()[1]
        stack.server_config.write_text(
            current.replace(
                f"bind_port = {stack.control_port}",
                f"bind_port = {blocked}",
            ),
            encoding="utf-8",
        )
        self.assertTrue(
            wait_until(
                lambda: len(server_child_pids(parent.pid)) == 1
                and server_child_pids(parent.pid)[0] != old_child,
                20.0,
            ),
            "CLI override rollback did not start a child",
        )
        self.assertTrue(
            wait_until(
                lambda: tcp_roundtrip(
                    stack.remote_tcp, b"cli-override-rollback", timeout=1.0
                ),
                15.0,
            ),
            "CLI token override was lost after rollback",
        )
        self.assertTrue(
            http_get(
                stack.dashboard_port,
                "/api/status",
                password="cli-override-pass",
            )[0]
            == 200,
            "rollback config was not the last good config",
        )
        blocker.close()

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
