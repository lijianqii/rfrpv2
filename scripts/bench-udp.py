#!/usr/bin/env python3
"""UDP 代理端到端压测（RDP-UDP 场景）。

用法（先 `cargo build --release`）：

    scripts/bench-udp.py burst     # 单会话突发：端到端回收率 + 服务端 rfrp_udp_dropped_total
    scripts/bench-udp.py latency   # 200B 小包往返：直连 vs 代理（P50/P95/P99）

脚本会临时拉起 target/release/rfrp 的 server/client 与本地 UDP echo，结束后自动清理。
端口固定使用 17xxx（控制/Dashboard）、19xxx（本地 echo）、339xx（公网侧代理端口），
避免与常用端口冲突。
"""

import base64
import os
import re
import socket
import statistics
import subprocess
import sys
import threading
import time
import urllib.request

BIN = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "target/release/rfrp"))
ECHO_UDP = 19500
CONTROL_PORT = 17300
DASH_PORT = 17600
SHARED_PORT = 33950  # RDP 场景：TCP/UDP 共用同一公网端口


def udp_echo_server(port: int) -> None:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", port))
    while True:
        data, peer = s.recvfrom(65535)
        s.sendto(data, peer)


def write_configs() -> None:
    with open("/tmp/rfrp-bench-udp-s.toml", "w") as f:
        f.write(
            f"""[server]
bind_addr = "127.0.0.1"
bind_port = {CONTROL_PORT}
token = "bench"
tls_enable = false
work_conn_tls = false

[proxy]
allow_ports = "{SHARED_PORT}"

[dashboard]
addr = "127.0.0.1:{DASH_PORT}"
user = "admin"
password = "secret123"

[log]
level = "warn"
"""
        )
    with open("/tmp/rfrp-bench-udp-c.toml", "w") as f:
        f.write(
            f"""[client]
server_addr = "127.0.0.1"
server_port = {CONTROL_PORT}
token = "bench"
tls_enable = false
work_conn_tls = false
run_id_file = "/tmp/rfrp-bench-udp.runid"

[[proxy]]
name = "bench-udp"
type = "udp"
local_ip = "127.0.0.1"
local_port = {ECHO_UDP}
remote_port = {SHARED_PORT}
pool_size = 0

[log]
level = "warn"
"""
        )


def start_stack():
    write_configs()
    log = open("/tmp/rfrp-bench-udp.log", "w")
    procs = [
        subprocess.Popen([BIN, "server", "-c", "/tmp/rfrp-bench-udp-s.toml"], stdout=log, stderr=subprocess.STDOUT),
        subprocess.Popen([BIN, "client", "-c", "/tmp/rfrp-bench-udp-c.toml"], stdout=log, stderr=subprocess.STDOUT),
    ]
    deadline = time.time() + 10
    while time.time() < deadline:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(1)
        try:
            s.sendto(b"warm", ("127.0.0.1", SHARED_PORT))
            s.recvfrom(64)
            s.close()
            return procs
        except OSError:
            s.close()
            time.sleep(0.1)
    for p in procs:
        p.terminate()
    raise SystemExit("代理未就绪，见 /tmp/rfrp-bench-udp.log")


def metric(name: str) -> int:
    req = urllib.request.Request(f"http://127.0.0.1:{DASH_PORT}/metrics")
    req.add_header("Authorization", "Basic " + base64.b64encode(b"admin:secret123").decode())
    text = urllib.request.urlopen(req, timeout=3).read().decode()
    m = re.search(rf"^{name} (\d+)$", text, re.M)
    return int(m.group(1)) if m else 0


def cmd_burst(n: int = 2000, size: int = 1000) -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(0.5)
    sock.connect(("127.0.0.1", SHARED_PORT))
    # 用将要发突发的同一个源端口先完成一次往返：否则首包落在"待配对"窗口，
    # 测到的是建连延迟而不是队列行为。
    sock.send(b"warm")
    sock.recv(64)
    before = metric("rfrp_udp_dropped_total")

    msg = b"u" * size
    t0 = time.perf_counter()
    for _ in range(n):
        try:
            sock.send(msg)
        except OSError:
            pass
    send_s = time.perf_counter() - t0

    got, buf = 0, bytearray(4096)
    t1 = time.perf_counter()
    while time.perf_counter() - t1 < 3.0 and got < n:
        try:
            sock.recv_into(buf)
            got += 1
        except socket.timeout:
            break
    sock.close()
    dropped = metric("rfrp_udp_dropped_total") - before
    print(f"发送 {n} × {size}B（{n / send_s / 1000:.0f}k pps）")
    print(f"端到端回收: {got}/{n}（{got / n * 100:.1f}%）")
    print(f"服务端应用层丢包 (rfrp_udp_dropped_total): {dropped}")


def rtt_stats(port: int, iters: int = 2000, size: int = 200):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(2)
    s.connect(("127.0.0.1", port))
    msg, buf = b"r" * size, bytearray(4096)
    for _ in range(50):
        s.send(msg)
        s.recv_into(buf)
    samples = []
    for _ in range(iters):
        t0 = time.perf_counter()
        s.send(msg)
        s.recv_into(buf)
        samples.append((time.perf_counter() - t0) * 1e6)
    s.close()
    samples.sort()
    return (
        statistics.median(samples),
        samples[int(len(samples) * 0.95)],
        samples[int(len(samples) * 0.99)],
    )


def cmd_latency() -> None:
    direct = rtt_stats(ECHO_UDP)
    proxied = rtt_stats(SHARED_PORT)
    print(f"{'路径':<22} {'P50 µs':>9} {'P95 µs':>9} {'P99 µs':>9}")
    for name, (p50, p95, p99) in (("直连 UDP echo", direct), ("rfrp UDP 代理", proxied)):
        print(f"{name:<22} {p50:>9.1f} {p95:>9.1f} {p99:>9.1f}")


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "burst"
    if mode not in ("burst", "latency"):
        raise SystemExit(__doc__)
    if not os.path.exists(BIN):
        raise SystemExit(f"未找到 {BIN}，请先 `cargo build --release`")

    threading.Thread(target=udp_echo_server, args=(ECHO_UDP,), daemon=True).start()
    procs = start_stack()
    try:
        cmd_burst() if mode == "burst" else cmd_latency()
    finally:
        for p in procs:
            p.terminate()


if __name__ == "__main__":
    main()
