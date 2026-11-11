#!/usr/bin/env python3
"""长时间稳定性测试（soak）：真实 rfrps + rfrpc + 混合负载 + 故障注入。

默认 10 分钟冒烟；通过环境变量拉长到 1 小时 / 8 小时：

    E2E_SOAK_SECS=600  RFRP_BIN=target/debug/rfrp   python3 scripts/soak_env.py
    E2E_SOAK_SECS=3600 RFRP_BIN=target/release/rfrp python3 scripts/soak_env.py
    E2E_SOAK_SECS=28800 RFRP_BIN=target/release/rfrp python3 scripts/soak_env.py

输出：
    target/soak-<时间戳>.jsonl              每 10 秒采样 + 事件
    target/soak-<时间戳>-summary.json       机器可读摘要
    target/soak-<时间戳>-summary.md         人类可读摘要
    临时目录（默认保留）                      server.toml / client.toml / rfrps.log / rfrpc.log

判定：
    - 进程崩溃、无任何有效负载、样本严重缺失：始终失败（退出码 1）；
    - E2E_SOAK_STRICT=1 时额外按成功率/fd/RSS/静默回落阈值判定；
    - 默认 E2E_SOAK_STRICT=0，用于 10 分钟冒烟和 1 小时基线采集。
"""

from __future__ import annotations

import json
import os
import random
import socket
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
import e2e_env as base

ROOT = base.ROOT
DURATION = max(1, int(os.environ.get("E2E_SOAK_SECS", "600")))
SAMPLE_SECS = max(1.0, float(os.environ.get("E2E_SOAK_SAMPLE_SECS", "10")))
PULSE_SECS = max(0, int(os.environ.get("E2E_SOAK_PULSE_SECS", "300")))
RESTART_SECS = max(0, int(os.environ.get("E2E_SOAK_RESTART_SECS", "1800")))
FREEZE_SECS = max(0, int(os.environ.get("E2E_SOAK_FREEZE_SECS", "1800")))
SILENCE_SECS = max(0, int(os.environ.get("E2E_SOAK_SILENCE_SECS", "600")))
FREEZE_HOLD_SECS = max(1, int(os.environ.get("E2E_SOAK_FREEZE_HOLD_SECS", "12")))
SILENCE_HOLD_SECS = max(1, int(os.environ.get("E2E_SOAK_SILENCE_HOLD_SECS", "12")))

STRICT = os.environ.get("E2E_SOAK_STRICT", "0") == "1"
KEEP_LOGS = os.environ.get("E2E_SOAK_KEEP_LOGS", "1") != "0"
TCP_MIN = float(os.environ.get("E2E_SOAK_TCP_MIN", "0.999"))
UDP_MIN = float(os.environ.get("E2E_SOAK_UDP_MIN", "0.95"))
FD_MAX = int(os.environ.get("E2E_SOAK_FD_MAX", "64"))
RSS_MAX_MIB = float(os.environ.get("E2E_SOAK_RSS_MAX_MIB", "128"))
SILENCE_FAIL_MAX = int(os.environ.get("E2E_SOAK_SILENCE_FAIL_MAX", "0"))
MIN_SAMPLE_RATIO = float(os.environ.get("E2E_SOAK_MIN_SAMPLE_RATIO", "0.9"))
LOG_MAX_MIB = float(os.environ.get("E2E_SOAK_LOG_MAX_MIB", "512"))
TCP_WORKERS = max(1, int(os.environ.get("E2E_SOAK_TCP_WORKERS", "4")))
UDP_WORKERS = max(1, int(os.environ.get("E2E_SOAK_UDP_WORKERS", "2")))
TCP_INTERVAL_MS = max(1, int(os.environ.get("E2E_SOAK_TCP_INTERVAL_MS", "100")))
UDP_INTERVAL_MS = max(1, int(os.environ.get("E2E_SOAK_UDP_INTERVAL_MS", "50")))
LOG_LEVEL = os.environ.get("E2E_SOAK_LOG_LEVEL", "info")
REPORT_DIR = Path(os.environ.get("E2E_SOAK_REPORT_DIR", str(ROOT / "target")))
TCP_PAYLOAD = b"s" * 1024
UDP_PAYLOAD = b"u" * 200


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(len(ordered) * p))
    return ordered[index]


def log_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


class SoakContext:
    """满足 `e2e_env.Stack` 对测试上下文的最小接口。"""

    def __init__(self, temp: Path) -> None:
        self.temp = temp
        self.processes: list[tuple[object, object]] = []

    def logs_tail(self, lines: int = 60) -> str:
        parts = []
        for name in ("rfrps.log", "rfrpc.log"):
            path = self.temp / name
            if path.is_file():
                text = path.read_text(encoding="utf-8", errors="replace").splitlines()
                parts.append(f"--- {name} ---\n" + "\n".join(text[-lines:]))
        return "\n".join(parts)


class Stats:
    """线程安全的窗口统计：每次采样时取出并清零。"""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.ok = 0
        self.fail = 0
        self.latencies: list[float] = []

    def record(self, ok: bool, latency: float) -> None:
        with self.lock:
            if ok:
                self.ok += 1
            else:
                self.fail += 1
            if len(self.latencies) < 5000:
                self.latencies.append(latency)

    def take(self) -> tuple[int, int, list[float]]:
        with self.lock:
            result = (self.ok, self.fail, self.latencies)
            self.ok = 0
            self.fail = 0
            self.latencies = []
            return result


def write_jsonl(handle, record: dict, lock: threading.Lock) -> None:
    # 只 flush 不 fsync：10 秒一行，进程正常退出时数据完整；fsync 会明显拖慢
    # 采样线程并造成采样节拍漂移。
    with lock:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        handle.flush()


def build_workers(
    stack,
    stop: threading.Event,
    pause: threading.Event,
    tcp_stats: Stats,
    udp_stats: Stats,
) -> list[threading.Thread]:
    def tcp_worker() -> None:
        while not stop.is_set():
            if pause.is_set():
                time.sleep(0.05)
                continue
            started = time.perf_counter()
            ok = base.tcp_roundtrip(stack.remote_tcp, TCP_PAYLOAD, timeout=5.0)
            tcp_stats.record(ok, time.perf_counter() - started)
            time.sleep(random.uniform(0.0, TCP_INTERVAL_MS / 1000.0))

    def udp_worker() -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("127.0.0.1", stack.remote_udp))
        try:
            while not stop.is_set():
                if pause.is_set():
                    time.sleep(0.05)
                    continue
                started = time.perf_counter()
                ok = base.udp_roundtrip(sock, UDP_PAYLOAD, timeout=2.0)
                udp_stats.record(ok, time.perf_counter() - started)
                time.sleep(random.uniform(0.0, UDP_INTERVAL_MS / 1000.0))
        finally:
            sock.close()

    workers = [threading.Thread(target=tcp_worker, daemon=True) for _ in range(TCP_WORKERS)]
    workers += [threading.Thread(target=udp_worker, daemon=True) for _ in range(UDP_WORKERS)]
    return workers


def summarize(
    samples: list[dict],
    events: list[dict],
    duration: float,
    report_path: Path,
    temp: Path,
    sample_duration: Optional[float] = None,
) -> tuple[dict, list[str], list[str]]:
    warnings: list[str] = []
    fatal: list[str] = []

    tcp_ok = sum(s["load"]["tcp_ok"] for s in samples)
    tcp_fail = sum(s["load"]["tcp_fail"] for s in samples)
    udp_ok = sum(s["load"]["udp_ok"] for s in samples)
    udp_fail = sum(s["load"]["udp_fail"] for s in samples)
    tcp_total = tcp_ok + tcp_fail
    udp_total = udp_ok + udp_fail
    tcp_rate = tcp_ok / tcp_total if tcp_total else 0.0
    udp_rate = udp_ok / udp_total if udp_total else 0.0

    effective_sample_duration = (
        sample_duration if sample_duration is not None else duration
    )
    expected_samples = max(1, int(effective_sample_duration / SAMPLE_SECS))
    sample_ratio = min(1.0, len(samples) / expected_samples)
    silence_events = [e for e in events if e.get("event") == "silence"]
    silence_failures = [e for e in silence_events if not e.get("ok")]
    restart_events = [e for e in events if str(e.get("event", "")).startswith("restart_")]
    restart_failures = [e for e in restart_events if not e.get("ok")]
    freeze_events = [e for e in events if e.get("event") == "freeze_client"]
    freeze_failures = [e for e in freeze_events if not e.get("ok")]

    def resource_value(sample: dict, key: str) -> Optional[int]:
        value = sample.get("resources", {}).get(key)
        return value if isinstance(value, int) else None

    last = samples[-1] if samples else {}
    # 计划重启会替换进程：基线必须取“最后一个 server/client 进程的首次样本”，
    # 不能跨代比较 fd/RSS，否则会误报泄漏或掩盖泄漏。
    last_server_pid = resource_value(last, "server_pid")
    last_client_pid = resource_value(last, "client_pid")
    server_samples = [
        s for s in samples
        if last_server_pid is None
        or resource_value(s, "server_pid") == last_server_pid
    ]
    client_samples = [
        s for s in samples
        if last_client_pid is None
        or resource_value(s, "client_pid") == last_client_pid
    ]
    server_first = server_samples[0] if server_samples else (samples[0] if samples else {})
    client_first = client_samples[0] if client_samples else (samples[0] if samples else {})
    server_fd_first = resource_value(server_first, "server_fd")
    server_fd_last = resource_value(last, "server_fd")
    client_fd_first = resource_value(client_first, "client_fd")
    client_fd_last = resource_value(last, "client_fd")
    server_rss_first = resource_value(server_first, "server_rss_kib")
    server_rss_last = resource_value(last, "server_rss_kib")

    # 后半程 RSS 线性斜率（MiB/小时）。短测试只作为参考，不用于强判定。
    half = duration / 2.0
    tail = [
        (s["elapsed"], s["resources"]["server_rss_kib"] / 1024.0)
        for s in samples
        if s["elapsed"] >= half
        and isinstance(s.get("resources", {}).get("server_rss_kib"), int)
        and (
            last_server_pid is None
            or resource_value(s, "server_pid") == last_server_pid
        )
    ]
    rss_slope_mib_per_hour: Optional[float] = None
    if len(tail) >= 3:
        n = len(tail)
        mean_x = sum(x for x, _ in tail) / n
        mean_y = sum(y for _, y in tail) / n
        denom = sum((x - mean_x) ** 2 for x, _ in tail)
        if denom > 0:
            slope = sum((x - mean_x) * (y - mean_y) for x, y in tail) / denom
            rss_slope_mib_per_hour = slope * 3600.0

    tcp_p95 = [s["load"]["tcp_p95_ms"] for s in samples if s["load"]["tcp_ok"]]
    udp_p95 = [s["load"]["udp_p95_ms"] for s in samples if s["load"]["udp_ok"]]
    max_log = max(
        (
            max(
                s["resources"].get("server_log_bytes", 0),
                s["resources"].get("client_log_bytes", 0),
            )
            for s in samples
        ),
        default=0,
    )

    if not samples:
        fatal.append("no samples collected")
    if not tcp_total:
        fatal.append("no TCP load completed")
    if not udp_total:
        fatal.append("no UDP load completed")
    if sample_ratio < MIN_SAMPLE_RATIO:
        fatal.append(
            f"sample ratio {sample_ratio:.2f} below {MIN_SAMPLE_RATIO:.2f} "
            f"({len(samples)}/{expected_samples})"
        )
    if any(s.get("server", {}).get("alive") is False for s in samples):
        fatal.append("rfrps exited unexpectedly")
    if any(
        s.get("client", {}) is not None and s.get("client", {}).get("alive") is False
        for s in samples
    ):
        fatal.append("rfrpc exited unexpectedly")

    if STRICT:
        if tcp_rate < TCP_MIN:
            fatal.append(f"TCP success {tcp_rate:.4%} below {TCP_MIN:.4%}")
        if udp_rate < UDP_MIN:
            fatal.append(f"UDP success {udp_rate:.4%} below {UDP_MIN:.4%}")
        if len(silence_failures) > SILENCE_FAIL_MAX:
            fatal.append(
                f"silence checks failed {len(silence_failures)} times "
                f"(max {SILENCE_FAIL_MAX})"
            )
        if restart_failures:
            fatal.append(f"{len(restart_failures)} planned restarts failed")
        if freeze_failures:
            fatal.append(f"{len(freeze_failures)} client freeze injections failed")
        if (
            server_fd_first is not None
            and server_fd_last is not None
            and server_fd_last - server_fd_first > FD_MAX
        ):
            fatal.append(
                f"rfrps fd grew {server_fd_last - server_fd_first} (max {FD_MAX})"
            )
        if (
            server_rss_first is not None
            and server_rss_last is not None
            and (server_rss_last - server_rss_first) / 1024.0 > RSS_MAX_MIB
        ):
            fatal.append(
                f"rfrps RSS grew {(server_rss_last - server_rss_first) / 1024.0:.1f} MiB "
                f"(max {RSS_MAX_MIB:.1f})"
            )
        if max_log > LOG_MAX_MIB * 1024 * 1024:
            fatal.append(f"log size {max_log / 1024 / 1024:.1f} MiB exceeds {LOG_MAX_MIB:.1f} MiB")
    else:
        if tcp_rate < TCP_MIN:
            warnings.append(f"TCP success {tcp_rate:.4%} below strict threshold {TCP_MIN:.4%}")
        if udp_rate < UDP_MIN:
            warnings.append(f"UDP success {udp_rate:.4%} below strict threshold {UDP_MIN:.4%}")
        if silence_failures:
            warnings.append(f"{len(silence_failures)} silence checks did not fully drain")
        if restart_failures:
            warnings.append(f"{len(restart_failures)} planned restarts failed")
        if freeze_failures:
            warnings.append(f"{len(freeze_failures)} client freeze injections failed")
        if max_log > LOG_MAX_MIB * 1024 * 1024:
            warnings.append(f"log size {max_log / 1024 / 1024:.1f} MiB exceeds {LOG_MAX_MIB:.1f} MiB")

    summary = {
        "duration_secs": round(duration, 1),
        "samples": len(samples),
        "expected_samples": expected_samples,
        "sample_ratio": round(sample_ratio, 4),
        "strict": STRICT,
        "tcp": {
            "ok": tcp_ok,
            "fail": tcp_fail,
            "success_rate": round(tcp_rate, 6),
            "p95_ms_first": round(tcp_p95[0], 3) if tcp_p95 else None,
            "p95_ms_last": round(tcp_p95[-1], 3) if tcp_p95 else None,
            "p95_ms_max": round(max(tcp_p95), 3) if tcp_p95 else None,
        },
        "udp": {
            "ok": udp_ok,
            "fail": udp_fail,
            "success_rate": round(udp_rate, 6),
            "p95_ms_first": round(udp_p95[0], 3) if udp_p95 else None,
            "p95_ms_last": round(udp_p95[-1], 3) if udp_p95 else None,
            "p95_ms_max": round(max(udp_p95), 3) if udp_p95 else None,
        },
        "resources": {
            "server_fd_first": server_fd_first,
            "server_fd_last": server_fd_last,
            "client_fd_first": client_fd_first,
            "client_fd_last": client_fd_last,
            "server_rss_kib_first": server_rss_first,
            "server_rss_kib_last": server_rss_last,
            "server_rss_growth_mib": (
                round((server_rss_last - server_rss_first) / 1024.0, 2)
                if server_rss_first is not None and server_rss_last is not None
                else None
            ),
            "rss_slope_mib_per_hour_tail": (
                round(rss_slope_mib_per_hour, 3)
                if rss_slope_mib_per_hour is not None
                else None
            ),
            "max_log_bytes": max_log,
        },
        "events": {
            "restarts": len(restart_events),
            "restart_failures": len(restart_failures),
            "freezes": len(freeze_events),
            "freeze_failures": len(freeze_failures),
            "silence_checks": len(silence_events),
            "silence_failures": len(silence_failures),
        },
        "verdict": "PASS" if not fatal else "FAIL",
        "fatal": fatal,
        "warnings": warnings,
        "report": str(report_path),
        "logs_dir": str(temp),
    }
    return summary, fatal, warnings


def render_markdown(summary: dict) -> str:
    tcp = summary["tcp"]
    udp = summary["udp"]
    res = summary["resources"]
    events = summary["events"]
    lines = [
        "# Soak 稳定性测试摘要",
        "",
        f"- 时长：{summary['duration_secs']} 秒",
        f"- 采样：{summary['samples']} / 预期 {summary['expected_samples']}"
        f"（{summary['sample_ratio']:.0%}）",
        f"- 模式：{'严格判定' if summary['strict'] else '基线采集（不按阈值失败）'}",
        f"- 结论：**{summary['verdict']}**",
        "",
        "## 负载",
        "",
        f"- TCP：成功 {tcp['ok']}，失败 {tcp['fail']}，成功率 {tcp['success_rate']:.4%}，"
        f"p95 首/尾/最大 {tcp['p95_ms_first']}/{tcp['p95_ms_last']}/{tcp['p95_ms_max']} ms",
        f"- UDP：成功 {udp['ok']}，失败 {udp['fail']}，成功率 {udp['success_rate']:.4%}，"
        f"p95 首/尾/最大 {udp['p95_ms_first']}/{udp['p95_ms_last']}/{udp['p95_ms_max']} ms",
        "",
        "## 资源",
        "",
        f"- rfrps fd：{res['server_fd_first']} → {res['server_fd_last']}",
        f"- rfrpc fd：{res['client_fd_first']} → {res['client_fd_last']}",
        f"- rfrps RSS 增长：{res['server_rss_growth_mib']} MiB",
        f"- 后半程 RSS 斜率：{res['rss_slope_mib_per_hour_tail']} MiB/小时"
        "（短测试仅参考）",
        f"- 最大日志：{res['max_log_bytes'] / 1024 / 1024:.1f} MiB",
        "",
        "## 故障注入",
        "",
        f"- 计划重启：{events['restarts']} 次，失败 {events['restart_failures']} 次",
        f"- 客户端冻结：{events['freezes']} 次，失败 {events['freeze_failures']} 次",
        f"- 静默检查：{events['silence_checks']} 次，未完全回落 {events['silence_failures']} 次",
        "",
        "## 判定明细",
        "",
    ]
    if summary["fatal"]:
        lines += [f"- 失败：{item}" for item in summary["fatal"]]
    if summary["warnings"]:
        lines += [f"- 告警：{item}" for item in summary["warnings"]]
    if not summary["fatal"] and not summary["warnings"]:
        lines.append("- 无")
    lines += [
        "",
        f"- 报告：`{summary['report']}`",
        f"- 日志目录：`{summary['logs_dir']}`",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    base.BIN = base.find_binary()
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    report_path = REPORT_DIR / f"soak-{run_id}.jsonl"
    summary_json_path = REPORT_DIR / f"soak-{run_id}-summary.json"
    summary_md_path = REPORT_DIR / f"soak-{run_id}-summary.md"
    temp = Path(tempfile.mkdtemp(prefix="rfrp-soak-"))
    ctx = SoakContext(temp)

    tcp_echo = base.TcpEchoServer()
    udp_echo = base.UdpEchoServer()
    stack = None
    stop = threading.Event()
    pause = threading.Event()
    tcp_stats = Stats()
    udp_stats = Stats()
    samples: list[dict] = []
    events: list[dict] = []
    workers: list[threading.Thread] = []
    sampler_thread: Optional[threading.Thread] = None
    sampler_stop = threading.Event()
    sampler_started = 0.0
    sampler_stopped = 0.0
    report_lock = threading.Lock()
    handle = open(report_path, "w", encoding="utf-8", buffering=1)
    started = time.monotonic()
    fatal = []

    def record_event(event: str, **fields) -> None:
        item = {
            "type": "event",
            "ts": now_iso(),
            "elapsed": round(time.monotonic() - started, 3),
            "event": event,
            **fields,
        }
        events.append(item)
        write_jsonl(handle, item, report_lock)

    def take_load() -> dict:
        tcp_ok, tcp_fail, tcp_lat = tcp_stats.take()
        udp_ok, udp_fail, udp_lat = udp_stats.take()
        return {
            "tcp_ok": tcp_ok,
            "tcp_fail": tcp_fail,
            "tcp_p95_ms": round(percentile(tcp_lat, 0.95) * 1000, 3),
            "udp_ok": udp_ok,
            "udp_fail": udp_fail,
            "udp_p95_ms": round(percentile(udp_lat, 0.95) * 1000, 3),
        }

    def sample() -> None:
        load = take_load()
        # 计划重启/冻结窗口内 process 可能正处于新旧进程交替，alive 记为 None，
        # 避免把计划内重启误判为“进程崩溃”。
        planned = pause.is_set()
        server_alive = None if planned else stack.server_proc.poll() is None
        client_alive = None if planned else stack.client_proc.poll() is None
        record = {
            "type": "sample",
            "ts": now_iso(),
            "elapsed": round(time.monotonic() - started, 3),
            "planned_window": planned,
            "load": load,
            "server": {},
            "client": None,
            "resources": {},
        }
        try:
            status = stack.status()
            record["server"] = {
                "alive": server_alive,
                "sessions": len(status.get("sessions", [])),
                "pending_work": status.get("pending_work"),
                "udp_sessions": status.get("udp_sessions"),
                "active_sessions": status.get("active_sessions"),
                "total_sessions": status.get("total_sessions"),
                "proxies": status.get("proxies"),
                "pooled_work_conns": status.get("pooled_work_conns"),
                "rtt_ms": status.get("rtt_ms"),
                "metrics": status.get("metrics", {}),
                "proxy_stats": status.get("proxy_stats", []),
            }
        except Exception as exc:
            record["server"] = {
                "alive": server_alive,
                "error": str(exc),
            }
        try:
            if stack.status_port is not None:
                code, body = base.http_get(stack.status_port, "/api/status", auth=False)
                client = json.loads(body) if code == 200 else {"error": f"http {code}"}
            else:
                client = {}
            client["alive"] = client_alive
            record["client"] = client
        except Exception as exc:
            record["client"] = {
                "alive": client_alive,
                "error": str(exc),
            }
        record["resources"] = {
            "server_pid": stack.server_proc.pid,
            "client_pid": stack.client_proc.pid,
            "server_fd": base.process_fd_count(stack.server_proc.pid),
            "client_fd": base.process_fd_count(stack.client_proc.pid),
            "server_rss_kib": base.process_rss_kib(stack.server_proc.pid),
            "client_rss_kib": base.process_rss_kib(stack.client_proc.pid),
            "server_log_bytes": log_size(temp / "rfrps.log"),
            "client_log_bytes": log_size(temp / "rfrpc.log"),
        }
        samples.append(record)
        write_jsonl(handle, record, report_lock)

    def sampler() -> None:
        next_at = time.monotonic()
        while not sampler_stop.is_set():
            now = time.monotonic()
            if now < next_at:
                sampler_stop.wait(next_at - now)
                continue
            sample()
            next_at += SAMPLE_SECS
            if next_at <= time.monotonic():
                next_at = time.monotonic() + SAMPLE_SECS

    def pulse() -> None:
        if pause.is_set():
            return
        started_pulse = time.monotonic()
        ok = base.tcp_bulk_roundtrip(stack.remote_tcp, b"p" * (1 << 20), timeout=30)
        record_event(
            "pulse_tcp",
            ok=ok,
            elapsed=round(time.monotonic() - started_pulse, 3),
        )
        churn_ok = 0
        for _ in range(10):
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                sock.connect(("127.0.0.1", stack.remote_udp))
                if base.udp_roundtrip(sock, b"churn", timeout=3.0):
                    churn_ok += 1
            finally:
                sock.close()
        record_event("churn_udp", ok=churn_ok, total=10)

    def planned_restart(target: str) -> None:
        pause.set()
        time.sleep(2.0)
        item = {"event": f"restart_{target}", "ok": False}
        try:
            if target == "server":
                stack.restart_server()
            else:
                stack.restart_client()
            item["ok"] = True
        except Exception as exc:
            item["error"] = str(exc)
        finally:
            pause.clear()
        record_event(item.pop("event"), **item)

    def planned_freeze() -> None:
        pause.set()
        time.sleep(2.0)
        item = {"event": "freeze_client", "ok": False}
        try:
            stack.pause_client()
            time.sleep(FREEZE_HOLD_SECS)
            stack.resume_client()
            item["ok"] = stack._wait_proxies_ready(20.0)
            if not item["ok"]:
                item["error"] = "proxies did not recover after client freeze"
        except Exception as exc:
            item["error"] = str(exc)
        finally:
            pause.clear()
        record_event(item.pop("event"), **item)

    def silence_check() -> None:
        if pause.is_set():
            return
        pause.set()
        time.sleep(SILENCE_HOLD_SECS)
        item = {"event": "silence", "ok": False}
        try:
            status = stack.status()
            metrics = status.get("metrics", {})
            item.update(
                {
                    "active_connections": metrics.get("active_connections"),
                    "udp_sessions": status.get("udp_sessions"),
                    "pending_work": status.get("pending_work"),
                }
            )
            item["ok"] = (
                metrics.get("active_connections") == 0
                and status.get("udp_sessions") == 0
                and status.get("pending_work") == 0
            )
        except Exception as exc:
            item["error"] = str(exc)
        finally:
            pause.clear()
        record_event(item.pop("event"), **item)

    try:
        stack = base.Stack(
            ctx,
            tcp_local=tcp_echo.port,
            udp_local=udp_echo.port,
            udp_timeout=5,
            log_level=LOG_LEVEL,
            status=True,
        )
        sampler_thread = threading.Thread(target=sampler, daemon=True)
        sampler_started = time.monotonic()
        sampler_thread.start()
        workers = build_workers(stack, stop, pause, tcp_stats, udp_stats)
        for worker in workers:
            worker.start()

        next_pulse = time.monotonic() + PULSE_SECS if PULSE_SECS else float("inf")
        next_restart = time.monotonic() + RESTART_SECS if RESTART_SECS else float("inf")
        next_freeze = time.monotonic() + FREEZE_SECS if FREEZE_SECS else float("inf")
        next_silence = time.monotonic() + SILENCE_SECS if SILENCE_SECS else float("inf")
        restart_target = "server"

        while True:
            now = time.monotonic()
            if now - started >= DURATION:
                break
            if now >= next_pulse:
                pulse()
                next_pulse = time.monotonic() + PULSE_SECS
            if now >= next_restart:
                planned_restart(restart_target)
                restart_target = "client" if restart_target == "server" else "server"
                next_restart = time.monotonic() + RESTART_SECS
            if now >= next_freeze:
                planned_freeze()
                next_freeze = time.monotonic() + FREEZE_SECS
            if now >= next_silence:
                silence_check()
                next_silence = time.monotonic() + SILENCE_SECS
            time.sleep(0.2)

        stop.set()
        for worker in workers:
            worker.join(timeout=10)
        sampler_stop.set()
        if sampler_thread is not None:
            sampler_thread.join(timeout=15)
        sampler_stopped = time.monotonic()
        # 结束前做一次静默回落检查，再取最终样本。
        silence_check()
        sample()
    except Exception as exc:
        fatal.append(f"fatal: {exc}")
        record_event("fatal", error=str(exc))
    finally:
        stop.set()
        sampler_stop.set()
        if sampler_thread is not None:
            sampler_thread.join(timeout=15)
        if sampler_stopped == 0.0:
            sampler_stopped = time.monotonic()
        pause.clear()
        for worker in workers:
            worker.join(timeout=10)
        handle.close()
        if stack is not None:
            stack.stop()
        tcp_echo.close()
        udp_echo.close()

    elapsed = time.monotonic() - started
    sample_duration = (
        sampler_stopped - sampler_started
        if sampler_started > 0 and sampler_stopped > sampler_started
        else None
    )
    summary, summary_fatal, warnings = summarize(
        samples, events, elapsed, report_path, temp, sample_duration
    )
    fatal.extend(summary_fatal)
    if fatal:
        summary["verdict"] = "FAIL"
        summary["fatal"] = fatal
    else:
        summary["verdict"] = "PASS"
    summary_json_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    summary_md_path.write_text(render_markdown(summary), encoding="utf-8")

    print(render_markdown(summary))
    print(f"JSONL: {report_path}")
    print(f"SUMMARY: {summary_json_path}")
    print(f"LOGS: {temp}")
    if KEEP_LOGS:
        print("临时目录已保留（E2E_SOAK_KEEP_LOGS=0 可在成功时清理）")
    elif not fatal:
        import shutil

        shutil.rmtree(temp, ignore_errors=True)
    return 0 if not fatal else 1


if __name__ == "__main__":
    sys.exit(main())
