# 性能基准（Criterion）

运行方式：

```bash
cargo bench -p rfrp-common --bench forward
```

## 基线（2026-09-10，本地 Debian x86_64）

| Benchmark | 耗时 / 吞吐（中位数） | 说明 |
|---|---|---|
| frame_encode_decode_256b | ~178 ns | 256 字节帧编解码一次 |
| bridge_1mib/buf_8k | ~318 MiB/s | 桥接 1 MiB（tokio 默认 8 KiB 缓冲） |
| bridge_1mib/buf_32k | ~357 MiB/s | 桥接 1 MiB（`BRIDGE_BUF_SIZE` = 32 KiB） |
| bridge_1mib/buf_64k | ~359 MiB/s | 桥接 1 MiB（64 KiB，收益饱和） |
| config_parse_server | ~7.1 µs | 服务端配置解析 + 校验 |
| config_parse_client | ~12.4 µs | 客户端配置解析 + 校验 |

> 数值会随机器与负载波动；CI/发布前应重新记录。

## 数据面调优结论

- **桥接缓冲 32 KiB**：`bridge()` 由 tokio 默认 8 KiB 提升到 32 KiB，loopback 实测吞吐
  **+12%**（318 → 357 MiB/s）；64 KiB 相比 32 KiB 仅 +0.7%（收益饱和），故取 32 KiB。
  代价是每条桥接连接约 2×32 KiB 缓冲内存（8 KiB 时为 2×8 KiB）。
- **流量计数批量化**：`CountingStream` 改为本地累计，每 256 KiB、或距上次刷新超过 1s、
  或连接关闭时写入全局原子计数器，避免高并发大流量下多核争用同一 cache line；
  监控滞后 ≤ 1s / 256 KiB（低速长连接也能及时反映）。
- **UDP 分帧缓冲复用**：工作连接下行帧改用 `read_udp_frame_into` 复用缓冲，
  高频 UDP 转发下每包减少一次堆分配。
