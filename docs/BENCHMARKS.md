# 性能基准（Criterion）

运行方式：

```bash
cargo bench -p rfrp-common --bench forward
```

> **基准本身也"停摆"过**：协议升到 v2 后，bench 里的帧版本仍写死 `1`，`FrameCodec`
> 直接拒绝（`unsupported protocol version`）；bridge 基准每轮新建两对 TCP，跑满一万轮后
> 本地端口被 TIME_WAIT 耗尽（`AddrNotAvailable`）。两者已修复（版本改用 `PROTOCOL_VERSION`；
> bridge 改为在**一条常驻连接**上测稳态吞吐），`cargo bench` 现在可以完整跑通。

## 当前基线（2026-09-19，macOS / Apple Silicon）

| Benchmark | 结果（中位数） | 说明 |
|---|---|---|
| frame_encode_decode_256b | ~86.6 ns | 256 字节帧编解码一次 |
| bridge_1mib/buf_8k | 3.43 GiB/s | 每方向 8 KiB 缓冲（tokio 默认） |
| bridge_1mib/buf_32k | 6.03 GiB/s | `BRIDGE_BUF_SIZE`（当前默认） |
| bridge_1mib/buf_64k | 6.45 GiB/s | 相对 32 KiB 仅 +7%，但缓冲内存翻倍 |
| config_parse_server | ~2.36 µs | 服务端配置解析 + 校验 |
| config_parse_client | ~4.23 µs | 客户端配置解析 + 校验 |

## 端到端（loopback，release 二进制；256 MiB 流式 + 64B 往返 ping-pong）

| 场景 | 单向吞吐 | 64B 往返 RTT | 每连接（建连 + 首字节） |
|---|---|---|---|
| 直连 echo（基线） | 5.3 GiB/s | 15.7 µs | 126 µs |
| rfrp 明文，`pool_size=1` | 1.24 GiB/s（24%） | 52 µs（3.3×） | 194 µs |
| rfrp TLS，`pool_size=1` | 1.08 GiB/s（21%） | 53 µs（3.4×） | 314 µs |
| rfrp 明文，`pool_size=4` | — | — | 193 µs |
| rfrp TLS，`pool_size=4` | — | — | 215 µs |

> 对端是 Python 单线程 echo，绝对数值受生成器限制；重点看**相对关系**与配置差异。

**结论与调优建议：**

- **TLS + 短连接密集场景把 `pool_size` 提到 4 左右**：每连接成本从 ~314 µs 降到 ~215 µs（−32%）。
  原因是 `pool_size=1` 时"命中 → 请求补充"的补充连接常常赶不上下一个用户连接，于是退化为
  按需建连（含完整 TLS 握手）；池子稍大便能吸收这种突发。
- 明文场景 `pool_size=1` 已经够（194 µs ≈ pool=4 的 193 µs）。
- `BRIDGE_BUF_SIZE = 32 KiB` 是合理取舍：相比 8 KiB 吞吐 +76%，相比 64 KiB 只差 7% 却省一半缓冲内存。
- 端到端吞吐约为直连的 1/4：路径上有两次桥接（rfrps 一次、rfrpc 一次），每跳都是用户态
  拷贝 + 读写系统调用。这是当前最大的优化空间（Linux 可考虑 `splice` 零拷贝，需评估可移植性）。

## 复现方式（UDP 通路）

```bash
cargo build --release
scripts/bench-udp.py burst     # 单会话突发：端到端回收率 + 服务端 rfrp_udp_dropped_total
scripts/bench-udp.py latency   # 200B 小包往返：直连 vs 代理（P50/P95/P99）
```

> 突发回收率对机器负载很敏感（同一台机器上实测 60%~97% 波动），因此脚本只打印数字、
> 不做断言；`crates/rfrpc/tests/udp_burst.rs` 里另有一道 release 下的粗粒度闸门。

## 分配与分配器（可选 feature = mimalloc）

判断"换分配器有没有用"，先量**分配发生了多少次**（`cargo test -p rfrpc --test alloc_count -- --nocapture`，
用自定义 `GlobalAlloc` 统计，server+client 同进程）：

| 场景 | 分配次数 | 分配字节 |
|---|---|---|
| 短连接 churn（每条连接：用户连接 + 工作连接 + 本地连接） | **43.2 次/连接** | **159.9 KiB/连接** |
| 大流量传输（单连接 128 MiB） | **0.18 次/MiB** | 8.6 KiB/MiB |

结论：

- **大流量与分配器无关**：传输路径每 MiB 只有 0.18 次分配（连接建立时的一次性分配），
  换 mimalloc/jemalloc 不会改变吞吐。
- **churn 才可能受益**：每连接 43 次分配、160 KiB，其中大头是 tokio
  `copy_bidirectional_with_sizes` 每次桥接都 `vec![0; 32 KiB]`——一条用户连接两次桥接
  （服务端 + 客户端）= **128 KiB 清零**，这是分配/内存带宽成本的主体。
  按 macOS 实测的候选分配器 A/B：**系统分配器 1100 conn/s vs mimalloc 1090 conn/s（噪声内）**。
  同一次实验的后续轮次因**测试端 TIME_WAIT 端口耗尽**而失真（两种二进制同样塌到几十 conn/s），
  因此这台机器上的 A/B 只能作为参考，结论不足以支持默认启用。

**建议**：不要默认启用。若要继续压 churn 成本，"复用桥接缓冲"这条已经实验过，见下一节。

如要在自己的 Debian 上复测：

```bash
cargo build --release -p rfrp-bin                        # 系统分配器
cargo build --release -p rfrp-bin --features mimalloc    # mimalloc（可选 feature，默认关闭）
```

## 桥接缓冲复用（已实验，**未采纳**）

动机：tokio 的 `copy_bidirectional` 每次调用都 `vec![0; 32 KiB]`（零初始化），而一条用户
连接要过两次桥接（服务端 + 客户端），等于**每连接多写 128 KiB 的零**。

实测（macOS，release）：

| 项 | 结果 |
|---|---|
| `vec![0; 32 KiB]` 分配 + 清零 | 0.534 µs |
| `Vec::with_capacity(32 KiB)`（不初始化） | 0.012 µs |
| 分配 + 真正写入 32 KiB（零初始化 / capacity） | 0.279 / 0.284 µs（**几乎无差别**） |
| 每连接成本基准（in-memory duplex，单桥接） | 新实现 22.50 µs vs tokio 22.90 µs |
| **端到端全双工单流吞吐**（交错 3 轮） | tokio 1755 / 1719 / 1790 MiB/s vs 新实现 1576 / 1563 / 1541 MiB/s |

结论：缓冲真正被数据填满时，零初始化的开销会被吸收（0.279 vs 0.284 µs），可省下的只有
"分配了但没填满"的部分，约 0.4 µs/桥接、~2 µs/连接；而为了让两个方向并发读写同一对
socket，必须先用 `tokio::io::split`，**它让同一 socket 的读半端与写半端共享一把锁**——
真实用户连接是全双工（RDP/SSH/echo），实测吞吐 **−11%**。收益与代价不成比例，因此已回退，
生产路径继续使用 tokio 的实现；只有手写 poll 状态机（需要 `unsafe` 操作未初始化缓冲，
与 `#![forbid(unsafe_code)]` 冲突）才能同时拿到两者，当前不做。

保留的两个语义测试（`bridge_propagates_half_close_and_keeps_reverse_direction`、
`bridge_reuses_buffer_for_many_small_bursts`）对任何实现都适用，用于守住半关闭与交互小包行为。

## 历史基线（2026-09-10，Debian x86_64，方法论不同）

| Benchmark | 耗时 / 吞吐（中位数） | 说明 |
|---|---|---|
| frame_encode_decode_256b | ~178 ns | 256 字节帧编解码一次 |
| bridge_1mib/buf_8k | ~318 MiB/s | 桥接 1 MiB（tokio 默认 8 KiB 缓冲） |
| bridge_1mib/buf_32k | ~357 MiB/s | 桥接 1 MiB（`BRIDGE_BUF_SIZE` = 32 KiB） |
| bridge_1mib/buf_64k | ~359 MiB/s | 桥接 1 MiB（64 KiB，收益饱和） |
| config_parse_server | ~7.1 µs | 服务端配置解析 + 校验 |
| config_parse_client | ~12.4 µs | 客户端配置解析 + 校验 |

> 这组数字含**每轮新建连接**的开销（旧 bridge 基准实现），且机器不同，**不可与新表直接对比**；
> 保留在此仅作历史参考。数值会随机器与负载波动，发布前应重新记录。

## 数据面调优结论

- **桥接缓冲 32 KiB**：`bridge()` 由 tokio 默认 8 KiB 提升到 32 KiB，loopback 实测吞吐
  **+12%**（318 → 357 MiB/s）；64 KiB 相比 32 KiB 仅 +0.7%（收益饱和），故取 32 KiB。
  代价是每条桥接连接约 2×32 KiB 缓冲内存（8 KiB 时为 2×8 KiB）。
- **流量计数批量化**：`CountingStream` 改为本地累计，每 256 KiB、或距上次刷新超过 1s、
  或连接关闭时写入全局原子计数器，避免高并发大流量下多核争用同一 cache line；
  监控滞后 ≤ 1s / 256 KiB（低速长连接也能及时反映）。
- **UDP 分帧缓冲复用**：工作连接下行帧改用 `read_udp_frame_into` 复用缓冲，
  高频 UDP 转发下每包减少一次堆分配。
