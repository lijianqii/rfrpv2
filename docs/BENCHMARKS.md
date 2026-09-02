# 性能基准（Criterion）

运行方式：

```bash
cargo bench -p rfrp-common --bench forward
```

## 基线（2026-09-01，本地 Debian x86_64，debug/release profile）

| Benchmark | 耗时（中位数） | 说明 |
|---|---|---|
| frame_encode_decode_256b | ~312 ns | 256 字节帧编解码一次 |
| bridge_duplex_64k | ~64.6 µs | 64KiB 双向透传一轮 |
| config_parse_server | ~10.6 µs | 服务端配置解析 + 校验 |
| config_parse_client | ~18.7 µs | 客户端配置解析 + 校验 |

> 数值会随机器与负载波动；CI/发布前应重新记录。
