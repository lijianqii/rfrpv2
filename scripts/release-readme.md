# rfrp {{VERSION}} — {{PACKAGE}}

Rust 反向代理（内网穿透）工具。同一二进制既是服务端也是客户端：
`rfrp server` / `rfrp client`。

## 本包内容

| 文件 | 说明 |
|---|---|
| `rfrp`（Windows 为 `rfrp.exe`） | 主程序 |
| `rfrp-server.toml` / `rfrp-client.toml` | 配置模板（**请修改 token、地址、证书路径**） |
| `rfrp-server.service` / `rfrp-client.service` | systemd unit（仅 Linux 包） |
| `LICENSE` / `README.md` | 许可证与本说明 |

## 快速开始

Linux：

```bash
./rfrp server -c rfrp-server.toml
./rfrp client -c rfrp-client.toml
```

Windows：

```cmd
rfrp.exe server -c rfrp-server.toml
rfrp.exe client -c rfrp-client.toml
```

提示：

- `rfrp --help` / `rfrp server --help` 查看全部参数（CLI 参数可覆盖配置文件）。
- 启用 TLS 需要证书与私钥；自签场景可用仓库 `scripts/gen-self-signed-cert.sh` 生成。
- 客户端 `status_addr` 可开启只读状态端点（`/`、`/api/status`、`/metrics`），
  服务端 `[dashboard]` 提供 Dashboard 与 Prometheus 指标。
- Windows 若被杀毒软件拦截，见仓库 `docs/WINDOWS_ANTIVIRUS.md`。

## systemd 安装（Linux）

```bash
sudo useradd -r -s /usr/sbin/nologin rfrp
sudo mkdir -p /etc/rfrp /var/lib/rfrp /var/log/rfrp
sudo chown rfrp:rfrp /var/lib/rfrp /var/log/rfrp

sudo cp rfrp /usr/local/bin/rfrp
sudo cp rfrp-server.toml rfrp-client.toml /etc/rfrp/
sudo cp rfrp-server.service rfrp-client.service /etc/systemd/system/

sudo systemctl daemon-reload
sudo systemctl enable --now rfrp-server   # 或 rfrp-client
```

> 建议在配置中显式设置 `run_id_file = "/var/lib/rfrp/run_id"`，并保持服务用户可写。

## 校验

```bash
sha256sum -c SHA256SUMS
```

## 文档

- 项目 README：配置示例、Dashboard/指标、排障
- `DESIGN.md`：协议与设计说明
- `CHANGELOG.md`：变更记录
