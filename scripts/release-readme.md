# rfrp {{VERSION}} — {{PACKAGE}}

Rust 反向代理（内网穿透）工具。同一二进制既是服务端也是客户端：
`rfrp server` / `rfrp client`。

## 本包内容

| 文件 | 说明 |
|---|---|
| `rfrp`（Windows 为 `rfrp.exe`） | 主程序 |
| `rfrp-server.toml` / `rfrp-client.toml` | 配置模板（**请修改 token、地址、证书路径**） |
| `cert.pem` / `key.pem` / `ca.pem` | 发布时生成的自签演示证书（仅供本地试用，**生产必须替换**） |
| `vhost-cert.pem` / `vhost-key.pem` | HTTPS vhost 演示证书（同上） |
| `rfrp-server.service` / `rfrp-client.service` | systemd unit（仅 Linux 包） |
| `LICENSE` / `README.md` | 许可证与本说明 |

> 包内证书是发布流水线生成的自签证书，私钥随包分发，**只能用于本地/内网试用**。
> 生产环境请用 `scripts/gen-self-signed-cert.sh` 重新生成并妥善保管私钥，
> 或替换为正式 CA 签发的证书。

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
- 解包后可先执行 `./rfrp server -c rfrp-server.toml --check` 与
  `./rfrp client -c rfrp-client.toml --check`，只校验配置、不监听端口/不建连。
- 启用 TLS 需要证书与私钥；包内已附带演示证书，生产环境务必替换
  （可用仓库 `scripts/gen-self-signed-cert.sh` 生成自签证书）。
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
> unit 已设置 `LimitNOFILE`，并通过 `StateDirectory`/`LogsDirectory` 自动创建
> `/var/lib/rfrp`、`/var/log/rfrp`。需要绑定 <1024 端口时，请使用 unit 中注释的
> `AmbientCapabilities=CAP_NET_BIND_SERVICE`（`NoNewPrivileges` 下 setcap 不会生效）。
> 使用 `file:` 日志输出时，可将 `logrotate/rfrp` 安装到 `/etc/logrotate.d/rfrp`；
> 它使用 `copytruncate`，轮转瞬间可能丢失极少量日志行。

## 校验

```bash
sha256sum -c SHA256SUMS
```

## 文档

- 项目 README：配置示例、Dashboard/指标、排障
- `DESIGN.md`：协议与设计说明
- `CHANGELOG.md`：变更记录
