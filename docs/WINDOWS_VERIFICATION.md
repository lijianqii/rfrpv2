# Windows 实机验证清单

Linux 上的 `x86_64-pc-windows-gnu` 交叉编译只能证明代码可编译，不能验证以下运行时行为。
每次发布前应在真实 Windows（建议 Windows 10/11 + Server 2022 各一台）执行本清单。

## 1. 产物与路径

- [ ] `cargo build --locked --release --target x86_64-pc-windows-gnu -p rfrp-bin`
- [ ] 复制 `target/x86_64-pc-windows-gnu/release/rfrp.exe` 到独立目录。
- [ ] 配置文件放在含空格/非 ASCII 字符的目录，启动不报路径错误。
- [ ] `rfrp.exe server -c <config> --check` 与 `client --check` 返回 0。
- [ ] 故意写错 `allow_ports`/端口，`--check` 返回非零且错误可读。

## 2. 基础功能

- [ ] server/client 前台启动，TCP 代理可往返传输。
- [ ] UDP 代理小包/突发可工作，`/metrics` 的 UDP 计数增长。
- [ ] HTTP/HTTPS vhost 路由正确；重复 Host/超长头被拒绝。
- [ ] Dashboard 登录、Cookie、`/healthz`、`/metrics` 正常。
- [ ] 控制链路 TLS、工作连接 TLS 均能建立；错误 CA/SNI 快速失败。

## 3. 配置监听（重点）

- [ ] `--watch-config=restart` 修改合法配置后子进程替换、代理恢复。
- [ ] 新配置端口被占用时回滚旧 stage，旧代理恢复。
- [ ] 新配置启动超时（可用慢 DNS/慢盘模拟）时回滚，不误删旧 stage。
- [ ] 配置为符号链接时，修改目标文件能触发重载。
- [ ] 配置目录写入权限不足时，错误日志明确指出 stage 写入失败。
- [ ] `--watch-config=exit` 修改合法配置后退出码为 75。
- [ ] 服务包装器（NSSM/WinSW）按退出码 75 重新拉起。

## 4. 退出与进程清理

- [ ] 控制台 Ctrl-C 优雅退出，退出码 0。
- [ ] Ctrl-Break（`GenerateConsoleCtrlEvent`）优雅退出。
- [ ] supervisor 被任务管理器结束（TerminateProcess）后，子进程 stdin EOF 并退出。
- [ ] 正常退出后 stage/ready 文件被删除；异常退出残留文件权限不过宽。
- [ ] 停止时在途 TCP 连接按 `grace_secs` 保留，超过 grace 后被关闭。

## 5. 持久化与权限

- [ ] `run_id_file` 指向自定义可写路径，重启两次后内容不变。
- [ ] 默认 `%USERPROFILE%\.rfrp\run_id` 在服务账户下可写；不可写时有明确告警。
- [ ] 检查 run_id/stage/ready 文件 ACL，非服务账户不可读写。
- [ ] 复制配置/证书后重启，相对证书路径解析正确。

## 6. 网络与安全

- [ ] Windows 防火墙首次启动弹窗/规则符合预期。
- [ ] 杀毒软件未误杀 `rfrp.exe`（参考 `docs/WINDOWS_ANTIVIRUS.md`）。
- [ ] 监听 `0.0.0.0`/具体网卡地址、IPv4/IPv6（`[::1]`）均符合预期。
- [ ] Dashboard 非回环绑定时有告警，认证失败限速生效（连续错误后 429）。

## 7. 记录

将每项结果、Windows 版本、rfrp 版本、配置摘要（不含 token/密码）附在发布记录中。
未执行的项必须显式标记“未验证”，不得以 Linux E2E 或交叉编译替代。
