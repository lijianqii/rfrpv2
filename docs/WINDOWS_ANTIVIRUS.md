# Windows 杀毒软件误报说明

## 现象

`rfrp.exe`（或 rfrp 相关二进制）在 Windows 上可能被 Windows Defender 或第三方杀毒软件报告为病毒 / 风险软件（常见的报法：`HackTool`、`RiskWare`、`PUA` 等）。

## 原因

这不是 rfrp 特有的问题，而是**内网穿透 / 反向代理类工具的普遍现象**。同类工具（frp、nps、ngrok、chisel 等）同样会被安全软件标记。主要原因：

1. **功能特征**：这类工具具备"远程访问 / 隧道转发 / 端口映射"能力，安全软件的静态特征库会将其归类为
   `HackTool` / `RiskWare`（风险工具），与"是否真的恶意"无关。
2. **无数字签名**：本项目没有 Authenticode 代码签名证书，未签名程序在 SmartScreen / Defender 中
   天然不受信任。
3. **元数据缺失**：历史上发布的二进制不带 PE 版本信息（公司名 / 产品名 / 描述 / 版权），
   启发式引擎更容易把"来路不明"的程序判为可疑。

## 已做的缓解（仓库内）

- **PE 版本元数据**：`crates/rfrp-bin/resources/rfrp.rc` 嵌入 `VERSIONINFO`（FileDescription、
  CompanyName、ProductName、LegalCopyright、FileVersion、OriginalFilename），构建出的 exe
  在资源管理器中显示完整"详细信息"。
- **应用清单**：`rfrp.manifest.xml` 声明 `asInvoker`（不请求管理员权限，避免 UAC 弹窗）、
  Windows 10/11 兼容性与 DPI 感知。
- **程序图标**：`rfrp.ico`（16/32/48 三尺寸），生成脚本 `scripts/gen-windows-icon.py`。
- **构建配置**：release 已启用 `strip`、`lto = "fat"`、`panic = "abort"`。

以上元数据能显著降低"无签名 / 无元数据"类启发式误报，但**无法消除**基于功能特征的
`HackTool` 归类——只要二进制具备隧道/反代能力，杀软就可能按特征库匹配。

> 注意：本项目**不做加壳 / 混淆 / 免杀处理**。加壳会大幅增加误报概率，且违背开源项目
> 透明可审计的原则。任何声称"免杀版"的第三方分发都不可信。

## 用户侧处理建议

1. **加入信任区**：Windows 安全中心 → 病毒和威胁防护 → "病毒和威胁防护设置" → 排除项，
   将 rfrp 目录或 `rfrp.exe` 加入排除。
2. **提交误报**：若确认是从官方渠道获取的版本，可在
   [Microsoft 安全智能提交](https://www.microsoft.com/wdsi/filesubmission) 提交文件申请复查，
   撤销误报通常需要数日。
3. **官方发布做代码签名**：正式发布时对 exe 执行 Authenticode 签名（需购买代码签名证书，
   可用 `/fd sha256 /tr http://timestamp.digicert.com /td sha256` 加时间戳）：
   ```powershell
   signtool sign /fd sha256 /tr http://timestamp.digicert.com /td sha256 /f cert.pfx /p <password> rfrp.exe
   ```
   签名后 Defender / SmartScreen 不再显示"未知发布者"。
4. **校验来源**：仅从官方 GitHub Release 下载，核对仓库发布的 SHA-256 校验值，
   防止第三方篡改后带毒。

## 构建与验证

交叉编译（Linux 宿主 → Windows）：

```bash
rustup target add x86_64-pc-windows-gnu   # 或 -msvc
cargo build --release --target x86_64-pc-windows-gnu -p rfrp-bin
```

验证资源是否嵌入：

```powershell
# 资源管理器查看 exe 属性 → 详细信息（应有版本/公司/产品等字段）
# 或命令行：
sigcheck.exe rfrp.exe   # Sysinternals
```

重新生成图标（修改 `scripts/gen-windows-icon.py` 后）：

```bash
python3 scripts/gen-windows-icon.py
```