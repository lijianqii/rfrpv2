#!/usr/bin/env bash
# Debian 开发机工具链一键安装（DESIGN §3.3.2）。
# 支持 Debian 11/12/13 及 Ubuntu 20.04+，宿主架构支持 amd64 与 arm64：
# 两种宿主都要能产出 x86_64 的 Linux 与 Windows 产物（arm64 宿主需要交叉工具链）。
set -euo pipefail

HOST_ARCH="$(dpkg --print-architecture 2>/dev/null || uname -m)"
echo "==> 宿主架构: ${HOST_ARCH}"

echo "==> 安装系统基础包"
sudo apt update
sudo apt install -y build-essential pkg-config curl git

if [ "$HOST_ARCH" != "amd64" ]; then
  echo "==> 安装 x86_64 交叉工具链（arm64 宿主构建 x86_64 Linux 产物所需）"
  # 提供 /usr/bin/x86_64-linux-gnu-gcc，对应 .cargo/config.toml 里
  # [target.x86_64-unknown-linux-gnu] linker 的取值。
  sudo apt install -y crossbuild-essential-amd64
fi

echo "==> 安装 MinGW-w64 交叉编译器（Windows 产物）"
sudo apt install -y mingw-w64

echo "==> 验证工具链"
require() {
  local bin="$1" hint="${2:-}"
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "  ✗ 缺少 $bin${hint:+（$hint）}" >&2
    exit 1
  fi
  echo "  ✓ $bin"
}
require gcc "build-essential"                                    # 宿主原生编译
require x86_64-linux-gnu-gcc "amd64: build-essential；arm64: crossbuild-essential-amd64"
require x86_64-w64-mingw32-gcc "mingw-w64"                       # Windows 产物

echo "==> 添加 Rust target"
rustup target add x86_64-unknown-linux-gnu || true
rustup target add x86_64-pc-windows-gnu || true

echo "==> 验证构建（两条命令均应在 Debian 上成功）"
cargo build --release --target x86_64-unknown-linux-gnu
cargo build --release --target x86_64-pc-windows-gnu

echo "==> 完成。产物位于 target/<triple>/release/rfrp[.exe]"
