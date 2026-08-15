#!/usr/bin/env bash
# Debian 开发机工具链一键安装（DESIGN §3.3.2）。
# 支持 Debian 11/12/13 及 Ubuntu 20.04+。所有交叉编译产物均在 Linux 开发机完成。
set -euo pipefail

echo "==> 安装系统基础包"
sudo apt update
sudo apt install -y build-essential pkg-config curl git

echo "==> 安装 musl 静态编译工具"
sudo apt install -y musl-tools

echo "==> 安装 MinGW-w64 交叉编译器（Windows 产物）"
sudo apt install -y mingw-w64

echo "==> 验证工具链"
command -v gcc         && echo "  gcc OK"
command -v musl-gcc    && echo "  musl-gcc OK"
command -v x86_64-w64-mingw32-gcc && echo "  mingw32-gcc OK"

echo "==> 添加 Rust target"
rustup target add x86_64-unknown-linux-gnu || true
rustup target add x86_64-unknown-linux-musl || true
rustup target add x86_64-pc-windows-gnu || true

echo "==> 验证构建（三条命令均应在 Debian 上成功）"
cargo build --release --target x86_64-unknown-linux-gnu
cargo build --release --target x86_64-unknown-linux-musl
cargo build --release --target x86_64-pc-windows-gnu

echo "==> 完成。产物位于 target/<triple>/release/rfrp[.exe]"
