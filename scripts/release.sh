#!/usr/bin/env bash
# 在 Debian 开发机上交叉编译并打包 rfrp 三产物。
# 产物输出到 dist/，并生成 SHA256SUMS。
set -euo pipefail

VERSION="$(cargo metadata --no-deps --format-version 1 | python3 -c 'import sys,json; print(json.load(sys.stdin)["packages"][0]["version"])')"
DIST="dist"
rm -rf "$DIST"
mkdir -p "$DIST"

echo "==> Building linux-gnu"
cargo build --release --target x86_64-unknown-linux-gnu

echo "==> Building linux-musl"
cargo build --release --target x86_64-unknown-linux-musl

echo "==> Building windows-gnu"
cargo build --release --target x86_64-pc-windows-gnu

# 打包公共内容：包内 README（替换版本/包名）+ 配置模板 + 许可证
prepare_common() {
  local dest="$1" pkg="$2"
  mkdir -p "$dest"
  sed -e "s/{{VERSION}}/${VERSION}/g" -e "s/{{PACKAGE}}/${pkg}/g" \
    scripts/release-readme.md > "$dest/README.md"
  cp examples/rfrp-server.toml examples/rfrp-client.toml LICENSE "$dest/"
}

# Linux gnu
mkdir -p "$DIST/linux-gnu"
cp target/x86_64-unknown-linux-gnu/release/rfrp "$DIST/linux-gnu/rfrp"
prepare_common "$DIST/linux-gnu" "x86_64-linux-gnu"
cp deploy/systemd/rfrp-server.service deploy/systemd/rfrp-client.service "$DIST/linux-gnu/"
tar -C "$DIST/linux-gnu" -czf "$DIST/rfrp-${VERSION}-x86_64-linux-gnu.tar.gz" .

# Linux musl
mkdir -p "$DIST/linux-musl"
cp target/x86_64-unknown-linux-musl/release/rfrp "$DIST/linux-musl/rfrp"
prepare_common "$DIST/linux-musl" "x86_64-linux-musl"
cp deploy/systemd/rfrp-server.service deploy/systemd/rfrp-client.service "$DIST/linux-musl/"
tar -C "$DIST/linux-musl" -czf "$DIST/rfrp-${VERSION}-x86_64-linux-musl.tar.gz" .

# Windows gnu（zip 内含 exe + 配置模板 + 许可证 + 说明）
mkdir -p "$DIST/windows-gnu"
cp target/x86_64-pc-windows-gnu/release/rfrp.exe "$DIST/windows-gnu/rfrp.exe"
prepare_common "$DIST/windows-gnu" "x86_64-windows-gnu"
if command -v zip >/dev/null 2>&1; then
  (cd "$DIST/windows-gnu" && zip -q "../rfrp-${VERSION}-x86_64-windows-gnu.zip" \
    rfrp.exe README.md LICENSE rfrp-server.toml rfrp-client.toml)
else
  (cd "$DIST/windows-gnu" && python3 -m zipfile -c \
    "../rfrp-${VERSION}-x86_64-windows-gnu.zip" \
    rfrp.exe README.md LICENSE rfrp-server.toml rfrp-client.toml)
fi

# 清理临时目录
rm -rf "$DIST/linux-gnu" "$DIST/linux-musl" "$DIST/windows-gnu"

# 校验和
(cd "$DIST" && sha256sum *.tar.gz *.zip > SHA256SUMS)

echo "==> Release artifacts in $DIST"
ls -lh "$DIST"
