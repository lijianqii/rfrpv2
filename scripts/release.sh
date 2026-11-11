#!/usr/bin/env bash
# 在 Debian 开发机上交叉编译并打包 rfrp 两产物（linux-gnu / windows-gnu）。
# 产物输出到 dist/，并生成 SHA256SUMS。
set -euo pipefail

# 固定到仓库根：避免从其他目录执行时 rm -rf 到错误的 dist/。
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# 跳过质量门（仅用于本地反复打包；正式发布不应带 SKIP_QUALITY_GATES=1）。
if [ "${SKIP_QUALITY_GATES:-0}" = "1" ]; then
  echo "==> WARNING: quality gates skipped (SKIP_QUALITY_GATES=1)"
else
  # 质量门：格式化检查 + Clippy 严格 + 构建 + 测试。
  # 放在交叉编译之前：坏掉的提交不必浪费几分钟去产出产物。
  echo "==> Quality gates: fmt-check / clippy / build / test"
  cargo fmt --all --check
  cargo clippy --locked --all-targets -- -D warnings
  cargo build --locked --all
  cargo test --locked --all

  # UDP 突发回收率闸门：release-only 用例（debug 构建下新旧代码都会丢包，
  # 测不出差异），因此必须显式带 --ignored 跑。
  echo "==> Quality gates: udp burst regression (release)"
  cargo test --locked --release -p rfrpc --test udp_burst -- --ignored --nocapture
fi

VERSION="$(cargo metadata --locked --no-deps --format-version 1 | python3 -c 'import sys,json; d=json.load(sys.stdin); print(next(p["version"] for p in d["packages"] if p["name"] == "rfrp-bin"))')"
DIST="$ROOT/dist"
rm -rf "$DIST"
mkdir -p "$DIST"

echo "==> Building linux-gnu"
cargo build --locked --release --target x86_64-unknown-linux-gnu

echo "==> Building windows-gnu"
cargo build --locked --release --target x86_64-pc-windows-gnu

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
rm -rf "$DIST/linux-gnu" "$DIST/windows-gnu"

# 校验和
(cd "$DIST" && sha256sum *.tar.gz *.zip > SHA256SUMS)

echo "==> Release artifacts in $DIST"
ls -lh "$DIST"
