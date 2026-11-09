# 常用开发任务。用法：make <target>，默认 make ci

.PHONY: all fmt fmt-check clippy build check test ci ci-release clean

all: fmt-check clippy build test

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all --check

clippy:
	cargo clippy --all-targets -- -D warnings

build:
	cargo build --all

check:
	cargo check --all-targets

test:
	cargo test --all

# CI 等价流程：格式化检查 + Clippy 严格 + 构建 + 测试
ci: fmt-check clippy build test

# 完整质量门：在 ci 基础上追加 release-only 的 UDP 突发闸门。
ci-release: ci bench-gate

clean:
	cargo clean

.PHONY: bench bench-gate gen-cert release

# 性能基准（Criterion）
bench:
	cargo bench -p rfrp-common --bench forward

# release-only 性能回归闸门（UDP 突发回收率）。
# debug 构建的转发速率低一个数量级，突发用例在 debug 下新旧代码都会丢包、测不出差异，
# 因此必须用 release 构建并显式带 --ignored 运行。
# scripts/release.sh 会在打包前自动跑这道闸门。
bench-gate:
	cargo test --release -p rfrpc --test udp_burst -- --ignored --nocapture

# 生成 examples 下的自签证书
gen-cert:
	./scripts/gen-self-signed-cert.sh

# 交叉编译并打包三产物到 dist/
release:
	./scripts/release.sh
