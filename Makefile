# 常用开发任务。用法：make <target>，默认 make ci

.PHONY: all fmt fmt-check clippy build check test e2e soak soak-long ci ci-release clean

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

# 真实进程级 Python E2E：功能、性能冒烟与稳定性（TCP/UDP/Dashboard/TLS/重启）。
# 只依赖 python3 标准库；显式使用刚构建的 debug 二进制，避免误用旧 release。
e2e: build
	RFRP_BIN=target/debug/rfrp python3 scripts/e2e_env.py -v

SOAK_SECS ?= 600
SOAK_LONG_SECS ?= 3600
SOAK_BIN ?= target/debug/rfrp

# 长时间稳定性测试：默认 10 分钟冒烟；报告保留在 target/soak-*。
soak: build
	E2E_SOAK_SECS=$(SOAK_SECS) RFRP_BIN=$(SOAK_BIN) python3 scripts/soak_env.py

# 1 小时 release 标准档；8 小时：make soak-long SOAK_LONG_SECS=28800
soak-long:
	cargo build --release --all
	E2E_SOAK_SECS=$(SOAK_LONG_SECS) E2E_SOAK_TCP_INTERVAL_MS=100 E2E_SOAK_UDP_INTERVAL_MS=50 RFRP_BIN=target/release/rfrp python3 scripts/soak_env.py

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
