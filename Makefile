# 常用开发任务。用法：make <target>，默认 make ci

.PHONY: all fmt fmt-check clippy build check test ci clean

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

clean:
	cargo clean
