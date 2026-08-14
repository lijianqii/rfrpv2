//! M0 基准骨架。
//!
//! 真实基准（帧编解码吞吐、双向桥接单连接吞吐、配置解析耗时）在 M6 用 Criterion 填充，
//! 见 DESIGN §14.3。此处仅保留一个可编译目标，使 `cargo bench` / `cargo test` 有基准入口。

#[test]
fn bench_skeleton_compiles() {
    // 占位，避免 bench 目标为空导致编译告警。M6 用 Criterion 填充。
}
