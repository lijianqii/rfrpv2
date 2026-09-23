//! shell 补全生成测试。

use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_rfrp");

#[test]
fn generates_bash_completions() {
    let out = Command::new(BIN)
        .args(["completions", "bash"])
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("rfrp"), "{stdout}");
}

#[test]
fn rejects_unknown_shell() {
    let out = Command::new(BIN)
        .args(["completions", "not-a-shell"])
        .output()
        .unwrap();
    assert!(!out.status.success(), "{out:?}");
}
