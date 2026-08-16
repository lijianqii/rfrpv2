//! 鉴权原语。首版为单一共享 token，登录时由 rfrps 常量时间比对。
//!
//! `M3` 起服务端登录流程调用本模块校验，且空 token 在配置校验阶段报错。
//! 见 DESIGN §10.1、§9.4。

/// 常量时间比对两个 token。
///
/// 长度不同直接返回 `false`（长度本身不视为机密）；长度相同时按字节做
/// 恒定时间异或累加，避免时序侧信道。空串与空串比较会返回 `true`，
/// 调用方需在 M3 保证 expected 非空。
#[allow(dead_code)]
pub fn verify_token(expected: &str, provided: &str) -> bool {
    if expected.len() != provided.len() {
        return false;
    }
    let mut diff: u8 = 0;
    let e = expected.as_bytes();
    let p = provided.as_bytes();
    for (a, b) in e.iter().zip(p.iter()) {
        diff |= a ^ b;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_tokens_pass() {
        assert!(verify_token("shared-secret", "shared-secret"));
    }

    #[test]
    fn different_tokens_fail() {
        assert!(!verify_token("shared-secret", "shared-wrong"));
        assert!(!verify_token("abc", "abd"));
    }

    #[test]
    fn length_mismatch_fails() {
        assert!(!verify_token("short", "longer-token"));
    }

    #[test]
    fn empty_tokens_equal() {
        // 调用方需保证 expected 非空；此处仅验证函数本身语义。
        assert!(verify_token("", ""));
    }
}
