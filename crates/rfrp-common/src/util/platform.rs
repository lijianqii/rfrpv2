//! 平台差异封装。任何平台相关的路径/信号/行为都集中在此模块，
//! 上层代码一律使用 tokio 抽象，不直接调用平台特定 API。
//!
//! 当前提供 run_id 默认路径解析；信号处理由 rfrps / rfrpc 内部实现。

/// 返回用户主目录（Linux `$HOME` / Windows `%USERPROFILE%`）。
pub fn home_dir() -> Option<std::path::PathBuf> {
    #[cfg(unix)]
    {
        std::env::var_os("HOME").map(std::path::PathBuf::from)
    }
    #[cfg(windows)]
    {
        std::env::var_os("USERPROFILE").map(std::path::PathBuf::from)
    }
    #[cfg(not(any(unix, windows)))]
    {
        None
    }
}

/// run_id 默认持久化路径：`~/.rfrp/run_id`。
///
/// 可通过配置 `run_id_file` 覆盖（见 DESIGN §6.2.1）。
pub fn default_run_id_path() -> std::path::PathBuf {
    let mut p = home_dir().unwrap_or_else(|| std::path::PathBuf::from("."));
    p.push(".rfrp");
    p.push("run_id");
    p
}
