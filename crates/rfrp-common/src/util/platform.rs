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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_run_id_path_under_home_dot_rfrp() {
        // HOME 存在（常见环境）：路径应为 ~/.rfrp/run_id（§6.2.1）。
        if let Some(home) = home_dir() {
            let p = default_run_id_path();
            assert_eq!(p.parent().unwrap().parent().unwrap(), home.as_path());
            assert_eq!(p.file_name().unwrap(), "run_id");
            assert_eq!(p.parent().unwrap().file_name().unwrap(), ".rfrp");
        }
    }

    #[test]
    fn default_run_id_path_falls_back_to_current_dir() {
        // 无 HOME（服务/容器环境）：回退到当前目录下的 .rfrp/run_id，不 panic。
        // 用互斥锁串行化，避免与其他读环境变量的测试并发竞争。
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = LOCK.lock().unwrap();
        #[cfg(unix)]
        {
            let old = std::env::var_os("HOME");
            std::env::remove_var("HOME");
            let p = default_run_id_path();
            assert_eq!(p, std::path::PathBuf::from("./.rfrp/run_id"));
            match old {
                Some(v) => std::env::set_var("HOME", v),
                None => std::env::remove_var("HOME"),
            }
        }
        #[cfg(windows)]
        {
            let old = std::env::var_os("USERPROFILE");
            std::env::remove_var("USERPROFILE");
            let p = default_run_id_path();
            assert_eq!(p, std::path::PathBuf::from("./.rfrp/run_id"));
            match old {
                Some(v) => std::env::set_var("USERPROFILE", v),
                None => std::env::remove_var("USERPROFILE"),
            }
        }
    }
}
