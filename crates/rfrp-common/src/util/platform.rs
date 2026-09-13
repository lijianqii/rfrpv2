//! 平台差异封装。任何平台相关的路径/信号/行为都集中在此模块，
//! 上层代码一律使用 tokio 抽象，不直接调用平台特定 API。
//!
//! 当前提供 run_id 默认路径解析；信号处理由 rfrps / rfrpc 内部实现。

/// 过滤空字符串的环境变量值（服务/容器环境可能设置为空）。
#[cfg(any(unix, windows))]
fn non_empty(value: Option<std::ffi::OsString>) -> Option<std::ffi::OsString> {
    value.filter(|v| !v.is_empty())
}

/// 返回用户主目录（Linux `$HOME` / Windows `%USERPROFILE%`；空值视为缺失）。
pub fn home_dir() -> Option<std::path::PathBuf> {
    #[cfg(unix)]
    {
        non_empty(std::env::var_os("HOME")).map(std::path::PathBuf::from)
    }
    #[cfg(windows)]
    {
        // USERPROFILE 是正常交互式会话的主目录；服务/精简环境可能缺失或为空，
        // 此时回退到 Windows 传统变量 HOMEDRIVE + HOMEPATH。
        non_empty(std::env::var_os("USERPROFILE"))
            .or_else(|| {
                let drive = non_empty(std::env::var_os("HOMEDRIVE"))?;
                let path = non_empty(std::env::var_os("HOMEPATH"))?;
                let mut p = std::path::PathBuf::from(drive);
                p.push(path);
                Some(p.into_os_string())
            })
            .map(std::path::PathBuf::from)
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

    /// 环境变量是进程级全局状态：涉及修改的测试用同一把锁串行化，
    /// 避免并发读取到中间态。
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// 恢复环境变量（None = 删除）。
    fn restore(key: &str, value: Option<std::ffi::OsString>) {
        match value {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
    }

    #[test]
    fn default_run_id_path_under_home_dot_rfrp() {
        let _guard = ENV_LOCK.lock().unwrap();
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
        let _guard = ENV_LOCK.lock().unwrap();
        #[cfg(unix)]
        {
            let old = std::env::var_os("HOME");
            std::env::remove_var("HOME");
            assert_eq!(
                default_run_id_path(),
                std::path::PathBuf::from("./.rfrp/run_id")
            );
            // 空值同样视为缺失（服务/容器环境可能设置为空串）。
            std::env::set_var("HOME", "");
            assert_eq!(
                default_run_id_path(),
                std::path::PathBuf::from("./.rfrp/run_id")
            );
            restore("HOME", old);
        }
        #[cfg(windows)]
        {
            // Windows 下主目录来源依次为 USERPROFILE → HOMEDRIVE+HOMEPATH；
            // 全部缺失时才回退当前目录。
            let old = [
                ("USERPROFILE", std::env::var_os("USERPROFILE")),
                ("HOMEDRIVE", std::env::var_os("HOMEDRIVE")),
                ("HOMEPATH", std::env::var_os("HOMEPATH")),
            ];
            for (k, _) in &old {
                std::env::remove_var(k);
            }
            assert_eq!(
                default_run_id_path(),
                std::path::PathBuf::from("./.rfrp/run_id")
            );
            // USERPROFILE 为空串也视为缺失；HOMEDRIVE/HOMEPATH 存在时走回退组合。
            std::env::set_var("USERPROFILE", "");
            std::env::set_var("HOMEDRIVE", "C:");
            std::env::set_var("HOMEPATH", r"\Users\rfrp-empty");
            assert_eq!(
                default_run_id_path(),
                std::path::PathBuf::from(r"C:\Users\rfrp-empty\.rfrp\run_id")
            );
            for (k, v) in old {
                restore(k, v);
            }
        }
    }

    #[cfg(windows)]
    #[test]
    fn default_run_id_path_uses_home_drive_fallback() {
        // USERPROFILE 缺失（服务/精简环境）时回退 HOMEDRIVE + HOMEPATH。
        let _guard = ENV_LOCK.lock().unwrap();
        let old = [
            ("USERPROFILE", std::env::var_os("USERPROFILE")),
            ("HOMEDRIVE", std::env::var_os("HOMEDRIVE")),
            ("HOMEPATH", std::env::var_os("HOMEPATH")),
        ];

        std::env::remove_var("USERPROFILE");
        std::env::set_var("HOMEDRIVE", "C:");
        std::env::set_var("HOMEPATH", r"\Users\rfrp-test");
        assert_eq!(
            default_run_id_path(),
            std::path::PathBuf::from(r"C:\Users\rfrp-test\.rfrp\run_id")
        );

        for (k, v) in old {
            restore(k, v);
        }
    }
}
