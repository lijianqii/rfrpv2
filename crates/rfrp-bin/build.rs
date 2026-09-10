//! Windows 资源嵌入（版本信息 / 应用清单 / 图标）。
//!
//! 目的：让 Windows 二进制携带完整的 PE 元数据（公司名、产品名、描述、版权、
//! 版本号），并声明 asInvoker 权限与 Win10+ 兼容清单——没有这些元数据时，
//! 杀毒软件的启发式引擎更容易把网络工具判定为可疑程序。
//!
//! 仅在目标平台为 Windows 时嵌入；Linux/macOS 宿主交叉编译 Windows 目标同样
//! 生效（embed-resource 内置 rc 解析器，不依赖 MSVC 工具链）。

fn main() {
    let target = std::env::var("TARGET").unwrap_or_default();
    if !target.contains("windows") {
        return;
    }
    println!("cargo:rerun-if-changed=resources/rfrp.rc");
    println!("cargo:rerun-if-changed=resources/rfrp.manifest.xml");
    println!("cargo:rerun-if-changed=resources/rfrp.ico");
    embed_resource::compile("resources/rfrp.rc", embed_resource::NONE);
}
