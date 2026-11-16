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

    // PE 版本信息必须与 workspace 版本同步；直接在源码 .rc 里写死会在版本升级后
    // 与文件名/包名不一致。这里把版本替换进模板，并复制资源文件到 OUT_DIR，
    // 让 rc 中的相对引用仍能解析。
    let manifest_dir =
        std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let resources = manifest_dir.join("resources");
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));
    let version = std::env::var("CARGO_PKG_VERSION").expect("CARGO_PKG_VERSION");

    let mut parts = version.split('.');
    let major = parts.next().unwrap_or("0");
    let minor = parts.next().unwrap_or("0");
    let patch = parts.next().unwrap_or("0");
    let file_version = format!("{major},{minor},{patch},0");

    let template =
        std::fs::read_to_string(resources.join("rfrp.rc")).expect("read resources/rfrp.rc");
    let rc = template
        .replace("{{FILEVERSION}}", &file_version)
        .replace("{{VERSION}}", &version);
    std::fs::create_dir_all(&out_dir).expect("create OUT_DIR");
    std::fs::write(out_dir.join("rfrp.rc"), rc).expect("write generated rfrp.rc");
    std::fs::copy(
        resources.join("rfrp.manifest.xml"),
        out_dir.join("rfrp.manifest.xml"),
    )
    .expect("copy manifest");
    std::fs::copy(resources.join("rfrp.ico"), out_dir.join("rfrp.ico")).expect("copy icon");

    embed_resource::compile(out_dir.join("rfrp.rc"), embed_resource::NONE);
}
