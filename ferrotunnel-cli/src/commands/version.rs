//! Version subcommand implementation

pub fn run() {
    println!("ferrotunnel {}", env!("CARGO_PKG_VERSION"));
    println!("rustc {}", rustc_version());

    #[cfg(target_os = "linux")]
    println!("target: linux");
    #[cfg(target_os = "macos")]
    println!("target: macos");
    #[cfg(target_os = "windows")]
    println!("target: windows");
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    println!("target: unknown");
}

fn rustc_version() -> &'static str {
    // This would ideally come from build.rs, but for simplicity we use a placeholder
    "1.91+"
}
