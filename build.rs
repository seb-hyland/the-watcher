use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=web");

    let out_dir = env::var("OUT_DIR").expect("OUT_DIR not set");
    let out_path = PathBuf::from(out_dir).join("web_js");

    let status = Command::new("tsc")
        .arg("--outDir")
        .arg(&out_path)
        .arg("--target")
        .arg("es2022")
        .args(vec!["src/web/build_log.ts"])
        .status()
        .expect("Failed to execute tsc. Is TypeScript installed globally?");

    if !status.success() {
        panic!("TypeScript compilation failed.");
    }
}
