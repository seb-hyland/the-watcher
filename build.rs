use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=web");

    let out_dir = env::var("OUT_DIR").expect("OUT_DIR not set");

    let status = Command::new("esbuild")
        .arg("src/web/main.ts")
        .arg("--bundle")
        .arg("--platform=browser")
        .arg("--format=iife")
        .arg(format!(
            "--outfile={}",
            PathBuf::from(out_dir).join("bundle.js").display()
        ))
        .status()
        .expect("Failed to execute esbuild");

    if !status.success() {
        panic!("TypeScript bundling failed.");
    }
}
