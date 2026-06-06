use std::{
    env::{args, current_dir, set_current_dir},
    fs::File,
    io::Read,
    path::PathBuf,
};

use tokio::runtime;
use uuid::Uuid;

use crate::{build::BuildLog, config::WatcherConfig, state::STATE};

mod build;
mod config;
mod serve;
mod state;
mod utils;

const CONFIG_FILE_NAME: &str = "config.toml";

fn main() {
    let mut init_args = args();
    let serve_dir = if let Some(arg) = init_args.nth(1) {
        PathBuf::from(arg)
    } else {
        current_dir().expect("Working directory could not be accessed, and no serve directory set")
    };

    if !serve_dir.exists() {
        panic!("Serve directory {} does not exist!", serve_dir.display());
    }
    set_current_dir(&serve_dir).unwrap_or_else(|e| {
        panic!(
            "Failed to set working directory to serve directory ({}). Error: {e}",
            serve_dir.display()
        )
    });

    let mut config_file = File::open(CONFIG_FILE_NAME).unwrap_or_else(|e| {
        panic!("Failed to open config file at {CONFIG_FILE_NAME} due to error {e}",)
    });
    let config_file_string = {
        let mut s = String::new();
        config_file.read_to_string(&mut s).unwrap_or_else(|e| {
            panic!("Failed to read config file at {CONFIG_FILE_NAME} into string. Error: {e}",)
        });
        s
    };
    let config: WatcherConfig = toml::from_str(&config_file_string).unwrap_or_else(|e| {
        panic!("Failed to parse config file at {CONFIG_FILE_NAME}. Error: {e}",)
    });

    let rt = runtime::Builder::new_multi_thread()
        .enable_io()
        .build()
        .unwrap_or_else(|e| panic!("Failed to start Tokio runtime! Error: {e}"));
    rt.block_on(async {
        let initial_build_id = Uuid::now_v7();

        STATE.initialize(initial_build_id, config);

        let initial_build_log = BuildLog::with_capacity(STATE.config.build_stages.len());

        let build_res = build::build(
            &STATE.config.build_stages,
            initial_build_id,
            initial_build_log,
        )
        .await;

        if build_res == build::BuildResult::Failure {
            panic!("Initial build failed!")
        }
        serve::serve().await;
    });
}
