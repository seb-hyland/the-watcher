use std::path::PathBuf;

use serde::Deserialize;

use crate::utils::BuildId;

#[derive(Deserialize)]
pub struct WatcherConfig {
    #[serde(default = "default_ip")]
    pub ip: String,

    #[serde(default = "default_port")]
    pub port: String,

    #[serde(default = "default_build_dir")]
    pub build_dir: PathBuf,

    pub build_stages: Vec<BuildStage>,
    pub artifact_path: PathBuf,
}

#[derive(Deserialize)]
pub struct BuildStage {
    pub name: String,
    pub program: String,
    pub args: Vec<String>,
}

fn default_ip() -> String {
    "127.0.0.1".to_owned()
}

fn default_port() -> String {
    "9999".to_owned()
}

fn default_build_dir() -> PathBuf {
    PathBuf::from("builds/")
}

impl WatcherConfig {
    pub fn build_subdirectory_of(&self, id: BuildId) -> PathBuf {
        self.build_dir.join(id.to_string())
    }
}
