use std::path::PathBuf;

use uuid::Uuid;

use crate::{build::BuildEventType, state::STATE};

pub trait UuidFormatExt {
    fn display(&self) -> String;
    fn from_display(s: &str) -> Self;
    fn build_dir(&self) -> PathBuf;
}

impl UuidFormatExt for Uuid {
    fn display(&self) -> String {
        base62::encode(self.as_u128())
    }

    fn from_display(s: &str) -> Self {
        base62::decode(s).map(Uuid::from_u128).unwrap()
    }

    fn build_dir(&self) -> PathBuf {
        STATE.config.build_dir.join(self.display())
    }
}

pub fn log_file_name(stage: usize, ty: BuildEventType) -> String {
    match ty {
        BuildEventType::Message => format!(".watcher_log_{stage}"),
        BuildEventType::Error => format!(".watcher_err_{stage}"),
    }
}
