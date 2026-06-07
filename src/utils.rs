use std::fmt::Display;

use uuid::Uuid;

use crate::build::BuildEventType;

#[derive(Clone, Copy, PartialEq)]
pub struct BuildId {
    inner: Uuid,
}

impl BuildId {
    pub fn new() -> Self {
        Self {
            inner: Uuid::now_v7(),
        }
    }

    pub fn parse(s: &str) -> Self {
        Self {
            inner: base62::decode(s).map(Uuid::from_u128).unwrap(),
        }
    }
}

impl Display for BuildId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", base62::encode(self.inner.as_u128()))
    }
}

pub fn log_file_name(stage: usize, ty: BuildEventType) -> String {
    match ty {
        BuildEventType::Message => format!(".watcher_log_{stage}"),
        BuildEventType::Error => format!(".watcher_err_{stage}"),
    }
}
