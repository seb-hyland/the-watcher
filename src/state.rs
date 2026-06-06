use std::{ops::Deref, sync::OnceLock};

use chrono::Local;
use uuid::Uuid;

use crate::{
    build::{CurrentBuild, LastBuild, LastBuildInfo},
    config::WatcherConfig,
};

pub static STATE: GlobalServeState = GlobalServeState(OnceLock::new());

pub struct GlobalServeState(OnceLock<ServeState>);

impl GlobalServeState {
    pub fn initialize(&self, initial_build_id: Uuid, config: WatcherConfig) {
        let state = ServeState {
            current_build: CurrentBuild::default(),
            last_build: LastBuild::from(LastBuildInfo {
                id: initial_build_id,
                timestamp: Local::now(),
            }),
            config,
        };

        self.0.get_or_init(|| state);
    }
}

impl Deref for GlobalServeState {
    type Target = ServeState;

    fn deref(&self) -> &Self::Target {
        self.0
            .get()
            .expect("Attempted access of global state before initialization!")
    }
}

pub struct ServeState {
    pub current_build: CurrentBuild,
    pub last_build: LastBuild,
    pub config: WatcherConfig,
}
