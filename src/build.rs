use std::{
    fs::OpenOptions,
    io::Write,
    ops::Deref,
    path::PathBuf,
    process::{ExitStatus, Stdio},
    sync::{Arc, LazyLock},
};

use arc_swap::ArcSwap;
use async_stream::stream;
use chrono::{DateTime, Local};
use futures::{Stream, StreamExt};
use serde::Serialize;
use tokio::{
    fs::create_dir_all,
    io::{self, AsyncBufReadExt, BufReader},
    process::Command,
    spawn,
    sync::broadcast::{self},
};
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

use crate::{
    config::BuildStage,
    state::STATE,
    utils::{UuidFormatExt, log_file_name},
};

pub struct CurrentBuild {
    inner: ArcSwap<Option<CurrentBuildInfo>>,
}

static NO_BUILD: LazyLock<Arc<Option<CurrentBuildInfo>>> = LazyLock::new(|| Arc::new(None));

impl Default for CurrentBuild {
    fn default() -> Self {
        Self {
            inner: ArcSwap::new(Arc::clone(&NO_BUILD)),
        }
    }
}

impl CurrentBuild {
    pub fn subscribe(&self) -> Option<(Uuid, BuildLog)> {
        self.inner
            .load()
            .as_ref()
            .as_ref()
            .map(|build| (build.id, build.log.clone()))
    }

    pub fn start_build(&self) -> Result<Uuid, Uuid> {
        let build_stages = &STATE.config.build_stages;
        let current_id = Uuid::now_v7();

        let log = BuildLog::with_capacity(build_stages.len());
        let current_build_info = CurrentBuildInfo {
            id: current_id,
            log: log.clone(),
        };

        let existing_build_info = self
            .inner
            .compare_and_swap(&*NO_BUILD, Arc::new(Some(current_build_info)));
        if let Some(build_info) = &**existing_build_info
            && build_info.id != current_id
        {
            // Was already building
            return Err(build_info.id);
        }

        spawn(build(build_stages, current_id, log));
        Ok(current_id)
    }
}

struct CurrentBuildInfo {
    id: Uuid,
    log: BuildLog,
}

#[derive(Clone)]
pub struct BuildLog {
    history: Arc<ArcSwap<Vec<Arc<BuildEvent>>>>,
    channel: broadcast::Sender<Arc<BuildEvent>>,
}

impl BuildLog {
    pub fn with_capacity(cap: usize) -> Self {
        let (channel, _) = broadcast::channel::<Arc<BuildEvent>>(100);
        let history = Arc::new(ArcSwap::from_pointee(Vec::with_capacity(cap)));
        Self { history, channel }
    }

    fn log_event(&self, event: BuildEvent) {
        let event = Arc::new(event);

        let current_history = self.history.load();
        let mut new_history = (**current_history).clone();
        new_history.push(event.clone());
        self.history.store(Arc::new(new_history));

        let _ = self.channel.send(event);
    }

    pub fn event_stream(self) -> impl Stream<Item = Arc<BuildEvent>> {
        let mut live_rx = BroadcastStream::new(self.channel.subscribe());
        let history_snapshot = self.history.load();

        stream! {
            for event in history_snapshot.iter() {
                yield Arc::clone(event);
            }
            drop(history_snapshot);

            while let Some(result) = live_rx.next().await {
                match result {
                    Ok(event) => yield event,
                    Err(_err) => { break }
                }
            }
        }
    }

    pub fn dump(&self, dir: PathBuf) {
        for entry in self.history.load().iter() {
            let path = log_file_name(entry.stage, entry.ty);

            let _ = OpenOptions::new()
                .append(true)
                .create(true)
                .open(dir.join(path))
                .map(|mut file| {
                    let _ = file.write_all(entry.payload.as_bytes());
                    let _ = file.write_all(b"\n");
                });
        }
    }
}

#[derive(Serialize)]
pub struct BuildEvent {
    pub ty: BuildEventType,
    pub payload: String,
    pub stage: usize,
}

#[derive(Clone, Copy, PartialEq, Serialize)]
pub enum BuildEventType {
    Message,
    Error,
}

pub struct LastBuild {
    inner: ArcSwap<LastBuildInfo>,
}

impl From<LastBuildInfo> for LastBuild {
    fn from(value: LastBuildInfo) -> Self {
        Self {
            inner: ArcSwap::from_pointee(value),
        }
    }
}

impl Deref for LastBuild {
    type Target = ArcSwap<LastBuildInfo>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct LastBuildInfo {
    pub id: Uuid,
    pub timestamp: DateTime<Local>,
}

#[derive(PartialEq)]
pub enum BuildResult {
    Success,
    Failure,
}

pub async fn build(build_stages: &[BuildStage], id: Uuid, log: BuildLog) -> BuildResult {
    use BuildEventType::*;
    use BuildResult::*;

    // Sets the current build to `NO_BUILD` when this function returns
    struct EndBuildGuard<'s> {
        id: Uuid,
        log: &'s BuildLog,
    }
    impl<'s> Drop for EndBuildGuard<'s> {
        fn drop(&mut self) {
            self.log.dump(self.id.build_dir());
            println!("Finished build");
            STATE.current_build.inner.store(Arc::clone(&NO_BUILD));
        }
    }
    let _guard = EndBuildGuard { id, log: &log };

    println!("Started build {}", id.display());

    let build_dir = id.build_dir();
    if let Err(e) = create_dir_all(&build_dir).await {
        let event = BuildEvent {
            ty: Error,
            payload: format!(
                "Failed to create build directory at {}: {e}",
                build_dir.display()
            ),
            stage: 0,
        };
        log.log_event(event);
        return Failure;
    }

    for (stage_idx, BuildStage { program, args, .. }) in build_stages.iter().enumerate() {
        let child = Command::new(program)
            .args(args)
            .current_dir(&build_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| BuildEvent {
                ty: Error,
                payload: format!(
                    "Failed to spawn command {program} with args [{}]: {e}",
                    args.join(", ")
                ),
                stage: stage_idx,
            });
        let mut child = match child {
            Ok(c) => c,
            Err(e) => {
                log.log_event(e);
                return Failure;
            }
        };

        let stdout = child
            .stdout
            .take()
            .expect("Failed to acquire stdout. This is a bug!");
        let mut stdout_lines = BufReader::new(stdout).lines();
        let stderr = child
            .stderr
            .take()
            .expect("Failed to acquire stderr. This is a bug!");
        let mut stderr_lines = BufReader::new(stderr).lines();

        // Task has not yet completed
        loop {
            let handle_line = |res: Result<Option<String>, io::Error>| {
                if let Ok(Some(line)) = res {
                    let event = BuildEvent {
                        ty: Message,
                        payload: line,
                        stage: stage_idx,
                    };
                    log.log_event(event);
                }
            };
            let handle_exit = |status: Result<ExitStatus, io::Error>| -> BuildEvent {
                match status {
                    Err(e) => BuildEvent {
                        ty: Error,
                        payload: format!("Failed to get build status: {e}"),
                        stage: stage_idx,
                    },
                    Ok(code) if !code.success() => BuildEvent {
                        ty: Error,
                        payload: format!("Build failed with code {code}"),
                        stage: stage_idx,
                    },
                    Ok(_) => BuildEvent {
                        ty: Message,
                        payload: "========== Stage completed successfully ==========".to_owned(),
                        stage: stage_idx,
                    },
                }
            };

            tokio::select! {
                stdout_res = stdout_lines.next_line() => handle_line(stdout_res),
                stderr_res = stderr_lines.next_line() => handle_line(stderr_res),
                status = child.wait() => {
                    let exit_event = handle_exit(status);
                    let build_failed = exit_event.ty == Error;

                    log.log_event(exit_event);
                    if build_failed { return Failure }

                    break;
                }
            }
        }
    }

    let artifact_path = build_dir.join(&STATE.config.artifact_path);
    if !artifact_path.exists() {
        log.log_event(BuildEvent {
            ty: Error,
            payload: format!(
                "Expected build output at {} does not exist.",
                artifact_path.display()
            ),
            stage: build_stages.len() - 1,
        });
        return Failure;
    }

    // Dump logs and clear current build
    drop(_guard);
    STATE.last_build.store(Arc::new(LastBuildInfo {
        id,
        timestamp: Local::now(),
    }));

    Success
}
