use std::{
    fs::OpenOptions,
    io::Write,
    path::PathBuf,
    process::{ExitStatus, Stdio},
    sync::Arc,
};

use async_stream::stream;
use chrono::{DateTime, Local};
use futures::{
    Stream, StreamExt,
    channel::{
        mpsc::{self},
        oneshot,
    },
    future::OptionFuture,
};
use serde::Serialize;
use tokio::{
    fs::create_dir_all,
    io::{self, AsyncBufReadExt, BufReader},
    process::Command,
    runtime,
    sync::broadcast,
    task::LocalSet,
};
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    config::{BuildStage, WatcherConfig},
    utils::{BuildId, log_file_name},
};

pub const BROADCAST_CHANNEL_SIZE: usize = 250;

pub struct BuildManager {
    channel: mpsc::Receiver<BuildRequest>,
    current: Option<CurrentBuild>,
    last: Option<LastBuild>,
}

impl BuildManager {
    pub async fn initialize(
        config: Arc<WatcherConfig>,
    ) -> Result<mpsc::Sender<BuildRequest>, BuildId> {
        let (sender, receiver) = mpsc::channel(BROADCAST_CHANNEL_SIZE);
        std::thread::spawn(move || {
            let rt = runtime::Builder::new_current_thread()
                .enable_io()
                .build()
                .expect("Failed to start runtime for BuildManager. This is a bug!");
            let local = LocalSet::new();

            local.block_on(&rt, async {
                let mut manager = BuildManager {
                    channel: receiver,
                    current: None,
                    last: None,
                };

                loop {
                    tokio::select! {
                        Ok(msg) = manager.channel.recv() => {
                            let run_ctx = RunContext {
                                manager: &mut manager,
                                config: Arc::clone(&config),
                                local_set: &local,
                            };
                            msg.run(run_ctx);
                        },
                        Some(build_event) = OptionFuture::from(
                            manager.current.as_mut().map(|cur_build| cur_build.build_channel.recv())
                        ) => {
                            use broadcast::error::RecvError;

                            let current = manager
                                .current
                                .as_mut()
                                .expect("No ongoing build while build event received. This is a bug!");
                            match build_event {
                                Ok(event) => current.info.log.log_event(event),
                                Err(RecvError::Lagged(_)) => {},
                                // Build finished
                                Err(RecvError::Closed) => {
                                    if manager.last.is_none() {
                                        println!("Server initialized: first build complete at {id}", id = current.info.id);
                                    }

                                    manager.last = Some(LastBuild {
                                        id: current.info.id,
                                        timestamp: Local::now(),
                                    });
                                    current.info.log.dump(config.build_subdirectory_of(current.info.id));
                                    manager.current = None;
                                },
                            };
                        },
                    }
                }
            });
        });

        Ok(sender)
    }
}

pub struct RunContext<'env> {
    manager: &'env mut BuildManager,
    config: Arc<WatcherConfig>,
    local_set: &'env LocalSet,
}

pub trait RunnableRequest: Sized {
    type Response;
    fn into_request(self) -> BuildRequest;

    fn compute_response(&self, ctx: RunContext<'_>) -> Self::Response;

    fn response_channel(self) -> oneshot::Sender<Self::Response>;

    fn run(self, ctx: RunContext<'_>) {
        let resp = self.compute_response(ctx);
        let resp_tx = self.response_channel();
        resp_tx
            .send(resp)
            .unwrap_or_else(|_| panic!("Failed to send response to BuildRequest. This is a bug!"));
    }
}

pub enum BuildRequest {
    Start(StartBuild),
    Subscribe(SubscribeBuild),
    QueryLast(QueryLastBuild),
}

impl BuildRequest {
    fn run(self, ctx: RunContext<'_>) {
        match self {
            BuildRequest::Start(start_build) => start_build.run(ctx),
            BuildRequest::Subscribe(subscribe_build) => subscribe_build.run(ctx),
            BuildRequest::QueryLast(query_last_build) => query_last_build.run(ctx),
        }
    }
}

pub struct StartBuild {
    pub res_tx: oneshot::Sender<StartBuildResponse>,
}

type StartBuildResponse = Result<BuildId, BuildId>;

impl RunnableRequest for StartBuild {
    type Response = StartBuildResponse;
    fn into_request(self) -> BuildRequest {
        BuildRequest::Start(self)
    }

    fn compute_response(&self, ctx: RunContext<'_>) -> Self::Response {
        match &ctx.manager.current {
            Some(cur_build) => Err(cur_build.info.id),
            None => {
                let build_info = CurrentBuildInfo::with_capacity(ctx.config.build_stages.len());
                let id = build_info.id;

                let (tx, rx) = broadcast::channel(BROADCAST_CHANNEL_SIZE);
                ctx.local_set.spawn_local(async move {
                    build(id, tx, ctx.config).await;
                });

                ctx.manager.current = Some(CurrentBuild {
                    info: build_info,
                    build_channel: rx,
                });
                Ok(id)
            }
        }
    }

    fn response_channel(self) -> oneshot::Sender<Self::Response> {
        self.res_tx
    }
}

pub struct SubscribeBuild {
    pub res_tx: oneshot::Sender<SubscribeBuildResponse>,
}

type SubscribeBuildResponse = Option<(BuildId, BuildLog)>;

impl RunnableRequest for SubscribeBuild {
    type Response = Option<(BuildId, BuildLog)>;
    fn into_request(self) -> BuildRequest {
        BuildRequest::Subscribe(self)
    }

    fn compute_response(&self, ctx: RunContext<'_>) -> Self::Response {
        ctx.manager
            .current
            .as_ref()
            .map(|cur_build| (cur_build.info.id, cur_build.info.log.clone()))
    }

    fn response_channel(self) -> oneshot::Sender<Self::Response> {
        self.res_tx
    }
}

pub struct QueryLastBuild {
    pub res_tx: oneshot::Sender<QueryLastBuildResponse>,
}

type QueryLastBuildResponse = Option<LastBuild>;

impl RunnableRequest for QueryLastBuild {
    type Response = QueryLastBuildResponse;
    fn into_request(self) -> BuildRequest {
        BuildRequest::QueryLast(self)
    }

    fn compute_response(&self, ctx: RunContext<'_>) -> Self::Response {
        ctx.manager.last
    }

    fn response_channel(self) -> oneshot::Sender<Self::Response> {
        self.res_tx
    }
}

struct CurrentBuild {
    info: CurrentBuildInfo,
    build_channel: broadcast::Receiver<Arc<BuildEvent>>,
}

struct CurrentBuildInfo {
    id: BuildId,
    log: BuildLog,
}

#[derive(Clone, Copy)]
pub struct LastBuild {
    pub id: BuildId,
    pub timestamp: DateTime<Local>,
}

impl CurrentBuildInfo {
    fn with_capacity(cap: usize) -> Self {
        Self {
            id: BuildId::new(),
            log: BuildLog::with_capacity(cap),
        }
    }
}

async fn build(
    id: BuildId,
    channel: broadcast::Sender<Arc<BuildEvent>>,
    config: Arc<WatcherConfig>,
) -> BuildResult {
    use BuildEventType::*;
    use BuildResult::*;

    let build_dir = config.build_dir.join(id.to_string());

    if let Err(e) = create_dir_all(&build_dir).await {
        let event = BuildEvent {
            ty: Error,
            payload: format!(
                "Failed to create build directory at {}: {e}",
                build_dir.display()
            ),
            stage: 0,
        };
        let _ = channel.send(Arc::new(event));

        return Failure;
    }

    for (stage_idx, BuildStage { program, args, .. }) in config.build_stages.iter().enumerate() {
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
                let _ = channel.send(Arc::new(e));
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
                    let _ = channel.send(Arc::new(event));
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

                    let _ = channel.send(Arc::new(exit_event));
                    if build_failed {
                        return Failure;
                    }

                    break;
                }
            }
        }
    }

    let artifact_path = build_dir.join(&config.artifact_path);
    if !artifact_path.exists() {
        let err_event = BuildEvent {
            ty: Error,
            payload: format!(
                "Expected build output at {} does not exist.",
                artifact_path.display()
            ),
            stage: config.build_stages.len() - 1,
        };
        let _ = channel.send(Arc::new(err_event));

        return Failure;
    }

    Success
}

#[derive(PartialEq)]
pub enum BuildResult {
    Success,
    Failure,
}

#[derive(Clone)]
pub struct BuildLog {
    history: Vec<Arc<BuildEvent>>,
    channel: broadcast::Sender<Arc<BuildEvent>>,
}

impl BuildLog {
    pub fn with_capacity(cap: usize) -> Self {
        let (channel, _) = broadcast::channel::<Arc<BuildEvent>>(BROADCAST_CHANNEL_SIZE);
        let history = Vec::with_capacity(cap);
        Self { history, channel }
    }

    fn log_event(&mut self, event: Arc<BuildEvent>) {
        self.history.push(Arc::clone(&event));
        let _ = self.channel.send(event);
    }

    pub fn event_stream(self) -> impl Stream<Item = Arc<BuildEvent>> {
        let mut live_rx = BroadcastStream::new(self.channel.subscribe());
        let history_snapshot = self.history;

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

    fn dump(&self, dir: PathBuf) {
        for entry in self.history.iter() {
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
