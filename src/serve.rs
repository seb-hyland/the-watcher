use std::{fmt::Display, sync::Arc};

use async_stream::stream;
use axum::{
    Router,
    body::Body,
    extract::{
        Path, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{Request, StatusCode, Uri, header},
    response::{Html, IntoResponse, Redirect, Response},
    routing::get,
};
use futures::{
    SinkExt, Stream, StreamExt,
    channel::{mpsc, oneshot},
    future::join_all,
};
use http_body_util::BodyExt;
use serde::Serialize;
use tokio::{fs::read_to_string, net::TcpListener};
use tower::ServiceExt;
use tower_http::services::ServeDir;

use crate::{
    build::{
        BuildEvent, BuildEventType, BuildRequest, LastBuild, QueryLastBuild, RunnableRequest,
        StartBuild, SubscribeBuild,
    },
    config::{BuildStage, WatcherConfig},
    utils::{BuildId, log_file_name},
};

#[derive(Clone)]
pub struct ServerCtx {
    pub request_tx: mpsc::Sender<BuildRequest>,
    pub config: Arc<WatcherConfig>,
}

impl ServerCtx {
    async fn make_build_request<R: RunnableRequest>(
        &mut self,
        request: impl FnOnce(oneshot::Sender<R::Response>) -> R,
    ) -> R::Response {
        let (res_tx, res_rx) = oneshot::channel();
        self.request_tx
            .send(request(res_tx).into_request())
            .await
            .expect("Failed to send to BuildManager. This is a bug!");
        res_rx
            .await
            .expect("Channel dropped without response. This is a bug!")
    }
}

pub async fn serve(ctx: ServerCtx) {
    let server = Router::new()
        .route("/rebuild", get(rebuild_handler))
        .route("/build_{id}", get(build_handler))
        .route("/build_{id}/ws", get(build_ws_handler))
        .fallback(serve_dir)
        .with_state(ctx.clone());

    let addr = format!("{}:{}", ctx.config.ip, ctx.config.port);
    let listener = TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("Failed to open listener at {addr} due to {e}"));

    println!("Serving at http://{addr}");
    axum::serve(listener, server)
        .await
        .unwrap_or_else(|e| panic!("Failed to start server at {addr} due to {e}"));
}

async fn rebuild_handler(State(mut ctx): State<ServerCtx>) -> Response {
    let response = ctx.make_build_request(|res_tx| StartBuild { res_tx }).await;
    let id = match response {
        Ok(id) => id,
        Err(id) => id,
    };
    Redirect::to(&format!("/build_{}", id)).into_response()
}

async fn build_handler(Path(id): Path<String>, State(ctx): State<ServerCtx>) -> Response {
    let log_divs = ctx
        .config
        .build_stages
        .iter()
        .enumerate()
        .map(|(idx, BuildStage { name, .. })| {
            format!(
                r#"
                    <h1 class="build-stage-name">{name}</h1>
                    <pre class="log-messages" id="log-messages-{idx}"></pre>
                    <pre class="log-error" id="log-error-{idx}"></pre>
                "#
            )
        })
        .collect::<Vec<String>>()
        .join("");
    let document = format!(
        include_str!("web/build.html"),
        id = id,
        log_divs = log_divs,
        bundle_js = include_str!(concat!(env!("OUT_DIR"), "/bundle.js")),
    );

    Html::from(document).into_response()
}

async fn build_ws_handler(
    Path(id): Path<String>,
    State(mut ctx): State<ServerCtx>,
    ws: WebSocketUpgrade,
) -> Response {
    #[derive(Serialize)]
    struct WsInitMessage {
        ty: &'static str,
        is_active: bool,
    }

    let id = BuildId::parse(&id);
    let response = ctx
        .make_build_request(|res_tx| SubscribeBuild { res_tx })
        .await;

    if let Some((cur_id, log)) = response
        && cur_id == id
    {
        return ws.on_upgrade(async |mut socket| {
            let init_msg = WsInitMessage {
                ty: "Init",
                is_active: true,
            };
            send_ws(&mut socket, &init_msg).await;

            stream_ws(socket, log.event_stream().boxed()).await;
        });
    }

    let build_dir = ctx.config.build_subdirectory_of(id);
    if !build_dir.exists() {
        return StatusCode::NOT_FOUND.into_response();
    }

    ws.on_upgrade(async move |mut socket| {
        let init_msg = WsInitMessage {
            ty: "Init",
            is_active: false,
        };
        send_ws(&mut socket, &init_msg).await;

        let (msg_futures, err_futures): (Vec<_>, Vec<_>) = (0..ctx.config.build_stages.len())
            .map(|idx| {
                let msg_path = build_dir.join(log_file_name(idx, BuildEventType::Message));
                let err_path = build_dir.join(log_file_name(idx, BuildEventType::Error));

                (
                    async move { (read_to_string(msg_path).await.unwrap_or_default(), idx) },
                    async move { (read_to_string(err_path).await.unwrap_or_default(), idx) },
                )
            })
            .unzip();
        let (msgs, errs) = (join_all(msg_futures).await, join_all(err_futures).await);

        let stream = stream! {
            for (msg, stage) in msgs {
                yield Arc::new(BuildEvent {
                    ty: BuildEventType::Message,
                    payload: msg,
                    stage
                });
            }
            for (err, stage) in errs {
                yield Arc::new(BuildEvent {
                    ty: BuildEventType::Error,
                    payload: err,
                    stage
                });
            }
        };
        stream_ws(socket, stream.boxed()).await;
    })
}

async fn stream_ws(mut socket: WebSocket, mut stream: impl Stream<Item = Arc<BuildEvent>> + Unpin) {
    while let Some(event) = stream.next().await {
        send_ws(&mut socket, &*event).await;
    }
}

async fn send_ws(socket: &mut WebSocket, data: &impl Serialize) {
    let _ = socket
        .send(Message::Text(
            serde_json::to_string(data)
                .expect("Failed to serialize event! This is a bug.")
                .into(),
        ))
        .await;
}

async fn serve_dir(uri: Uri, State(mut ctx): State<ServerCtx>) -> Response {
    let response = ctx
        .make_build_request(|res_tx| QueryLastBuild { res_tx })
        .await;
    let Some(LastBuild { id, timestamp }) = response else {
        // Still initializing! Redirect will send it to the build.
        return Redirect::to("/rebuild").into_response();
    };

    let last_build_dir = ctx.config.build_subdirectory_of(id);
    let artifact_path = last_build_dir.join(&ctx.config.artifact_path);

    let service = ServeDir::new(artifact_path);
    let req = Request::builder()
        .uri(uri)
        .body(Body::empty())
        .expect("Failed to construct request object. This is a bug!");

    let Ok(res) = service.oneshot(req).await;
    let is_html = res
        .headers()
        .get(header::CONTENT_TYPE)
        .is_some_and(|h| h.to_str().map(|s| s.contains("text/html")).unwrap_or(false));

    if !is_html {
        return res.into_response();
    }

    let (head, body) = res.into_parts();
    let Ok(collected_body) = body.collect().await else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let body_bytes = collected_body.to_bytes();
    let html_content = String::from_utf8_lossy(&body_bytes);

    let current_build = ctx
        .make_build_request(|res_tx| SubscribeBuild { res_tx })
        .await;

    fn anchor_generator(link: impl Display, text: impl Display) -> String {
        format!(r#"<a target="blank" href="{link}">{text}</a>"#)
    }
    fn build_link_generator(id: BuildId, text: impl Display) -> String {
        anchor_generator(format!("/build_{id}"), text)
    }
    let cur_build_link = build_link_generator(id, format!("build {id}"));

    let banner_msg = if let Some((new_build_id, _)) = current_build {
        format!(
            r#"You are viewing {cur_build_link}. A rebuild is in process; {new_build_link} to see its status."#,
            new_build_link = build_link_generator(new_build_id, "click here"),
        )
    } else {
        format!(
            r#"You are viewing {cur_build_link}, which finished at {last_build_time}. To rebuild, click {rebuild_link}."#,
            last_build_time = timestamp.format("%B %d at %I:%M %p (%Z)"),
            rebuild_link = anchor_generator("/rebuild", "here"),
        )
    };
    let header_element = format!(
        r#"<header style="
                background-color: LightGray;
                color: #3C3836;
                text-align: center;
                padding: 15px 0;
                font-style: oblique;
                box-sizing: border-box;
            ">{banner_msg}</header>"#
    );

    let marker = "<body>";
    let modified_html = if let Some(pos) = html_content.find(marker) {
        let (before, after) = html_content.split_at(pos + marker.chars().count());
        format!("{before}{header_element}{after}")
    } else {
        format!("{header_element}{html_content}")
    };

    (head.status, Html(modified_html)).into_response()
}
