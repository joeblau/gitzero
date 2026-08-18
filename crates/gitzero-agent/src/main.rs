mod action;
mod artifact;
mod cache;
mod concurrency;
mod executor;
mod repository_access;

use anyhow::{Context, Result, bail};
use clap::Parser;
use concurrency::ConcurrencyClient;
use executor::{Executor, ExecutorConfig, default_runner_labels};
use futures_util::{SinkExt, StreamExt};
use gitzero_protocol::{
    AgentHello, AgentMessage, Conclusion, MAX_RUNNER_LABELS, MAX_RUNNER_SELECTOR_BYTES,
    PROTOCOL_VERSION, ServerMessage,
};
use repository_access::RepositoryAccessClient;
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, Semaphore, mpsc, watch},
    task::JoinHandle,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::{HeaderValue, header::AUTHORIZATION},
    },
};
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(version, about = "GitZero macOS build agent")]
struct Args {
    #[arg(long, env = "GITZERO_CONTROL_PLANE")]
    control_plane: String,

    #[arg(long, env = "GITZERO_WORKSPACE_ID")]
    workspace_id: String,

    #[arg(long, env = "GITZERO_AGENT_TOKEN", hide_env_values = true)]
    agent_token: String,

    #[arg(long, env = "GITZERO_AGENT_ID", default_value_t = default_agent_id())]
    agent_id: String,

    #[arg(long, env = "GITZERO_AGENT_NAME", default_value_t = default_agent_id())]
    name: String,

    #[arg(
        long = "label",
        env = "GITZERO_LABELS",
        value_delimiter = ',',
        help = "Custom runner label; repeat or provide a comma-separated list"
    )]
    labels: Vec<String>,

    #[arg(long, env = "GITZERO_RUNNER_GROUP")]
    runner_group: Option<String>,

    #[arg(long, env = "GITZERO_WORK_ROOT", default_value = "/tmp/gitzero-agent")]
    work_root: PathBuf,

    #[arg(long, env = "GITZERO_MAX_PARALLELISM", default_value_t = 1)]
    max_parallelism: u16,

    #[arg(long, env = "GITZERO_KEEP_FAILED_WORKSPACES", default_value_t = false)]
    keep_failed_workspaces: bool,

    #[arg(
        long,
        env = "GITZERO_CACHE_MAX_BYTES",
        default_value_t = 10 * 1024 * 1024 * 1024_u64
    )]
    cache_max_bytes: u64,

    #[arg(
        long,
        env = "GITZERO_CACHE_MAX_ENTRY_BYTES",
        default_value_t = 2 * 1024 * 1024 * 1024_u64
    )]
    cache_max_entry_bytes: u64,

    #[arg(
        long,
        env = "GITZERO_ARTIFACT_MAX_BYTES",
        default_value_t = 10 * 1024 * 1024 * 1024_u64
    )]
    artifact_max_bytes: u64,

    #[arg(
        long,
        env = "GITZERO_ARTIFACT_MAX_ENTRY_BYTES",
        default_value_t = 2 * 1024 * 1024 * 1024_u64
    )]
    artifact_max_entry_bytes: u64,
}

struct RunningJob {
    cancel: watch::Sender<bool>,
    task: JoinHandle<()>,
}

type RunningJobs = Arc<Mutex<HashMap<Uuid, RunningJob>>>;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "gitzero_agent=info".into()),
        )
        .json()
        .init();

    let mut args = Args::parse();
    if !(1..=64).contains(&args.max_parallelism) {
        bail!("--max-parallelism must be between 1 and 64");
    }
    if args.cache_max_entry_bytes == 0 || args.cache_max_bytes < args.cache_max_entry_bytes {
        bail!(
            "--cache-max-bytes must be at least --cache-max-entry-bytes, and both must be positive"
        );
    }
    if args.artifact_max_entry_bytes == 0 || args.artifact_max_bytes < args.artifact_max_entry_bytes
    {
        bail!(
            "--artifact-max-bytes must be at least --artifact-max-entry-bytes, and both must be positive"
        );
    }
    let (labels, runner_group) =
        normalize_runner_targeting(&args.labels, args.runner_group.as_deref())?;
    args.labels = labels;
    args.runner_group = runner_group;
    tokio::fs::create_dir_all(&args.work_root)
        .await
        .with_context(|| format!("create work root {}", args.work_root.display()))?;

    let executor = Arc::new(
        Executor::new(ExecutorConfig {
            work_root: args.work_root.clone(),
            runner_name: args.name.clone(),
            keep_failed_workspaces: args.keep_failed_workspaces,
            max_parallelism: usize::from(args.max_parallelism),
            cache_max_bytes: args.cache_max_bytes,
            cache_max_entry_bytes: args.cache_max_entry_bytes,
            artifact_max_bytes: args.artifact_max_bytes,
            artifact_max_entry_bytes: args.artifact_max_entry_bytes,
        })
        .with_runner_targeting(args.labels.clone(), args.runner_group.clone()),
    );
    let semaphore = Arc::new(Semaphore::new(usize::from(args.max_parallelism)));
    let running: RunningJobs = Arc::new(Mutex::new(HashMap::new()));

    let mut retry = Duration::from_secs(1);
    loop {
        match run_connection(&args, executor.clone(), semaphore.clone(), running.clone()).await {
            Ok(()) => warn!("control plane connection closed"),
            Err(error) => error!(error = %error, "control plane connection failed"),
        }
        stop_all(&running).await;
        tokio::time::sleep(retry).await;
        retry = (retry * 2).min(Duration::from_secs(30));
    }
}

async fn run_connection(
    args: &Args,
    executor: Arc<Executor>,
    semaphore: Arc<Semaphore>,
    running: RunningJobs,
) -> Result<()> {
    let url = websocket_url(args)?;
    let mut request = url
        .into_client_request()
        .context("build WebSocket request")?;
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", args.agent_token))
            .context("agent token is not a valid HTTP header value")?,
    );

    let (socket, _) = connect_async(request)
        .await
        .context("connect to control plane")?;
    info!(workspace_id = %args.workspace_id, agent_id = %args.agent_id, "connected");
    let (mut writer, mut reader) = socket.split();
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<AgentMessage>(512);
    let concurrency = ConcurrencyClient::remote(outbound_tx.clone());
    let repository_access = RepositoryAccessClient::remote(outbound_tx.clone());

    let hello = AgentMessage::Hello {
        hello: AgentHello {
            protocol_version: PROTOCOL_VERSION,
            agent_id: args.agent_id.clone(),
            name: args.name.clone(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            labels: args.labels.clone(),
            runner_group: args.runner_group.clone(),
            max_parallelism: args.max_parallelism,
        },
    };
    send_wire_message(&mut writer, &hello).await?;

    let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let result = async {
        loop {
            tokio::select! {
                outbound = outbound_rx.recv() => {
                    let Some(outbound) = outbound else {
                        bail!("outbound event channel closed");
                    };
                    send_wire_message(&mut writer, &outbound).await?;
                }
                incoming = reader.next() => {
                    let incoming = incoming.context("control plane disconnected")??;
                    match incoming {
                        Message::Text(text) => {
                            let message: ServerMessage = serde_json::from_str(text.as_ref())
                                .context("decode control plane message")?;
                            handle_server_message(
                                message,
                                executor.clone(),
                                semaphore.clone(),
                                running.clone(),
                                outbound_tx.clone(),
                                concurrency.clone(),
                                repository_access.clone(),
                            ).await?;
                        }
                        Message::Close(_) => return Ok(()),
                        Message::Ping(data) => writer.send(Message::Pong(data)).await?,
                        Message::Pong(_) | Message::Binary(_) | Message::Frame(_) => {}
                    }
                }
                _ = heartbeat.tick() => {
                    reap_finished(&running).await;
                    let job_ids = running.lock().await.keys().copied().collect();
                    let heartbeat = AgentMessage::Heartbeat {
                        message_id: Uuid::new_v4(),
                        running_job_ids: job_ids,
                    };
                    send_wire_message(&mut writer, &heartbeat).await?;
                }
            }
        }
    }
    .await;
    concurrency.cancel_all().await;
    repository_access.cancel_all().await;
    result
}

async fn handle_server_message(
    message: ServerMessage,
    executor: Arc<Executor>,
    semaphore: Arc<Semaphore>,
    running: RunningJobs,
    outbound: mpsc::Sender<AgentMessage>,
    concurrency: ConcurrencyClient,
    repository_access: RepositoryAccessClient,
) -> Result<()> {
    match message {
        ServerMessage::Welcome {
            protocol_version, ..
        } => {
            if protocol_version != PROTOCOL_VERSION {
                bail!(
                    "protocol mismatch: control plane={protocol_version}, agent={PROTOCOL_VERSION}"
                );
            }
        }
        ServerMessage::RunJob { job } => {
            let job = *job;
            reap_finished(&running).await;
            if running.lock().await.contains_key(&job.id) {
                warn!(job_id = %job.id, "ignoring duplicate assignment");
                return Ok(());
            }
            let permit = semaphore
                .clone()
                .try_acquire_owned()
                .context("control plane assigned more jobs than agent capacity")?;
            let (cancel_tx, cancel_rx) = watch::channel(false);
            let job_id = job.id;
            let task = tokio::spawn(async move {
                let _permit = permit;
                if let Err(error) = executor
                    .execute_with_services(
                        job,
                        cancel_rx,
                        outbound.clone(),
                        concurrency,
                        repository_access,
                    )
                    .await
                {
                    error!(job_id = %job_id, error = %error, "job execution crashed");
                    let _ = outbound
                        .send(AgentMessage::JobFinished {
                            message_id: Uuid::new_v4(),
                            job_id,
                            conclusion: Conclusion::Failure,
                            summary: format!("Agent execution error: {error:#}"),
                        })
                        .await;
                }
            });
            running.lock().await.insert(
                job_id,
                RunningJob {
                    cancel: cancel_tx,
                    task,
                },
            );
        }
        ServerMessage::CancelJob { job_id, reason } => {
            info!(job_id = %job_id, %reason, "cancelling job");
            if let Some(job) = running.lock().await.get(&job_id) {
                let _ = job.cancel.send(true);
            }
        }
        ServerMessage::ConcurrencyGranted { request_id } => {
            concurrency.handle_granted(request_id).await;
        }
        ServerMessage::ConcurrencyCancelled { request_id, reason } => {
            concurrency.handle_cancelled(request_id, reason).await;
        }
        ServerMessage::RepositoryTokenGranted { request_id, token } => {
            repository_access
                .handle_granted(request_id, token.into_inner())
                .await;
        }
        ServerMessage::RepositoryTokenDenied { request_id, reason } => {
            repository_access.handle_denied(request_id, reason).await;
        }
        ServerMessage::WorkflowTokenGranted { request_id, token } => {
            repository_access
                .handle_granted(request_id, token.into_inner())
                .await;
        }
        ServerMessage::WorkflowTokenDenied { request_id, reason } => {
            repository_access.handle_denied(request_id, reason).await;
        }
        ServerMessage::Ack { .. } => {}
        ServerMessage::Error { code, message } => {
            warn!(%code, %message, "control plane returned an error");
        }
    }
    Ok(())
}

async fn send_wire_message<S>(writer: &mut S, message: &AgentMessage) -> Result<()>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let json = serde_json::to_string(message).context("encode agent message")?;
    writer
        .send(Message::Text(json.into()))
        .await
        .context("send agent message")
}

async fn reap_finished(running: &RunningJobs) {
    let finished = {
        let mut running = running.lock().await;
        let ids = running
            .iter()
            .filter_map(|(job_id, job)| job.task.is_finished().then_some(*job_id))
            .collect::<Vec<_>>();
        ids.into_iter()
            .filter_map(|job_id| running.remove(&job_id).map(|job| (job_id, job.task)))
            .collect::<Vec<_>>()
    };
    for (job_id, task) in finished {
        if let Err(error) = task.await {
            error!(%job_id, %error, "job task terminated unexpectedly");
        }
    }
}

async fn stop_all(running: &RunningJobs) {
    let jobs = {
        let mut running = running.lock().await;
        for job in running.values() {
            let _ = job.cancel.send(true);
        }
        running.drain().collect::<Vec<_>>()
    };
    for (job_id, mut job) in jobs {
        match tokio::time::timeout(Duration::from_secs(30), &mut job.task).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                error!(%job_id, %error, "job task terminated unexpectedly while disconnecting");
            }
            Err(_) => {
                warn!(%job_id, "job did not stop after cancellation; aborting task");
                job.task.abort();
                let _ = job.task.await;
            }
        }
    }
}

fn websocket_url(args: &Args) -> Result<String> {
    let base = args.control_plane.trim_end_matches('/');
    let base = if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else if base.starts_with("ws://") || base.starts_with("wss://") {
        base.to_owned()
    } else {
        bail!("control plane URL must start with http://, https://, ws://, or wss://");
    };
    Ok(format!(
        "{base}/v1/workspaces/{}/connect?role=agent&agent_id={}",
        url_component(&args.workspace_id),
        url_component(&args.agent_id)
    ))
}

fn url_component(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
                vec![char::from(byte)]
            } else {
                format!("%{byte:02X}").chars().collect()
            }
        })
        .collect()
}

fn default_agent_id() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            std::process::Command::new("hostname")
                .output()
                .ok()
                .filter(|output| output.status.success())
                .and_then(|output| String::from_utf8(output.stdout).ok())
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| "gitzero-agent".to_owned())
}

fn normalize_runner_targeting(
    custom_labels: &[String],
    runner_group: Option<&str>,
) -> Result<(Vec<String>, Option<String>)> {
    let mut labels = Vec::new();
    let mut normalized = std::collections::BTreeSet::new();
    for label in default_runner_labels()
        .iter()
        .map(String::as_str)
        .chain(custom_labels.iter().map(String::as_str))
    {
        let label = label.trim();
        if label.is_empty()
            || label.len() > MAX_RUNNER_SELECTOR_BYTES
            || label.contains(['\0', '\n', '\r'])
        {
            bail!(
                "runner labels must contain 1 to {MAX_RUNNER_SELECTOR_BYTES} bytes without line breaks"
            );
        }
        if normalized.insert(label.to_ascii_lowercase()) {
            labels.push(label.to_owned());
        }
    }
    if labels.len() > MAX_RUNNER_LABELS {
        bail!("at most {MAX_RUNNER_LABELS} distinct runner labels may be configured");
    }
    let runner_group = runner_group
        .map(str::trim)
        .filter(|group| !group.is_empty())
        .map(|group| {
            if group.len() > MAX_RUNNER_SELECTOR_BYTES || group.contains(['\0', '\n', '\r']) {
                bail!(
                    "runner group must contain 1 to {MAX_RUNNER_SELECTOR_BYTES} bytes without line breaks"
                );
            }
            Ok(group.to_owned())
        })
        .transpose()?;
    Ok((labels, runner_group))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_websocket_url_from_https_endpoint() {
        let args = Args {
            control_plane: "https://ci.example.com/".to_owned(),
            workspace_id: "acme/42".to_owned(),
            agent_token: "secret".to_owned(),
            agent_id: "mini 1".to_owned(),
            name: "mini".to_owned(),
            labels: Vec::new(),
            runner_group: None,
            work_root: PathBuf::from("/tmp/test"),
            max_parallelism: 1,
            keep_failed_workspaces: false,
            cache_max_bytes: 10 * 1024 * 1024,
            cache_max_entry_bytes: 1024 * 1024,
            artifact_max_bytes: 10 * 1024 * 1024,
            artifact_max_entry_bytes: 1024 * 1024,
        };
        assert_eq!(
            websocket_url(&args).expect("url"),
            "wss://ci.example.com/v1/workspaces/acme%2F42/connect?role=agent&agent_id=mini%201"
        );
    }

    #[test]
    fn normalizes_default_and_custom_runner_targeting() {
        let (labels, group) = normalize_runner_targeting(
            &[
                " xcode-16 ".to_owned(),
                "arm64".to_owned(),
                "XCODE-16".to_owned(),
            ],
            Some(" release-minis "),
        )
        .expect("runner targeting");

        assert_eq!(labels, ["self-hosted", "macOS", "ARM64", "xcode-16"]);
        assert_eq!(group.as_deref(), Some("release-minis"));
    }

    #[test]
    fn rejects_invalid_runner_targeting() {
        assert!(normalize_runner_targeting(&["bad\nlabel".to_owned()], None).is_err());
        let too_many = (0..MAX_RUNNER_LABELS)
            .map(|index| format!("custom-{index}"))
            .collect::<Vec<_>>();
        assert!(normalize_runner_targeting(&too_many, None).is_err());
        assert!(
            normalize_runner_targeting(&[], Some(&"x".repeat(MAX_RUNNER_SELECTOR_BYTES + 1)))
                .is_err()
        );
    }

    #[tokio::test]
    async fn stop_all_cancels_and_joins_running_jobs() {
        let running: RunningJobs = Arc::new(Mutex::new(HashMap::new()));
        let (cancel, mut cancelled) = watch::channel(false);
        let task = tokio::spawn(async move {
            cancelled.changed().await.expect("cancellation sender");
            assert!(*cancelled.borrow());
        });
        running
            .lock()
            .await
            .insert(Uuid::new_v4(), RunningJob { cancel, task });

        stop_all(&running).await;

        assert!(running.lock().await.is_empty());
    }
}
