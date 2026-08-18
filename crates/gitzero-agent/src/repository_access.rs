use anyhow::{Result, bail};
use gitzero_protocol::{AgentMessage, RepositoryTokenPurpose};
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct RepositoryAccessClient {
    remote: Option<Arc<RemoteRepositoryAccessClient>>,
}

const TOKEN_REFRESH_SAFETY_SECONDS: u64 = 5 * 60;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ExpiringToken {
    pub(crate) token: String,
    pub(crate) expires_at_epoch_seconds: u64,
}

impl ExpiringToken {
    pub(crate) fn is_usable(&self) -> bool {
        self.expires_at_epoch_seconds
            > current_epoch_seconds().saturating_add(TOKEN_REFRESH_SAFETY_SECONDS)
    }

    pub(crate) fn clear(&mut self) {
        self.token.clear();
    }
}

impl std::fmt::Debug for ExpiringToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExpiringToken")
            .field("token", &"[REDACTED]")
            .field("expires_at_epoch_seconds", &self.expires_at_epoch_seconds)
            .finish()
    }
}

struct RemoteRepositoryAccessClient {
    outbound: mpsc::Sender<AgentMessage>,
    requests: Mutex<HashMap<Uuid, oneshot::Sender<std::result::Result<ExpiringToken, String>>>>,
}

impl RepositoryAccessClient {
    #[cfg(test)]
    pub(crate) fn local() -> Self {
        Self { remote: None }
    }

    pub(crate) fn remote(outbound: mpsc::Sender<AgentMessage>) -> Self {
        Self {
            remote: Some(Arc::new(RemoteRepositoryAccessClient {
                outbound,
                requests: Mutex::new(HashMap::new()),
            })),
        }
    }

    pub(crate) async fn request_token(
        &self,
        run_id: Uuid,
        purpose: RepositoryTokenPurpose,
        owner: &str,
        repository: &str,
        cancel: &watch::Receiver<bool>,
    ) -> Result<ExpiringToken> {
        let request_id = Uuid::new_v4();
        self.request(
            request_id,
            AgentMessage::RepositoryTokenRequest {
                message_id: Uuid::new_v4(),
                job_id: run_id,
                request_id,
                purpose,
                owner: owner.to_owned(),
                repository: repository.to_owned(),
            },
            match purpose {
                RepositoryTokenPurpose::Source => "source repository access",
                RepositoryTokenPurpose::Environment => "environment metadata access",
                RepositoryTokenPurpose::SharedSource => "private shared repository access",
                RepositoryTokenPurpose::Checkout => "private checkout repository access",
            },
            cancel,
        )
        .await
    }

    pub(crate) async fn request_workflow_token(
        &self,
        run_id: Uuid,
        read_permissions: &BTreeSet<String>,
        write_permissions: &BTreeSet<String>,
        cancel: &watch::Receiver<bool>,
    ) -> Result<ExpiringToken> {
        if read_permissions.is_empty() && write_permissions.is_empty() {
            bail!("workflow token request must contain at least one permission");
        }
        let request_id = Uuid::new_v4();
        self.request(
            request_id,
            AgentMessage::WorkflowTokenRequest {
                message_id: Uuid::new_v4(),
                job_id: run_id,
                request_id,
                read_permissions: read_permissions.iter().cloned().collect(),
                write_permissions: write_permissions.iter().cloned().collect(),
            },
            "scoped workflow token access",
            cancel,
        )
        .await
    }

    async fn request(
        &self,
        request_id: Uuid,
        message: AgentMessage,
        operation: &str,
        cancel: &watch::Receiver<bool>,
    ) -> Result<ExpiringToken> {
        let Some(remote) = &self.remote else {
            bail!("{operation} requires the connected control plane");
        };
        let (decision_tx, mut decision_rx) = oneshot::channel();
        remote.requests.lock().await.insert(request_id, decision_tx);
        if remote.outbound.send(message).await.is_err() {
            remote.requests.lock().await.remove(&request_id);
            bail!("control plane event channel closed while requesting {operation}");
        }

        let mut cancellation = cancel.clone();
        let response = loop {
            if *cancellation.borrow() {
                remote.requests.lock().await.remove(&request_id);
                bail!("run cancelled while requesting {operation}");
            }
            tokio::select! {
                response = &mut decision_rx => break response,
                changed = cancellation.changed() => {
                    if changed.is_err() {
                        break decision_rx.await;
                    }
                }
            }
        };
        match response {
            Ok(Ok(token)) if token.token.is_empty() => {
                bail!("control plane returned an empty token for {operation}")
            }
            Ok(Ok(token)) if !token.is_usable() => {
                bail!("control plane returned a token that expires too soon for {operation}")
            }
            Ok(Ok(token)) => Ok(token),
            Ok(Err(reason)) => bail!("{reason}"),
            Err(_) => {
                remote.requests.lock().await.remove(&request_id);
                bail!("{operation} request ended without a control plane decision")
            }
        }
    }

    pub(crate) async fn handle_granted(
        &self,
        request_id: Uuid,
        token: String,
        expires_at_epoch_seconds: u64,
    ) {
        let Some(remote) = &self.remote else {
            return;
        };
        if let Some(request) = remote.requests.lock().await.remove(&request_id) {
            let _ = request.send(Ok(ExpiringToken {
                token,
                expires_at_epoch_seconds,
            }));
        }
    }

    pub(crate) async fn handle_denied(&self, request_id: Uuid, reason: String) {
        let Some(remote) = &self.remote else {
            return;
        };
        if let Some(request) = remote.requests.lock().await.remove(&request_id) {
            let _ = request.send(Err(reason));
        }
    }

    pub(crate) async fn cancel_all(&self) {
        let Some(remote) = &self.remote else {
            return;
        };
        let requests = remote
            .requests
            .lock()
            .await
            .drain()
            .map(|(_, request)| request)
            .collect::<Vec<_>>();
        for request in requests {
            let _ = request.send(Err("Control plane connection closed.".to_owned()));
        }
    }
}

fn current_epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn remote_requests_wait_for_a_scoped_token() {
        let (outbound, mut events) = mpsc::channel(4);
        let client = RepositoryAccessClient::remote(outbound);
        let run_id = Uuid::new_v4();
        let (_, cancel) = watch::channel(false);
        let request = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .request_token(
                        run_id,
                        RepositoryTokenPurpose::SharedSource,
                        "owner",
                        "shared-actions",
                        &cancel,
                    )
                    .await
            })
        };
        let request_id = match events.recv().await.expect("token request") {
            AgentMessage::RepositoryTokenRequest {
                job_id,
                request_id,
                purpose,
                owner,
                repository,
                ..
            } => {
                assert_eq!(job_id, run_id);
                assert_eq!(purpose, RepositoryTokenPurpose::SharedSource);
                assert_eq!(owner, "owner");
                assert_eq!(repository, "shared-actions");
                request_id
            }
            message => panic!("unexpected event: {message:?}"),
        };
        client
            .handle_granted(
                request_id,
                "scoped-installation-token".to_owned(),
                4_102_444_800,
            )
            .await;
        assert_eq!(
            request.await.expect("join request").expect("token"),
            ExpiringToken {
                token: "scoped-installation-token".to_owned(),
                expires_at_epoch_seconds: 4_102_444_800,
            }
        );
    }

    #[tokio::test]
    async fn remote_denials_are_returned_without_tokens() {
        let (outbound, mut events) = mpsc::channel(4);
        let client = RepositoryAccessClient::remote(outbound);
        let run_id = Uuid::new_v4();
        let (_, cancel) = watch::channel(false);
        let request = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .request_token(
                        run_id,
                        RepositoryTokenPurpose::Checkout,
                        "owner",
                        "private",
                        &cancel,
                    )
                    .await
            })
        };
        let request_id = match events.recv().await.expect("token request") {
            AgentMessage::RepositoryTokenRequest {
                request_id,
                purpose,
                ..
            } => {
                assert_eq!(purpose, RepositoryTokenPurpose::Checkout);
                request_id
            }
            message => panic!("unexpected event: {message:?}"),
        };
        client
            .handle_denied(request_id, "sharing policy denied access".to_owned())
            .await;
        let error = request
            .await
            .expect("join request")
            .expect_err("request should fail");
        assert!(error.to_string().contains("sharing policy denied access"));
    }

    #[tokio::test]
    async fn remote_tokens_that_expire_too_soon_are_rejected() {
        let (outbound, mut events) = mpsc::channel(4);
        let client = RepositoryAccessClient::remote(outbound);
        let run_id = Uuid::new_v4();
        let (_, cancel) = watch::channel(false);
        let request = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .request_token(
                        run_id,
                        RepositoryTokenPurpose::Source,
                        "owner",
                        "repository",
                        &cancel,
                    )
                    .await
            })
        };
        let request_id = match events.recv().await.expect("token request") {
            AgentMessage::RepositoryTokenRequest { request_id, .. } => request_id,
            message => panic!("unexpected event: {message:?}"),
        };
        client
            .handle_granted(request_id, "nearly-expired-token".to_owned(), 1)
            .await;
        let error = request
            .await
            .expect("join request")
            .expect_err("near-expiry token should fail closed");
        assert!(error.to_string().contains("expires too soon"));
    }

    #[tokio::test]
    async fn remote_workflow_requests_preserve_exact_read_and_write_scopes() {
        let (outbound, mut events) = mpsc::channel(4);
        let client = RepositoryAccessClient::remote(outbound);
        let run_id = Uuid::new_v4();
        let read_permissions = BTreeSet::from(["contents".to_owned()]);
        let write_permissions = BTreeSet::from(["checks".to_owned()]);
        let (_, cancel) = watch::channel(false);
        let request = {
            let client = client.clone();
            let read_permissions = read_permissions.clone();
            let write_permissions = write_permissions.clone();
            tokio::spawn(async move {
                client
                    .request_workflow_token(run_id, &read_permissions, &write_permissions, &cancel)
                    .await
            })
        };
        let request_id = match events.recv().await.expect("workflow token request") {
            AgentMessage::WorkflowTokenRequest {
                job_id,
                request_id,
                read_permissions: requested_read,
                write_permissions: requested_write,
                ..
            } => {
                assert_eq!(job_id, run_id);
                assert_eq!(requested_read, ["contents"]);
                assert_eq!(requested_write, ["checks"]);
                request_id
            }
            message => panic!("unexpected event: {message:?}"),
        };
        client
            .handle_granted(
                request_id,
                "read-scoped-workflow-token".to_owned(),
                4_102_444_800,
            )
            .await;
        assert_eq!(
            request
                .await
                .expect("join request")
                .expect("workflow token"),
            ExpiringToken {
                token: "read-scoped-workflow-token".to_owned(),
                expires_at_epoch_seconds: 4_102_444_800,
            }
        );
    }
}
