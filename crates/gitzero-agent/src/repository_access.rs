use anyhow::{Result, bail};
use gitzero_protocol::AgentMessage;
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct RepositoryAccessClient {
    remote: Option<Arc<RemoteRepositoryAccessClient>>,
}

struct RemoteRepositoryAccessClient {
    outbound: mpsc::Sender<AgentMessage>,
    requests: Mutex<HashMap<Uuid, oneshot::Sender<std::result::Result<String, String>>>>,
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
        owner: &str,
        repository: &str,
        cancel: &watch::Receiver<bool>,
    ) -> Result<String> {
        let request_id = Uuid::new_v4();
        self.request(
            request_id,
            AgentMessage::RepositoryTokenRequest {
                message_id: Uuid::new_v4(),
                job_id: run_id,
                request_id,
                owner: owner.to_owned(),
                repository: repository.to_owned(),
            },
            "private shared repository access",
            cancel,
        )
        .await
    }

    pub(crate) async fn request_workflow_token(
        &self,
        run_id: Uuid,
        permissions: &BTreeSet<String>,
        cancel: &watch::Receiver<bool>,
    ) -> Result<String> {
        if permissions.is_empty() {
            bail!("workflow token request must contain at least one read permission");
        }
        let request_id = Uuid::new_v4();
        self.request(
            request_id,
            AgentMessage::WorkflowTokenRequest {
                message_id: Uuid::new_v4(),
                job_id: run_id,
                request_id,
                permissions: permissions.iter().cloned().collect(),
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
    ) -> Result<String> {
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
            Ok(Ok(token)) if !token.is_empty() => Ok(token),
            Ok(Ok(_)) => bail!("control plane returned an empty token for {operation}"),
            Ok(Err(reason)) => bail!("{reason}"),
            Err(_) => {
                remote.requests.lock().await.remove(&request_id);
                bail!("{operation} request ended without a control plane decision")
            }
        }
    }

    pub(crate) async fn handle_granted(&self, request_id: Uuid, token: String) {
        let Some(remote) = &self.remote else {
            return;
        };
        if let Some(request) = remote.requests.lock().await.remove(&request_id) {
            let _ = request.send(Ok(token));
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
                    .request_token(run_id, "owner", "shared-actions", &cancel)
                    .await
            })
        };
        let request_id = match events.recv().await.expect("token request") {
            AgentMessage::RepositoryTokenRequest {
                job_id,
                request_id,
                owner,
                repository,
                ..
            } => {
                assert_eq!(job_id, run_id);
                assert_eq!(owner, "owner");
                assert_eq!(repository, "shared-actions");
                request_id
            }
            message => panic!("unexpected event: {message:?}"),
        };
        client
            .handle_granted(request_id, "scoped-installation-token".to_owned())
            .await;
        assert_eq!(
            request.await.expect("join request").expect("token"),
            "scoped-installation-token"
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
                    .request_token(run_id, "owner", "private", &cancel)
                    .await
            })
        };
        let request_id = match events.recv().await.expect("token request") {
            AgentMessage::RepositoryTokenRequest { request_id, .. } => request_id,
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
    async fn remote_workflow_requests_preserve_the_exact_read_scope() {
        let (outbound, mut events) = mpsc::channel(4);
        let client = RepositoryAccessClient::remote(outbound);
        let run_id = Uuid::new_v4();
        let permissions = BTreeSet::from(["checks".to_owned(), "contents".to_owned()]);
        let (_, cancel) = watch::channel(false);
        let request = {
            let client = client.clone();
            let permissions = permissions.clone();
            tokio::spawn(async move {
                client
                    .request_workflow_token(run_id, &permissions, &cancel)
                    .await
            })
        };
        let request_id = match events.recv().await.expect("workflow token request") {
            AgentMessage::WorkflowTokenRequest {
                job_id,
                request_id,
                permissions: requested,
                ..
            } => {
                assert_eq!(job_id, run_id);
                assert_eq!(requested, ["checks", "contents"]);
                request_id
            }
            message => panic!("unexpected event: {message:?}"),
        };
        client
            .handle_granted(request_id, "read-scoped-workflow-token".to_owned())
            .await;
        assert_eq!(
            request
                .await
                .expect("join request")
                .expect("workflow token"),
            "read-scoped-workflow-token"
        );
    }
}
