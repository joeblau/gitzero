use anyhow::{Context, Result, bail};
use gitzero_protocol::{AgentMessage, ConcurrencyQueue};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct ConcurrencyClient {
    remote: Option<Arc<RemoteConcurrencyClient>>,
}

struct RemoteConcurrencyClient {
    outbound: mpsc::Sender<AgentMessage>,
    requests: Mutex<HashMap<Uuid, RequestState>>,
}

struct RequestState {
    granted: bool,
    grant: Option<oneshot::Sender<std::result::Result<(), String>>>,
    cancel: watch::Sender<bool>,
}

pub(crate) enum ConcurrencyAcquisition {
    Acquired(ConcurrencyLease),
    Cancelled(String),
}

pub(crate) struct ConcurrencyLease {
    client: ConcurrencyClient,
    run_id: Uuid,
    request_id: Uuid,
    cancel: watch::Receiver<bool>,
    released: bool,
}

impl ConcurrencyClient {
    #[cfg(test)]
    pub(crate) fn local() -> Self {
        Self { remote: None }
    }

    pub(crate) fn remote(outbound: mpsc::Sender<AgentMessage>) -> Self {
        Self {
            remote: Some(Arc::new(RemoteConcurrencyClient {
                outbound,
                requests: Mutex::new(HashMap::new()),
            })),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn acquire(
        &self,
        run_id: Uuid,
        unit_id: String,
        group: String,
        cancel_in_progress: bool,
        queue: ConcurrencyQueue,
        cancel: &watch::Receiver<bool>,
    ) -> Result<ConcurrencyAcquisition> {
        let request_id = Uuid::new_v4();
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let Some(remote) = &self.remote else {
            return Ok(ConcurrencyAcquisition::Acquired(ConcurrencyLease {
                client: self.clone(),
                run_id,
                request_id,
                cancel: cancel_rx,
                released: false,
            }));
        };
        let (grant_tx, mut grant_rx) = oneshot::channel();
        let unit_id = bounded_unit_id(unit_id);
        remote.requests.lock().await.insert(
            request_id,
            RequestState {
                granted: false,
                grant: Some(grant_tx),
                cancel: cancel_tx,
            },
        );
        if remote
            .outbound
            .send(AgentMessage::ConcurrencyAcquire {
                message_id: Uuid::new_v4(),
                job_id: run_id,
                request_id,
                unit_id,
                group,
                cancel_in_progress,
                queue,
            })
            .await
            .is_err()
        {
            remote.requests.lock().await.remove(&request_id);
            bail!("control plane event channel closed while acquiring concurrency");
        }

        let mut outer_cancel = cancel.clone();
        let response = loop {
            if *outer_cancel.borrow() {
                self.abandon(run_id, request_id).await;
                return Ok(ConcurrencyAcquisition::Cancelled(
                    "The parent run was cancelled while waiting for concurrency.".to_owned(),
                ));
            }
            tokio::select! {
                response = &mut grant_rx => break response,
                changed = outer_cancel.changed() => {
                    if changed.is_err() {
                        break grant_rx.await;
                    }
                }
            }
        };
        match response {
            Ok(Ok(())) => Ok(ConcurrencyAcquisition::Acquired(ConcurrencyLease {
                client: self.clone(),
                run_id,
                request_id,
                cancel: cancel_rx,
                released: false,
            })),
            Ok(Err(reason)) => Ok(ConcurrencyAcquisition::Cancelled(reason)),
            Err(_) => {
                remote.requests.lock().await.remove(&request_id);
                bail!("concurrency request ended without a control plane decision")
            }
        }
    }

    pub(crate) async fn handle_granted(&self, request_id: Uuid) {
        let Some(remote) = &self.remote else {
            return;
        };
        let grant = {
            let mut requests = remote.requests.lock().await;
            let Some(request) = requests.get_mut(&request_id) else {
                return;
            };
            request.granted = true;
            request.grant.take()
        };
        if let Some(grant) = grant {
            let _ = grant.send(Ok(()));
        }
    }

    pub(crate) async fn handle_cancelled(&self, request_id: Uuid, reason: String) {
        let Some(remote) = &self.remote else {
            return;
        };
        let pending = {
            let mut requests = remote.requests.lock().await;
            if let Some(request) = requests.get_mut(&request_id)
                && request.granted
            {
                request.cancel.send_replace(true);
                return;
            }
            requests.remove(&request_id)
        };
        if let Some(mut request) = pending
            && let Some(grant) = request.grant.take()
        {
            let _ = grant.send(Err(reason));
        }
    }

    pub(crate) async fn cancel_all(&self) {
        let Some(remote) = &self.remote else {
            return;
        };
        let requests = {
            let mut requests = remote.requests.lock().await;
            requests
                .drain()
                .map(|(_, request)| request)
                .collect::<Vec<_>>()
        };
        for mut request in requests {
            request.cancel.send_replace(true);
            if let Some(grant) = request.grant.take() {
                let _ = grant.send(Err("Control plane connection closed.".to_owned()));
            }
        }
    }

    async fn abandon(&self, run_id: Uuid, request_id: Uuid) {
        let Some(remote) = &self.remote else {
            return;
        };
        remote.requests.lock().await.remove(&request_id);
        let _ = remote
            .outbound
            .send(AgentMessage::ConcurrencyRelease {
                message_id: Uuid::new_v4(),
                job_id: run_id,
                request_id,
            })
            .await;
    }
}

fn bounded_unit_id(mut value: String) -> String {
    const MAX_BYTES: usize = 512;
    if value.len() <= MAX_BYTES {
        return value;
    }
    let mut boundary = MAX_BYTES - '…'.len_utf8();
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.truncate(boundary);
    value.push('…');
    value
}

impl ConcurrencyLease {
    pub(crate) fn cancellation(&self) -> watch::Receiver<bool> {
        self.cancel.clone()
    }

    pub(crate) async fn release(mut self) -> Result<()> {
        self.release_inner().await
    }

    async fn release_inner(&mut self) -> Result<()> {
        if self.released {
            return Ok(());
        }
        self.released = true;
        let Some(remote) = &self.client.remote else {
            return Ok(());
        };
        remote.requests.lock().await.remove(&self.request_id);
        remote
            .outbound
            .send(AgentMessage::ConcurrencyRelease {
                message_id: Uuid::new_v4(),
                job_id: self.run_id,
                request_id: self.request_id,
            })
            .await
            .context("control plane event channel closed while releasing concurrency")
    }
}

impl Drop for ConcurrencyLease {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let Some(remote) = &self.client.remote else {
            return;
        };
        if let Ok(mut requests) = remote.requests.try_lock() {
            requests.remove(&self.request_id);
        }
        let _ = remote.outbound.try_send(AgentMessage::ConcurrencyRelease {
            message_id: Uuid::new_v4(),
            job_id: self.run_id,
            request_id: self.request_id,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn remote_leases_wait_for_grant_and_forward_cancellation_and_release() {
        let (outbound, mut events) = mpsc::channel(8);
        let client = ConcurrencyClient::remote(outbound);
        let run_id = Uuid::new_v4();
        let (_, cancel) = watch::channel(false);
        let acquisition = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .acquire(
                        run_id,
                        "workflow / job".to_owned(),
                        "deploy".to_owned(),
                        true,
                        ConcurrencyQueue::Single,
                        &cancel,
                    )
                    .await
            })
        };
        let request_id = match events.recv().await.expect("acquire event") {
            AgentMessage::ConcurrencyAcquire {
                job_id,
                request_id,
                cancel_in_progress,
                ..
            } => {
                assert_eq!(job_id, run_id);
                assert!(cancel_in_progress);
                request_id
            }
            message => panic!("unexpected event: {message:?}"),
        };
        client.handle_granted(request_id).await;
        let ConcurrencyAcquisition::Acquired(lease) = acquisition
            .await
            .expect("join acquisition")
            .expect("acquire")
        else {
            panic!("expected acquired lease");
        };
        let mut cancelled = lease.cancellation();
        client
            .handle_cancelled(request_id, "superseded".to_owned())
            .await;
        cancelled.changed().await.expect("cancellation update");
        assert!(*cancelled.borrow());
        lease.release().await.expect("release");
        assert!(matches!(
            events.recv().await,
            Some(AgentMessage::ConcurrencyRelease {
                job_id,
                request_id: released,
                ..
            }) if job_id == run_id && released == request_id
        ));
    }

    #[test]
    fn concurrency_unit_ids_are_bounded_on_utf8_boundaries() {
        let bounded = bounded_unit_id(format!("scope:{}", "🧪".repeat(200)));
        assert!(bounded.len() <= 512);
        assert!(bounded.ends_with('…'));
    }
}
