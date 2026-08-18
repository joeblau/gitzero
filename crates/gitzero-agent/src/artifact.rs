use anyhow::{Context, Result, bail};
use axum::{
    Router,
    body::Body,
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{Mutex, oneshot},
    task::JoinHandle,
};
use tokio_util::io::ReaderStream;
use uuid::Uuid;

const ARTIFACT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_RPC_BODY_BYTES: u64 = 64 * 1024;
const MAX_BLOCK_LIST_BYTES: u64 = 1024 * 1024;
const MAX_ARTIFACT_NAME_BYTES: usize = 255;
#[cfg(test)]
const ARTIFACT_SERVICE: &str = "github.actions.results.api.v1.ArtifactService";

#[derive(Clone, Copy, Debug)]
pub struct ArtifactLimits {
    pub maximum_bytes: u64,
    pub maximum_entry_bytes: u64,
}

pub struct ArtifactService {
    base_url: String,
    runtime_token: String,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<std::io::Result<()>>>,
}

impl ArtifactService {
    pub async fn start(
        root: &Path,
        runtime_token: &str,
        workflow_run_backend_id: &str,
        workflow_job_run_backend_id: &str,
        limits: ArtifactLimits,
    ) -> Result<Self> {
        if limits.maximum_entry_bytes == 0 || limits.maximum_bytes < limits.maximum_entry_bytes {
            bail!("workflow artifact limits are invalid");
        }
        fs::create_dir_all(root)
            .await
            .context("create workflow artifact directory")?;
        let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .await
            .context("bind loopback workflow artifact service")?;
        let address = listener
            .local_addr()
            .context("read workflow artifact service address")?;
        let base_url = format!("http://{address}/");
        let state = ArtifactState {
            root: root.to_owned(),
            base_url: base_url.clone(),
            runtime_token: runtime_token.to_owned(),
            workflow_run_backend_id: workflow_run_backend_id.to_owned(),
            workflow_job_run_backend_id: workflow_job_run_backend_id.to_owned(),
            limits,
            next_id: Arc::new(AtomicU64::new(1)),
            artifacts: Arc::new(Mutex::new(BTreeMap::new())),
        };
        let router = Router::new()
            .route(
                "/twirp/github.actions.results.api.v1.ArtifactService/{method}",
                post(twirp),
            )
            .route("/_gitzero/artifacts/{id}/blob", put(upload_blob))
            .route(
                "/_gitzero/artifacts/{id}/{filename}",
                get(download_blob).head(head_blob),
            )
            .with_state(state);
        let (shutdown, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        Ok(Self {
            base_url,
            runtime_token: runtime_token.to_owned(),
            shutdown: Some(shutdown),
            task: Some(task),
        })
    }

    pub fn environment(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            ("ACTIONS_RESULTS_URL".to_owned(), self.base_url.clone()),
            ("ACTIONS_RUNTIME_URL".to_owned(), self.base_url.clone()),
            (
                "ACTIONS_RUNTIME_TOKEN".to_owned(),
                self.runtime_token.clone(),
            ),
        ])
    }

    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(mut task) = self.task.take() {
            match tokio::time::timeout(ARTIFACT_SHUTDOWN_TIMEOUT, &mut task).await {
                Ok(result) => result
                    .context("join workflow artifact service")?
                    .context("serve workflow artifact requests")?,
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                }
            }
        }
        Ok(())
    }
}

impl Drop for ArtifactService {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Clone)]
struct ArtifactState {
    root: PathBuf,
    base_url: String,
    runtime_token: String,
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
    limits: ArtifactLimits,
    next_id: Arc<AtomicU64>,
    artifacts: Arc<Mutex<BTreeMap<u64, ArtifactRecord>>>,
}

#[derive(Clone)]
struct ArtifactRecord {
    id: u64,
    name: String,
    mime_type: String,
    directory: PathBuf,
    blob_path: PathBuf,
    blocks: BTreeMap<String, Block>,
    blob_committed: bool,
    finalized: bool,
    size: u64,
    digest: Option<String>,
}

#[derive(Clone)]
struct Block {
    path: PathBuf,
    size: u64,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ProtoString {
    String(String),
    Wrapped { value: String },
}

impl ProtoString {
    fn into_inner(self) -> String {
        match self {
            Self::String(value) | Self::Wrapped { value } => value,
        }
    }
}

#[derive(Deserialize)]
struct CreateArtifactRequest {
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
    name: String,
    #[serde(default)]
    mime_type: Option<ProtoString>,
    #[serde(default)]
    version: i32,
}

#[derive(Deserialize)]
struct FinalizeArtifactRequest {
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
    name: String,
    size: JsonValue,
    #[serde(default)]
    hash: Option<ProtoString>,
}

#[derive(Deserialize)]
struct ListArtifactsRequest {
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
    #[serde(default)]
    name_filter: Option<ProtoString>,
    #[serde(default)]
    id_filter: Option<JsonValue>,
}

#[derive(Deserialize)]
struct NamedArtifactRequest {
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
    name: String,
}

#[derive(Deserialize)]
struct SignedQuery {
    sig: String,
    #[serde(default)]
    comp: Option<String>,
    #[serde(default)]
    blockid: Option<String>,
}

#[derive(Debug)]
struct ArtifactHttpError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ArtifactHttpError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_argument", message)
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            error.to_string(),
        )
    }
}

impl IntoResponse for ArtifactHttpError {
    fn into_response(self) -> Response {
        (
            self.status,
            axum::Json(json!({ "code": self.code, "msg": self.message })),
        )
            .into_response()
    }
}

type ArtifactResponse<T> = std::result::Result<T, ArtifactHttpError>;

async fn twirp(
    State(state): State<ArtifactState>,
    AxumPath(method): AxumPath<String>,
    headers: HeaderMap,
    body: Body,
) -> ArtifactResponse<Response> {
    authorize(&state, &headers)?;
    let body = read_body(body, MAX_RPC_BODY_BYTES).await?;
    match method.as_str() {
        "CreateArtifact" => {
            let request: CreateArtifactRequest = parse_json(&body)?;
            create_artifact(&state, request).await
        }
        "FinalizeArtifact" => {
            let request: FinalizeArtifactRequest = parse_json(&body)?;
            finalize_artifact(&state, request).await
        }
        "ListArtifacts" => {
            let request: ListArtifactsRequest = parse_json(&body)?;
            list_artifacts(&state, request).await
        }
        "GetSignedArtifactURL" => {
            let request: NamedArtifactRequest = parse_json(&body)?;
            signed_artifact_url(&state, request).await
        }
        "DeleteArtifact" => {
            let request: NamedArtifactRequest = parse_json(&body)?;
            delete_artifact(&state, request).await
        }
        _ => Err(ArtifactHttpError::new(
            StatusCode::NOT_FOUND,
            "bad_route",
            "artifact method is not supported",
        )),
    }
}

async fn create_artifact(
    state: &ArtifactState,
    request: CreateArtifactRequest,
) -> ArtifactResponse<Response> {
    validate_backend_ids(
        state,
        &request.workflow_run_backend_id,
        &request.workflow_job_run_backend_id,
    )?;
    validate_artifact_name(&request.name)?;
    if request.version < 4 {
        return Err(ArtifactHttpError::bad_request(
            "artifact protocol version is not supported",
        ));
    }
    let mut artifacts = state.artifacts.lock().await;
    if artifacts
        .values()
        .any(|artifact| artifact.name == request.name)
    {
        return Err(ArtifactHttpError::new(
            StatusCode::CONFLICT,
            "already_exists",
            "an artifact with this name already exists on the workflow run",
        ));
    }
    let id = state.next_id.fetch_add(1, Ordering::Relaxed);
    let directory = state.root.join(id.to_string());
    fs::create_dir_all(directory.join("blocks"))
        .await
        .map_err(ArtifactHttpError::internal)?;
    artifacts.insert(
        id,
        ArtifactRecord {
            id,
            name: request.name,
            mime_type: request
                .mime_type
                .map(ProtoString::into_inner)
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "application/zip".to_owned()),
            blob_path: directory.join("blob"),
            directory,
            blocks: BTreeMap::new(),
            blob_committed: false,
            finalized: false,
            size: 0,
            digest: None,
        },
    );
    let signed_upload_url = format!(
        "{}_gitzero/artifacts/{id}/blob?sig={}",
        state.base_url, state.runtime_token
    );
    Ok(axum::Json(json!({
        "ok": true,
        "signed_upload_url": signed_upload_url,
    }))
    .into_response())
}

async fn upload_blob(
    State(state): State<ArtifactState>,
    AxumPath(id): AxumPath<u64>,
    Query(query): Query<SignedQuery>,
    headers: HeaderMap,
    body: Body,
) -> ArtifactResponse<Response> {
    authorize_signed(&state, &query.sig)?;
    match query.comp.as_deref() {
        Some("block") => {
            let block_id = query
                .blockid
                .as_deref()
                .ok_or_else(|| ArtifactHttpError::bad_request("blockid is required"))?;
            stage_block(&state, id, block_id, &headers, body).await?;
        }
        Some("blocklist") => {
            let bytes = read_body(body, MAX_BLOCK_LIST_BYTES).await?;
            commit_block_list(&state, id, &bytes).await?;
        }
        Some(_) => {
            return Err(ArtifactHttpError::bad_request(
                "blob operation is not supported",
            ));
        }
        None => {
            upload_single_blob(&state, id, &headers, body).await?;
        }
    }
    Ok(azure_created_response())
}

async fn stage_block(
    state: &ArtifactState,
    id: u64,
    block_id: &str,
    headers: &HeaderMap,
    body: Body,
) -> ArtifactResponse<()> {
    if block_id.is_empty() || block_id.len() > 256 {
        return Err(ArtifactHttpError::bad_request("block ID is invalid"));
    }
    reject_large_content_length(headers, state.limits.maximum_entry_bytes)?;
    let (directory, finalized) = {
        let artifacts = state.artifacts.lock().await;
        let artifact = artifacts.get(&id).ok_or_else(artifact_not_found)?;
        (artifact.directory.clone(), artifact.finalized)
    };
    if finalized {
        return Err(ArtifactHttpError::new(
            StatusCode::CONFLICT,
            "failed_precondition",
            "artifact is already finalized",
        ));
    }
    let digest = hex_digest(block_id.as_bytes());
    let temporary = directory
        .join("blocks")
        .join(format!(".{digest}.{}.tmp", Uuid::new_v4().simple()));
    let size = write_body(body, &temporary, state.limits.maximum_entry_bytes).await?;
    let final_path = directory.join("blocks").join(digest);
    let mut artifacts = state.artifacts.lock().await;
    let old_size = artifacts
        .get(&id)
        .and_then(|artifact| artifact.blocks.get(block_id))
        .map_or(0, |block| block.size);
    let projected_artifact =
        artifact_logical_size(artifacts.get(&id).ok_or_else(artifact_not_found)?)
            .saturating_sub(old_size)
            .saturating_add(size);
    let projected_total = total_logical_size(&artifacts)
        .saturating_sub(old_size)
        .saturating_add(size);
    if projected_artifact > state.limits.maximum_entry_bytes
        || projected_total > state.limits.maximum_bytes
    {
        drop(artifacts);
        let _ = fs::remove_file(&temporary).await;
        return Err(ArtifactHttpError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "resource_exhausted",
            "workflow artifact storage limit exceeded",
        ));
    }
    fs::rename(&temporary, &final_path)
        .await
        .map_err(ArtifactHttpError::internal)?;
    let artifact = artifacts.get_mut(&id).ok_or_else(artifact_not_found)?;
    artifact.blocks.insert(
        block_id.to_owned(),
        Block {
            path: final_path,
            size,
        },
    );
    Ok(())
}

async fn commit_block_list(state: &ArtifactState, id: u64, xml: &[u8]) -> ArtifactResponse<()> {
    let order = parse_block_list(xml)?;
    let (blocks, blob_path, directory) = {
        let artifacts = state.artifacts.lock().await;
        let artifact = artifacts.get(&id).ok_or_else(artifact_not_found)?;
        if artifact.finalized {
            return Err(ArtifactHttpError::new(
                StatusCode::CONFLICT,
                "failed_precondition",
                "artifact is already finalized",
            ));
        }
        let blocks = order
            .iter()
            .map(|block_id| {
                artifact.blocks.get(block_id).cloned().ok_or_else(|| {
                    ArtifactHttpError::bad_request("block list refers to a missing block")
                })
            })
            .collect::<ArtifactResponse<Vec<_>>>()?;
        (
            blocks,
            artifact.blob_path.clone(),
            artifact.directory.clone(),
        )
    };
    let size = blocks
        .iter()
        .try_fold(0_u64, |total, block| total.checked_add(block.size))
        .ok_or_else(|| ArtifactHttpError::bad_request("artifact size overflow"))?;
    if size > state.limits.maximum_entry_bytes {
        return Err(ArtifactHttpError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "resource_exhausted",
            "artifact exceeds the configured per-entry limit",
        ));
    }
    let temporary = directory.join(format!(".blob.{}.tmp", Uuid::new_v4().simple()));
    let mut output = fs::File::create(&temporary)
        .await
        .map_err(ArtifactHttpError::internal)?;
    for block in &blocks {
        let mut input = fs::File::open(&block.path)
            .await
            .map_err(ArtifactHttpError::internal)?;
        tokio::io::copy(&mut input, &mut output)
            .await
            .map_err(ArtifactHttpError::internal)?;
    }
    output.flush().await.map_err(ArtifactHttpError::internal)?;
    fs::rename(&temporary, &blob_path)
        .await
        .map_err(ArtifactHttpError::internal)?;
    let mut artifacts = state.artifacts.lock().await;
    let artifact = artifacts.get_mut(&id).ok_or_else(artifact_not_found)?;
    artifact.blob_committed = true;
    artifact.size = size;
    artifact.blocks.clear();
    drop(artifacts);
    let _ = fs::remove_dir_all(directory.join("blocks")).await;
    Ok(())
}

async fn upload_single_blob(
    state: &ArtifactState,
    id: u64,
    headers: &HeaderMap,
    body: Body,
) -> ArtifactResponse<()> {
    reject_large_content_length(headers, state.limits.maximum_entry_bytes)?;
    let (blob_path, directory, old_size) = {
        let artifacts = state.artifacts.lock().await;
        let artifact = artifacts.get(&id).ok_or_else(artifact_not_found)?;
        if artifact.finalized {
            return Err(ArtifactHttpError::new(
                StatusCode::CONFLICT,
                "failed_precondition",
                "artifact is already finalized",
            ));
        }
        (
            artifact.blob_path.clone(),
            artifact.directory.clone(),
            artifact_logical_size(artifact),
        )
    };
    let temporary = directory.join(format!(".blob.{}.tmp", Uuid::new_v4().simple()));
    let size = write_body(body, &temporary, state.limits.maximum_entry_bytes).await?;
    let mut artifacts = state.artifacts.lock().await;
    let projected = total_logical_size(&artifacts)
        .saturating_sub(old_size)
        .saturating_add(size);
    if projected > state.limits.maximum_bytes {
        drop(artifacts);
        let _ = fs::remove_file(&temporary).await;
        return Err(ArtifactHttpError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "resource_exhausted",
            "workflow artifact storage limit exceeded",
        ));
    }
    fs::rename(&temporary, &blob_path)
        .await
        .map_err(ArtifactHttpError::internal)?;
    let artifact = artifacts.get_mut(&id).ok_or_else(artifact_not_found)?;
    artifact.blob_committed = true;
    artifact.size = size;
    artifact.blocks.clear();
    Ok(())
}

async fn finalize_artifact(
    state: &ArtifactState,
    request: FinalizeArtifactRequest,
) -> ArtifactResponse<Response> {
    validate_backend_ids(
        state,
        &request.workflow_run_backend_id,
        &request.workflow_job_run_backend_id,
    )?;
    let expected_size = json_u64(&request.size)?;
    let expected_hash = request.hash.map(ProtoString::into_inner);
    let (id, blob_path, committed, finalized, stored_size, stored_digest) = {
        let artifacts = state.artifacts.lock().await;
        let artifact = artifacts
            .values()
            .find(|artifact| artifact.name == request.name)
            .ok_or_else(artifact_not_found)?;
        (
            artifact.id,
            artifact.blob_path.clone(),
            artifact.blob_committed,
            artifact.finalized,
            artifact.size,
            artifact.digest.clone(),
        )
    };
    if finalized {
        if expected_size == stored_size
            && expected_hash
                .as_deref()
                .is_none_or(|hash| Some(hash) == stored_digest.as_deref())
        {
            return Ok(
                axum::Json(json!({ "ok": true, "artifact_id": id.to_string() })).into_response(),
            );
        }
        return Err(ArtifactHttpError::new(
            StatusCode::CONFLICT,
            "already_exists",
            "artifact is already finalized",
        ));
    }
    if !committed || expected_size != stored_size {
        return Err(ArtifactHttpError::bad_request(
            "final artifact size does not match the committed blob",
        ));
    }
    let digest = format!("sha256:{}", hash_file(&blob_path).await?);
    if expected_hash
        .as_deref()
        .is_some_and(|expected| expected != digest)
    {
        return Err(ArtifactHttpError::bad_request(
            "final artifact digest does not match the committed blob",
        ));
    }
    let mut artifacts = state.artifacts.lock().await;
    let artifact = artifacts.get_mut(&id).ok_or_else(artifact_not_found)?;
    artifact.finalized = true;
    artifact.digest = Some(digest);
    Ok(axum::Json(json!({ "ok": true, "artifact_id": id.to_string() })).into_response())
}

async fn list_artifacts(
    state: &ArtifactState,
    request: ListArtifactsRequest,
) -> ArtifactResponse<Response> {
    validate_backend_ids(
        state,
        &request.workflow_run_backend_id,
        &request.workflow_job_run_backend_id,
    )?;
    let name_filter = request.name_filter.map(ProtoString::into_inner);
    let id_filter = request.id_filter.as_ref().map(json_u64).transpose()?;
    let artifacts = state.artifacts.lock().await;
    let listed = artifacts
        .values()
        .rev()
        .filter(|artifact| artifact.finalized)
        .filter(|artifact| {
            name_filter
                .as_ref()
                .is_none_or(|name| artifact.name == *name)
        })
        .filter(|artifact| id_filter.is_none_or(|id| artifact.id == id))
        .map(|artifact| {
            json!({
                "workflow_run_backend_id": state.workflow_run_backend_id,
                "workflow_job_run_backend_id": state.workflow_job_run_backend_id,
                "database_id": artifact.id.to_string(),
                "name": artifact.name,
                "size": artifact.size.to_string(),
                "digest": artifact.digest,
            })
        })
        .collect::<Vec<_>>();
    Ok(axum::Json(json!({ "artifacts": listed })).into_response())
}

async fn signed_artifact_url(
    state: &ArtifactState,
    request: NamedArtifactRequest,
) -> ArtifactResponse<Response> {
    validate_backend_ids(
        state,
        &request.workflow_run_backend_id,
        &request.workflow_job_run_backend_id,
    )?;
    let artifacts = state.artifacts.lock().await;
    let artifact = artifacts
        .values()
        .find(|artifact| artifact.finalized && artifact.name == request.name)
        .ok_or_else(artifact_not_found)?;
    let filename = if is_zip_mime(&artifact.mime_type) {
        "artifact.zip"
    } else {
        "artifact.bin"
    };
    let signed_url = format!(
        "{}_gitzero/artifacts/{}/{filename}?sig={}",
        state.base_url, artifact.id, state.runtime_token
    );
    Ok(axum::Json(json!({ "signed_url": signed_url })).into_response())
}

async fn delete_artifact(
    state: &ArtifactState,
    request: NamedArtifactRequest,
) -> ArtifactResponse<Response> {
    validate_backend_ids(
        state,
        &request.workflow_run_backend_id,
        &request.workflow_job_run_backend_id,
    )?;
    let (id, directory) = {
        let mut artifacts = state.artifacts.lock().await;
        let id = artifacts
            .values()
            .filter(|artifact| artifact.name == request.name)
            .map(|artifact| artifact.id)
            .max()
            .ok_or_else(artifact_not_found)?;
        let artifact = artifacts.remove(&id).ok_or_else(artifact_not_found)?;
        (id, artifact.directory)
    };
    match fs::remove_dir_all(directory).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(ArtifactHttpError::internal(error)),
    }
    Ok(axum::Json(json!({ "ok": true, "artifact_id": id.to_string() })).into_response())
}

async fn download_blob(
    State(state): State<ArtifactState>,
    AxumPath((id, _filename)): AxumPath<(u64, String)>,
    Query(query): Query<SignedQuery>,
) -> ArtifactResponse<Response> {
    authorize_signed(&state, &query.sig)?;
    let (path, size, mime_type, name) = download_metadata(&state, id).await?;
    let file = fs::File::open(path)
        .await
        .map_err(ArtifactHttpError::internal)?;
    let mut response = Response::new(Body::from_stream(ReaderStream::new(file)));
    set_download_headers(response.headers_mut(), size, &mime_type, &name)?;
    Ok(response)
}

async fn head_blob(
    State(state): State<ArtifactState>,
    AxumPath((id, _filename)): AxumPath<(u64, String)>,
    Query(query): Query<SignedQuery>,
) -> ArtifactResponse<Response> {
    authorize_signed(&state, &query.sig)?;
    let (_path, size, mime_type, name) = download_metadata(&state, id).await?;
    let mut response = StatusCode::OK.into_response();
    set_download_headers(response.headers_mut(), size, &mime_type, &name)?;
    Ok(response)
}

async fn download_metadata(
    state: &ArtifactState,
    id: u64,
) -> ArtifactResponse<(PathBuf, u64, String, String)> {
    let artifacts = state.artifacts.lock().await;
    let artifact = artifacts.get(&id).ok_or_else(artifact_not_found)?;
    if !artifact.finalized {
        return Err(artifact_not_found());
    }
    Ok((
        artifact.blob_path.clone(),
        artifact.size,
        artifact.mime_type.clone(),
        artifact.name.clone(),
    ))
}

fn set_download_headers(
    headers: &mut HeaderMap,
    size: u64,
    mime_type: &str,
    name: &str,
) -> ArtifactResponse<()> {
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&size.to_string()).map_err(ArtifactHttpError::internal)?,
    );
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(mime_type).map_err(ArtifactHttpError::internal)?,
    );
    let safe_name = name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"{safe_name}\""))
            .map_err(ArtifactHttpError::internal)?,
    );
    Ok(())
}

fn authorize(state: &ArtifactState, headers: &HeaderMap) -> ArtifactResponse<()> {
    let expected = format!("Bearer {}", state.runtime_token);
    if headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        == Some(expected.as_str())
    {
        Ok(())
    } else {
        Err(ArtifactHttpError::new(
            StatusCode::UNAUTHORIZED,
            "unauthenticated",
            "artifact request is not authorized",
        ))
    }
}

fn authorize_signed(state: &ArtifactState, signature: &str) -> ArtifactResponse<()> {
    if signature == state.runtime_token {
        Ok(())
    } else {
        Err(ArtifactHttpError::new(
            StatusCode::UNAUTHORIZED,
            "unauthenticated",
            "artifact URL signature is invalid",
        ))
    }
}

fn validate_backend_ids(state: &ArtifactState, run_id: &str, job_id: &str) -> ArtifactResponse<()> {
    if run_id == state.workflow_run_backend_id && job_id == state.workflow_job_run_backend_id {
        Ok(())
    } else {
        Err(ArtifactHttpError::new(
            StatusCode::FORBIDDEN,
            "permission_denied",
            "artifact backend IDs are outside this run",
        ))
    }
}

fn validate_artifact_name(name: &str) -> ArtifactResponse<()> {
    if name.is_empty() || name.len() > MAX_ARTIFACT_NAME_BYTES || name.chars().any(char::is_control)
    {
        return Err(ArtifactHttpError::bad_request(
            "artifact name is empty, too large, or contains control characters",
        ));
    }
    Ok(())
}

fn reject_large_content_length(headers: &HeaderMap, maximum: u64) -> ArtifactResponse<()> {
    if headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|size| size > maximum)
    {
        return Err(ArtifactHttpError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "resource_exhausted",
            "artifact upload exceeds the configured limit",
        ));
    }
    Ok(())
}

async fn read_body(body: Body, maximum: u64) -> ArtifactResponse<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(ArtifactHttpError::internal)?;
        if (bytes.len() as u64).saturating_add(chunk.len() as u64) > maximum {
            return Err(ArtifactHttpError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "resource_exhausted",
                "artifact request body exceeds the configured limit",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

async fn write_body(body: Body, path: &Path, maximum: u64) -> ArtifactResponse<u64> {
    let mut file = fs::File::create(path)
        .await
        .map_err(ArtifactHttpError::internal)?;
    let mut size = 0_u64;
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(ArtifactHttpError::internal)?;
        size = size.checked_add(chunk.len() as u64).ok_or_else(|| {
            ArtifactHttpError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "resource_exhausted",
                "artifact upload size overflow",
            )
        })?;
        if size > maximum {
            drop(file);
            let _ = fs::remove_file(path).await;
            return Err(ArtifactHttpError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "resource_exhausted",
                "artifact upload exceeds the configured limit",
            ));
        }
        file.write_all(&chunk)
            .await
            .map_err(ArtifactHttpError::internal)?;
    }
    file.flush().await.map_err(ArtifactHttpError::internal)?;
    Ok(size)
}

fn parse_json<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> ArtifactResponse<T> {
    serde_json::from_slice(bytes)
        .map_err(|error| ArtifactHttpError::bad_request(format!("invalid JSON request: {error}")))
}

fn parse_block_list(xml: &[u8]) -> ArtifactResponse<Vec<String>> {
    let xml = std::str::from_utf8(xml)
        .map_err(|_| ArtifactHttpError::bad_request("block list is not UTF-8"))?;
    let mut remaining = xml;
    let mut blocks = Vec::new();
    loop {
        let next = ["Latest", "Uncommitted", "Committed"]
            .into_iter()
            .filter_map(|tag| {
                remaining
                    .find(&format!("<{tag}>"))
                    .map(|index| (index, tag))
            })
            .min_by_key(|(index, _)| *index);
        let Some((index, tag)) = next else {
            break;
        };
        let value_start = index + tag.len() + 2;
        let closing = format!("</{tag}>");
        let value_end = remaining[value_start..]
            .find(&closing)
            .map(|offset| value_start + offset)
            .ok_or_else(|| ArtifactHttpError::bad_request("block list XML is malformed"))?;
        let value = &remaining[value_start..value_end];
        if value.is_empty() || value.len() > 256 {
            return Err(ArtifactHttpError::bad_request(
                "block list contains an invalid block ID",
            ));
        }
        blocks.push(value.to_owned());
        remaining = &remaining[value_end + closing.len()..];
    }
    if blocks.is_empty() {
        return Err(ArtifactHttpError::bad_request("block list is empty"));
    }
    Ok(blocks)
}

fn json_u64(value: &JsonValue) -> ArtifactResponse<u64> {
    match value {
        JsonValue::String(value) => value
            .parse()
            .map_err(|_| ArtifactHttpError::bad_request("integer field is invalid")),
        JsonValue::Number(value) => value
            .as_u64()
            .ok_or_else(|| ArtifactHttpError::bad_request("integer field is invalid")),
        JsonValue::Object(value) => value
            .get("value")
            .ok_or_else(|| ArtifactHttpError::bad_request("integer wrapper is invalid"))
            .and_then(json_u64),
        _ => Err(ArtifactHttpError::bad_request("integer field is invalid")),
    }
}

fn artifact_logical_size(artifact: &ArtifactRecord) -> u64 {
    if artifact.blob_committed {
        artifact.size
    } else {
        artifact
            .blocks
            .values()
            .fold(0_u64, |total, block| total.saturating_add(block.size))
    }
}

fn total_logical_size(artifacts: &BTreeMap<u64, ArtifactRecord>) -> u64 {
    artifacts.values().fold(0_u64, |total, artifact| {
        total.saturating_add(artifact_logical_size(artifact))
    })
}

async fn hash_file(path: &Path) -> ArtifactResponse<String> {
    let mut file = fs::File::open(path)
        .await
        .map_err(ArtifactHttpError::internal)?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(ArtifactHttpError::internal)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn hex_digest(value: &[u8]) -> String {
    Sha256::digest(value)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn is_zip_mime(value: &str) -> bool {
    matches!(
        value.split(';').next().map(str::trim),
        Some("application/zip" | "application/x-zip-compressed" | "application/zip-compressed")
    )
}

fn artifact_not_found() -> ArtifactHttpError {
    ArtifactHttpError::new(StatusCode::NOT_FOUND, "not_found", "artifact was not found")
}

fn azure_created_response() -> Response {
    let mut response = StatusCode::CREATED.into_response();
    response.headers_mut().insert(
        "x-ms-request-id",
        HeaderValue::from_str(&Uuid::new_v4().to_string()).expect("UUID is a valid header value"),
    );
    response
        .headers_mut()
        .insert("x-ms-version", HeaderValue::from_static("2021-12-02"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::Client;
    use tempfile::TempDir;

    struct TestService {
        service: ArtifactService,
        client: Client,
        token: String,
        run_id: String,
        job_id: String,
    }

    impl TestService {
        async fn start(fixture: &TempDir, maximum_bytes: u64) -> Self {
            let token = "header.payload.signature".to_owned();
            let run_id = Uuid::new_v4().to_string();
            let job_id = Uuid::new_v4().to_string();
            let service = ArtifactService::start(
                &fixture.path().join("artifacts"),
                &token,
                &run_id,
                &job_id,
                ArtifactLimits {
                    maximum_bytes,
                    maximum_entry_bytes: maximum_bytes.min(1024),
                },
            )
            .await
            .expect("start artifact service");
            Self {
                service,
                client: Client::new(),
                token,
                run_id,
                job_id,
            }
        }

        fn twirp(&self, method: &str) -> reqwest::RequestBuilder {
            self.client
                .post(format!(
                    "{}twirp/{ARTIFACT_SERVICE}/{method}",
                    self.service.base_url
                ))
                .bearer_auth(&self.token)
        }

        async fn create(&self, name: &str) -> (u64, String) {
            let response = self
                .twirp("CreateArtifact")
                .json(&json!({
                    "workflow_run_backend_id": self.run_id,
                    "workflow_job_run_backend_id": self.job_id,
                    "name": name,
                    "version": 7,
                    "mime_type": "application/zip",
                }))
                .send()
                .await
                .expect("create artifact");
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.json::<JsonValue>().await.expect("create body");
            let url = body["signed_upload_url"]
                .as_str()
                .expect("upload URL")
                .to_owned();
            let id = url
                .split("/artifacts/")
                .nth(1)
                .and_then(|value| value.split('/').next())
                .and_then(|value| value.parse().ok())
                .expect("artifact ID in upload URL");
            (id, url)
        }
    }

    #[tokio::test]
    async fn artifact_service_stages_finalizes_lists_downloads_and_deletes() {
        let fixture = TempDir::new().expect("fixture");
        let test = TestService::start(&fixture, 4096).await;
        let (id, upload_url) = test.create("fixture").await;
        let first = "block-a";
        let second = "block-b";
        for (block_id, bytes) in [(second, b"world".as_slice()), (first, b"hello ".as_slice())] {
            let response = test
                .client
                .put(&upload_url)
                .query(&[("comp", "block"), ("blockid", block_id)])
                .body(bytes.to_vec())
                .send()
                .await
                .expect("stage block");
            assert_eq!(response.status(), StatusCode::CREATED);
        }
        let response = test
            .client
            .put(&upload_url)
            .query(&[("comp", "blocklist")])
            .body(format!(
                "<?xml version=\"1.0\"?><BlockList><Latest>{first}</Latest><Latest>{second}</Latest></BlockList>"
            ))
            .send()
            .await
            .expect("commit blocks");
        assert_eq!(response.status(), StatusCode::CREATED);

        let digest = format!(
            "sha256:{}",
            Sha256::digest(b"hello world")
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        let finalize = test
            .twirp("FinalizeArtifact")
            .json(&json!({
                "workflow_run_backend_id": test.run_id,
                "workflow_job_run_backend_id": test.job_id,
                "name": "fixture",
                "size": "11",
                "hash": digest,
            }))
            .send()
            .await
            .expect("finalize artifact");
        assert_eq!(finalize.status(), StatusCode::OK);
        assert_eq!(
            finalize.json::<JsonValue>().await.expect("finalize body")["artifact_id"],
            id.to_string()
        );

        let list = test
            .twirp("ListArtifacts")
            .json(&json!({
                "workflow_run_backend_id": test.run_id,
                "workflow_job_run_backend_id": test.job_id,
                "name_filter": "fixture",
            }))
            .send()
            .await
            .expect("list artifacts");
        let listed = list.json::<JsonValue>().await.expect("list body");
        assert_eq!(listed["artifacts"][0]["database_id"], id.to_string());
        assert_eq!(listed["artifacts"][0]["digest"], digest);

        let signed = test
            .twirp("GetSignedArtifactURL")
            .json(&json!({
                "workflow_run_backend_id": test.run_id,
                "workflow_job_run_backend_id": test.job_id,
                "name": "fixture",
            }))
            .send()
            .await
            .expect("get signed URL")
            .json::<JsonValue>()
            .await
            .expect("signed URL body");
        let download = test
            .client
            .get(signed["signed_url"].as_str().expect("download URL"))
            .send()
            .await
            .expect("download artifact");
        assert_eq!(
            download.bytes().await.expect("download bytes"),
            "hello world"
        );

        let deleted = test
            .twirp("DeleteArtifact")
            .json(&json!({
                "workflow_run_backend_id": test.run_id,
                "workflow_job_run_backend_id": test.job_id,
                "name": "fixture",
            }))
            .send()
            .await
            .expect("delete artifact");
        assert_eq!(deleted.status(), StatusCode::OK);
        test.service.shutdown().await.expect("shutdown service");
    }

    #[tokio::test]
    async fn artifact_service_rejects_bad_auth_duplicate_names_and_oversized_uploads() {
        let fixture = TempDir::new().expect("fixture");
        let test = TestService::start(&fixture, 8).await;
        let unauthorized = test
            .client
            .post(format!(
                "{}twirp/{ARTIFACT_SERVICE}/ListArtifacts",
                test.service.base_url
            ))
            .json(&json!({}))
            .send()
            .await
            .expect("unauthorized request");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        let (_id, upload_url) = test.create("fixture").await;
        let duplicate = test
            .twirp("CreateArtifact")
            .json(&json!({
                "workflow_run_backend_id": test.run_id,
                "workflow_job_run_backend_id": test.job_id,
                "name": "fixture",
                "version": 7,
            }))
            .send()
            .await
            .expect("duplicate request");
        assert_eq!(duplicate.status(), StatusCode::CONFLICT);
        let wrong_scope = test
            .twirp("ListArtifacts")
            .json(&json!({
                "workflow_run_backend_id": "another-run",
                "workflow_job_run_backend_id": test.job_id,
            }))
            .send()
            .await
            .expect("wrong-scope request");
        assert_eq!(wrong_scope.status(), StatusCode::FORBIDDEN);
        let oversized = test
            .client
            .put(&upload_url)
            .body(vec![0_u8; 9])
            .send()
            .await
            .expect("oversized upload");
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let full = test
            .client
            .put(&upload_url)
            .body(vec![0_u8; 8])
            .send()
            .await
            .expect("full-budget upload");
        assert_eq!(full.status(), StatusCode::CREATED);
        let (_second_id, second_url) = test.create("second").await;
        let no_space = test
            .client
            .put(second_url)
            .body(vec![0_u8; 1])
            .send()
            .await
            .expect("total-budget upload");
        assert_eq!(no_space.status(), StatusCode::PAYLOAD_TOO_LARGE);
        test.service.shutdown().await.expect("shutdown service");
    }
}
