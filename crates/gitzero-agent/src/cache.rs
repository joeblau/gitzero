use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    body::Body,
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, patch},
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fmt,
    io::SeekFrom,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex as StdMutex, OnceLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncSeekExt, AsyncWriteExt},
    net::TcpListener,
    sync::{Mutex, oneshot},
    task::JoinHandle,
};
use tokio_util::io::ReaderStream;
use uuid::Uuid;
use walkdir::WalkDir;

const CACHE_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const CACHE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CACHE_KEY_BYTES: usize = 512;
const MAX_CACHE_VERSION_BYTES: usize = 128;
const MAX_LOOKUP_KEYS: usize = 10;

static CACHE_ROOT_LOCKS: OnceLock<StdMutex<BTreeMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();

#[derive(Clone, Copy, Debug)]
pub struct CacheLimits {
    pub maximum_bytes: u64,
    pub maximum_entry_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CacheMode {
    None,
    Read,
    #[default]
    Write,
    WriteOnly,
}

impl CacheMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Read => "read",
            Self::Write => "write",
            Self::WriteOnly => "write-only",
        }
    }

    const fn allows_read(self) -> bool {
        matches!(self, Self::Read | Self::Write)
    }

    const fn allows_write(self) -> bool {
        matches!(self, Self::Write | Self::WriteOnly)
    }
}

impl fmt::Display for CacheMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for CacheMode {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "none" => Ok(Self::None),
            "read" => Ok(Self::Read),
            "write" => Ok(Self::Write),
            "write-only" => Ok(Self::WriteOnly),
            _ => Err("cache mode must be one of: none, read, write, write-only".to_owned()),
        }
    }
}

pub struct CacheService {
    base_url: String,
    runtime_token: String,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<std::io::Result<()>>>,
}

impl CacheService {
    pub async fn start(
        work_root: &Path,
        upload_root: &Path,
        repository: &str,
        scope: &str,
        runtime_token: &str,
        mode: CacheMode,
        limits: CacheLimits,
    ) -> Result<Self> {
        if limits.maximum_entry_bytes == 0 || limits.maximum_bytes < limits.maximum_entry_bytes {
            bail!("workflow cache limits are invalid");
        }
        let root = work_root.join("_workflow-cache");
        let scope_root = root
            .join("repositories")
            .join(hex_digest(repository.to_ascii_lowercase().as_bytes()))
            .join(hex_digest(scope.as_bytes()));
        fs::create_dir_all(&scope_root)
            .await
            .context("create workflow cache scope")?;
        fs::create_dir_all(upload_root)
            .await
            .context("create workflow cache upload directory")?;

        let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .await
            .context("bind loopback workflow cache service")?;
        let address = listener
            .local_addr()
            .context("read workflow cache service address")?;
        let base_url = format!("http://{address}/");
        let state = CacheState {
            root,
            scope_root,
            scope: scope.to_owned(),
            upload_root: upload_root.to_owned(),
            base_url: base_url.clone(),
            runtime_token: runtime_token.to_owned(),
            mode,
            limits,
            next_reservation: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            reservations: Arc::new(Mutex::new(BTreeMap::new())),
        };
        let router = Router::new()
            .route("/_apis/artifactcache/cache", get(lookup_cache))
            .route(
                "/_apis/artifactcache/caches",
                get(list_caches).post(reserve_cache),
            )
            .route(
                "/_apis/artifactcache/caches/{id}",
                patch(upload_chunk).post(commit_cache),
            )
            .route("/_gitzero/cache/{digest}", get(download_cache))
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
            ("ACTIONS_CACHE_URL".to_owned(), self.base_url.clone()),
            (
                "ACTIONS_RUNTIME_TOKEN".to_owned(),
                self.runtime_token.clone(),
            ),
            // The current toolkit selects the local v1 REST contract when this
            // feature flag is absent or empty.
            ("ACTIONS_CACHE_SERVICE_V2".to_owned(), String::new()),
        ])
    }

    #[cfg(test)]
    pub fn runtime_token(&self) -> &str {
        &self.runtime_token
    }

    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(mut task) = self.task.take() {
            match tokio::time::timeout(CACHE_SHUTDOWN_TIMEOUT, &mut task).await {
                Ok(result) => result
                    .context("join workflow cache service")?
                    .context("serve workflow cache requests")?,
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                }
            }
        }
        Ok(())
    }
}

impl Drop for CacheService {
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
struct CacheState {
    root: PathBuf,
    scope_root: PathBuf,
    scope: String,
    upload_root: PathBuf,
    base_url: String,
    runtime_token: String,
    mode: CacheMode,
    limits: CacheLimits,
    next_reservation: Arc<std::sync::atomic::AtomicU64>,
    reservations: Arc<Mutex<BTreeMap<u64, Arc<Mutex<Reservation>>>>>,
}

struct Reservation {
    key: String,
    version: String,
    upload_path: PathBuf,
    ranges: Vec<(u64, u64)>,
    committed: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct StoredCache {
    cache_key: String,
    cache_version: String,
    scope: String,
    creation_time: u64,
    last_accessed_time: u64,
    size: u64,
}

struct CacheEntry {
    directory: PathBuf,
    metadata: StoredCache,
}

#[derive(Deserialize)]
struct LookupQuery {
    keys: String,
    version: String,
}

#[derive(Default, Deserialize)]
struct ListQuery {
    key: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReserveRequest {
    key: String,
    version: Option<String>,
    cache_size: Option<u64>,
}

#[derive(Deserialize)]
struct CommitRequest {
    size: u64,
}

#[derive(Deserialize)]
struct DownloadQuery {
    token: String,
}

#[derive(Debug)]
struct CacheHttpError {
    status: StatusCode,
    message: String,
}

impl CacheHttpError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
    }
}

impl IntoResponse for CacheHttpError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "message": self.message }))).into_response()
    }
}

type CacheResponse<T> = std::result::Result<T, CacheHttpError>;

async fn lookup_cache(
    State(state): State<CacheState>,
    headers: HeaderMap,
    Query(query): Query<LookupQuery>,
) -> CacheResponse<Response> {
    authorize(&state, &headers)?;
    require_cache_read(&state)?;
    validate_version(&query.version)?;
    let keys = query.keys.split(',').map(str::to_owned).collect::<Vec<_>>();
    if keys.is_empty() || keys.len() > MAX_LOOKUP_KEYS {
        return Err(CacheHttpError::new(
            StatusCode::BAD_REQUEST,
            "cache lookup must contain between one and ten keys",
        ));
    }
    for key in &keys {
        validate_key(key)?;
    }

    let lock = cache_root_lock(&state.root);
    let _guard = lock.lock().await;
    let mut entries = load_scope_entries(&state.scope_root)
        .await
        .map_err(CacheHttpError::internal)?;
    discard_expired_entries(&mut entries)
        .await
        .map_err(CacheHttpError::internal)?;
    let Some(mut entry) = match_cache(&entries, &keys, &query.version) else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    };
    entry.metadata.last_accessed_time = now_millis();
    write_metadata_atomic(&entry.directory, &entry.metadata)
        .await
        .map_err(CacheHttpError::internal)?;
    let digest = entry
        .directory
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| CacheHttpError::internal("cache entry path is invalid"))?;
    let archive_location = format!(
        "{}_gitzero/cache/{digest}?token={}",
        state.base_url, state.runtime_token
    );
    Ok(Json(json!({
        "cacheKey": entry.metadata.cache_key,
        "scope": entry.metadata.scope,
        "cacheVersion": entry.metadata.cache_version,
        "creationTime": entry.metadata.creation_time.to_string(),
        "archiveLocation": archive_location,
    }))
    .into_response())
}

async fn list_caches(
    State(state): State<CacheState>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> CacheResponse<Response> {
    authorize(&state, &headers)?;
    require_cache_read(&state)?;
    let lock = cache_root_lock(&state.root);
    let _guard = lock.lock().await;
    let mut entries = load_scope_entries(&state.scope_root)
        .await
        .map_err(CacheHttpError::internal)?;
    discard_expired_entries(&mut entries)
        .await
        .map_err(CacheHttpError::internal)?;
    let caches = entries
        .into_iter()
        .filter(|entry| {
            query
                .key
                .as_ref()
                .is_none_or(|key| entry.metadata.cache_key.starts_with(key))
        })
        .map(|entry| {
            json!({
                "cacheKey": entry.metadata.cache_key,
                "scope": entry.metadata.scope,
                "cacheVersion": entry.metadata.cache_version,
                "creationTime": entry.metadata.creation_time.to_string(),
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "totalCount": caches.len(),
        "artifactCaches": caches,
    }))
    .into_response())
}

async fn reserve_cache(
    State(state): State<CacheState>,
    headers: HeaderMap,
    Json(request): Json<ReserveRequest>,
) -> CacheResponse<Response> {
    authorize(&state, &headers)?;
    require_cache_write(&state)?;
    validate_key(&request.key)?;
    let version = request.version.unwrap_or_default();
    validate_version(&version)?;
    if request
        .cache_size
        .is_some_and(|size| size > state.limits.maximum_entry_bytes)
    {
        return Err(CacheHttpError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "cache entry exceeds the configured per-entry limit",
        ));
    }

    let lock = cache_root_lock(&state.root);
    let _guard = lock.lock().await;
    let mut entries = load_scope_entries(&state.scope_root)
        .await
        .map_err(CacheHttpError::internal)?;
    discard_expired_entries(&mut entries)
        .await
        .map_err(CacheHttpError::internal)?;
    if entries.iter().any(|entry| {
        entry.metadata.cache_key == request.key && entry.metadata.cache_version == version
    }) {
        return Err(CacheHttpError::new(
            StatusCode::CONFLICT,
            "an immutable cache entry already exists for this key and version",
        ));
    }
    let id = state
        .next_reservation
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let upload_path = state.upload_root.join(format!("cache-{id}.upload"));
    let reservation = Reservation {
        key: request.key,
        version,
        upload_path,
        ranges: Vec::new(),
        committed: false,
    };
    state
        .reservations
        .lock()
        .await
        .insert(id, Arc::new(Mutex::new(reservation)));
    Ok((StatusCode::CREATED, Json(json!({ "cacheId": id }))).into_response())
}

async fn upload_chunk(
    State(state): State<CacheState>,
    AxumPath(id): AxumPath<u64>,
    headers: HeaderMap,
    body: Body,
) -> CacheResponse<Response> {
    authorize(&state, &headers)?;
    require_cache_write(&state)?;
    let (start, end) = parse_content_range(&headers)?;
    if end >= state.limits.maximum_entry_bytes {
        return Err(CacheHttpError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "cache upload exceeds the configured per-entry limit",
        ));
    }
    let reservation = state
        .reservations
        .lock()
        .await
        .get(&id)
        .cloned()
        .ok_or_else(|| CacheHttpError::new(StatusCode::NOT_FOUND, "cache reservation not found"))?;
    let mut reservation = reservation.lock().await;
    if reservation.committed {
        return Err(CacheHttpError::new(
            StatusCode::CONFLICT,
            "cache reservation is already committed",
        ));
    }
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&reservation.upload_path)
        .await
        .map_err(CacheHttpError::internal)?;
    file.seek(SeekFrom::Start(start))
        .await
        .map_err(CacheHttpError::internal)?;
    let expected = end - start + 1;
    let mut received = 0_u64;
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(CacheHttpError::internal)?;
        received = received.checked_add(chunk.len() as u64).ok_or_else(|| {
            CacheHttpError::new(StatusCode::BAD_REQUEST, "cache chunk is too large")
        })?;
        if received > expected {
            return Err(CacheHttpError::new(
                StatusCode::BAD_REQUEST,
                "cache chunk body exceeds Content-Range",
            ));
        }
        file.write_all(&chunk)
            .await
            .map_err(CacheHttpError::internal)?;
    }
    file.flush().await.map_err(CacheHttpError::internal)?;
    if received != expected {
        return Err(CacheHttpError::new(
            StatusCode::BAD_REQUEST,
            "cache chunk body does not match Content-Range",
        ));
    }
    reservation.ranges.push((start, end));
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn commit_cache(
    State(state): State<CacheState>,
    AxumPath(id): AxumPath<u64>,
    headers: HeaderMap,
    Json(request): Json<CommitRequest>,
) -> CacheResponse<Response> {
    authorize(&state, &headers)?;
    require_cache_write(&state)?;
    if request.size == 0 || request.size > state.limits.maximum_entry_bytes {
        return Err(CacheHttpError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "cache entry size is outside the configured limit",
        ));
    }
    let reservation = state
        .reservations
        .lock()
        .await
        .get(&id)
        .cloned()
        .ok_or_else(|| CacheHttpError::new(StatusCode::NOT_FOUND, "cache reservation not found"))?;
    let mut reservation = reservation.lock().await;
    if reservation.committed {
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    if !ranges_cover(&reservation.ranges, request.size) {
        return Err(CacheHttpError::new(
            StatusCode::BAD_REQUEST,
            "cache upload is incomplete",
        ));
    }
    let upload_size = fs::metadata(&reservation.upload_path)
        .await
        .map_err(CacheHttpError::internal)?
        .len();
    if upload_size != request.size {
        return Err(CacheHttpError::new(
            StatusCode::BAD_REQUEST,
            "committed cache size does not match the upload",
        ));
    }

    let lock = cache_root_lock(&state.root);
    let _guard = lock.lock().await;
    let mut entries = load_scope_entries(&state.scope_root)
        .await
        .map_err(CacheHttpError::internal)?;
    discard_expired_entries(&mut entries)
        .await
        .map_err(CacheHttpError::internal)?;
    if entries.iter().any(|entry| {
        entry.metadata.cache_key == reservation.key
            && entry.metadata.cache_version == reservation.version
    }) {
        return Err(CacheHttpError::new(
            StatusCode::CONFLICT,
            "an immutable cache entry already exists for this key and version",
        ));
    }
    evict_for_space(&state.root, state.limits.maximum_bytes, request.size)
        .await
        .map_err(CacheHttpError::internal)?;

    let digest = hex_digest(format!("{}\0{}", reservation.key, reservation.version).as_bytes());
    let final_directory = state.scope_root.join(&digest);
    if fs::try_exists(&final_directory)
        .await
        .map_err(CacheHttpError::internal)?
    {
        return Err(CacheHttpError::new(
            StatusCode::CONFLICT,
            "cache entry storage already exists",
        ));
    }
    let temporary_directory = state
        .scope_root
        .join(format!(".{digest}.tmp-{}", Uuid::new_v4().simple()));
    fs::create_dir(&temporary_directory)
        .await
        .map_err(CacheHttpError::internal)?;
    let now = now_millis();
    let metadata = StoredCache {
        cache_key: reservation.key.clone(),
        cache_version: reservation.version.clone(),
        scope: state.scope.clone(),
        creation_time: now,
        last_accessed_time: now,
        size: request.size,
    };
    let store_result: Result<()> = async {
        fs::rename(
            &reservation.upload_path,
            temporary_directory.join("archive"),
        )
        .await
        .context("move cache archive into durable storage")?;
        fs::write(
            temporary_directory.join("metadata.json"),
            serde_json::to_vec(&metadata)?,
        )
        .await
        .context("write cache metadata")?;
        fs::rename(&temporary_directory, &final_directory)
            .await
            .context("atomically publish cache entry")?;
        Ok(())
    }
    .await;
    if let Err(error) = store_result {
        let _ = fs::remove_dir_all(&temporary_directory).await;
        return Err(CacheHttpError::internal(error));
    }
    reservation.committed = true;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn download_cache(
    State(state): State<CacheState>,
    AxumPath(digest): AxumPath<String>,
    Query(query): Query<DownloadQuery>,
) -> CacheResponse<Response> {
    if query.token != state.runtime_token || !valid_digest(&digest) {
        return Err(CacheHttpError::new(
            StatusCode::UNAUTHORIZED,
            "cache download is not authorized",
        ));
    }
    require_cache_read(&state)?;
    let archive = state.scope_root.join(digest).join("archive");
    let file = fs::File::open(&archive).await.map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            CacheHttpError::new(StatusCode::NOT_FOUND, "cache archive not found")
        } else {
            CacheHttpError::internal(error)
        }
    })?;
    let size = file
        .metadata()
        .await
        .map_err(CacheHttpError::internal)?
        .len();
    let mut response = Response::new(Body::from_stream(ReaderStream::new(file)));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&size.to_string()).map_err(CacheHttpError::internal)?,
    );
    Ok(response)
}

fn authorize(state: &CacheState, headers: &HeaderMap) -> CacheResponse<()> {
    let expected = format!("Bearer {}", state.runtime_token);
    if headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        == Some(expected.as_str())
    {
        Ok(())
    } else {
        Err(CacheHttpError::new(
            StatusCode::UNAUTHORIZED,
            "cache request is not authorized",
        ))
    }
}

fn require_cache_read(state: &CacheState) -> CacheResponse<()> {
    if state.mode.allows_read() {
        Ok(())
    } else {
        Err(CacheHttpError::new(
            StatusCode::FORBIDDEN,
            format!(
                "cache read denied: effective cache mode '{}' does not permit reads",
                state.mode
            ),
        ))
    }
}

fn require_cache_write(state: &CacheState) -> CacheResponse<()> {
    if state.mode.allows_write() {
        Ok(())
    } else {
        Err(CacheHttpError::new(
            StatusCode::FORBIDDEN,
            format!(
                "cache write denied: effective cache mode '{}' does not permit writes",
                state.mode
            ),
        ))
    }
}

fn validate_key(key: &str) -> CacheResponse<()> {
    if key.is_empty() || key.len() > MAX_CACHE_KEY_BYTES || key.contains(',') {
        return Err(CacheHttpError::new(
            StatusCode::BAD_REQUEST,
            "cache key is empty, too large, or contains a comma",
        ));
    }
    Ok(())
}

fn validate_version(version: &str) -> CacheResponse<()> {
    if version.is_empty() || version.len() > MAX_CACHE_VERSION_BYTES {
        return Err(CacheHttpError::new(
            StatusCode::BAD_REQUEST,
            "cache version is empty or too large",
        ));
    }
    Ok(())
}

fn parse_content_range(headers: &HeaderMap) -> CacheResponse<(u64, u64)> {
    let value = headers
        .get(header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| CacheHttpError::new(StatusCode::BAD_REQUEST, "Content-Range is required"))?;
    let range = value
        .strip_prefix("bytes ")
        .and_then(|value| value.split_once('/'))
        .map(|(range, _)| range)
        .and_then(|range| range.split_once('-'))
        .ok_or_else(|| CacheHttpError::new(StatusCode::BAD_REQUEST, "Content-Range is invalid"))?;
    let start = range
        .0
        .parse::<u64>()
        .map_err(|_| CacheHttpError::new(StatusCode::BAD_REQUEST, "Content-Range is invalid"))?;
    let end = range
        .1
        .parse::<u64>()
        .map_err(|_| CacheHttpError::new(StatusCode::BAD_REQUEST, "Content-Range is invalid"))?;
    if end < start {
        return Err(CacheHttpError::new(
            StatusCode::BAD_REQUEST,
            "Content-Range is invalid",
        ));
    }
    Ok((start, end))
}

fn ranges_cover(ranges: &[(u64, u64)], size: u64) -> bool {
    let mut ranges = ranges.to_vec();
    ranges.sort_unstable();
    let mut covered = 0_u64;
    for (start, end) in ranges {
        if start > covered {
            return false;
        }
        covered = covered.max(end.saturating_add(1));
        if covered >= size {
            return true;
        }
    }
    false
}

fn match_cache(entries: &[CacheEntry], keys: &[String], version: &str) -> Option<CacheEntry> {
    for key in keys {
        let mut candidates = entries
            .iter()
            .filter(|entry| {
                entry.metadata.cache_version == version && entry.metadata.cache_key == *key
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|entry| std::cmp::Reverse(entry.metadata.creation_time));
        if let Some(entry) = candidates.first() {
            return Some(clone_entry(entry));
        }
        let mut candidates = entries
            .iter()
            .filter(|entry| {
                entry.metadata.cache_version == version && entry.metadata.cache_key.starts_with(key)
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|entry| std::cmp::Reverse(entry.metadata.creation_time));
        if let Some(entry) = candidates.first() {
            return Some(clone_entry(entry));
        }
    }
    None
}

fn clone_entry(entry: &&CacheEntry) -> CacheEntry {
    CacheEntry {
        directory: entry.directory.clone(),
        metadata: entry.metadata.clone(),
    }
}

async fn load_scope_entries(scope_root: &Path) -> Result<Vec<CacheEntry>> {
    let mut entries = Vec::new();
    let mut directory = fs::read_dir(scope_root).await?;
    while let Some(entry) = directory.next_entry().await? {
        let file_type = entry.file_type().await?;
        if !file_type.is_dir() || entry.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        let entry_directory = entry.path();
        let metadata_path = entry_directory.join("metadata.json");
        let archive_path = entry_directory.join("archive");
        let Ok(bytes) = fs::read(&metadata_path).await else {
            continue;
        };
        let Ok(metadata) = serde_json::from_slice::<StoredCache>(&bytes) else {
            continue;
        };
        if fs::try_exists(&archive_path).await? {
            entries.push(CacheEntry {
                directory: entry_directory,
                metadata,
            });
        }
    }
    Ok(entries)
}

async fn discard_expired_entries(entries: &mut Vec<CacheEntry>) -> Result<()> {
    let cutoff = now_millis().saturating_sub(CACHE_RETENTION.as_millis() as u64);
    let mut retained = Vec::with_capacity(entries.len());
    for entry in entries.drain(..) {
        if entry.metadata.last_accessed_time < cutoff {
            fs::remove_dir_all(&entry.directory).await?;
        } else {
            retained.push(entry);
        }
    }
    *entries = retained;
    Ok(())
}

async fn write_metadata_atomic(directory: &Path, metadata: &StoredCache) -> Result<()> {
    let temporary = directory.join(format!(".metadata-{}.tmp", Uuid::new_v4().simple()));
    fs::write(&temporary, serde_json::to_vec(metadata)?).await?;
    fs::rename(&temporary, directory.join("metadata.json")).await?;
    Ok(())
}

async fn evict_for_space(root: &Path, maximum_bytes: u64, required_bytes: u64) -> Result<()> {
    let root = root.to_owned();
    let mut entries = tokio::task::spawn_blocking(move || scan_cache_entries(&root))
        .await
        .context("join workflow cache scan")??;
    let cutoff = now_millis().saturating_sub(CACHE_RETENTION.as_millis() as u64);
    let mut total = entries.iter().fold(0_u64, |total, entry| {
        total.saturating_add(entry.metadata.size)
    });
    entries.sort_by_key(|entry| {
        (
            entry.metadata.last_accessed_time,
            entry.metadata.creation_time,
        )
    });
    for entry in entries {
        if entry.metadata.last_accessed_time < cutoff
            || total.saturating_add(required_bytes) > maximum_bytes
        {
            match fs::remove_dir_all(&entry.directory).await {
                Ok(()) => total = total.saturating_sub(entry.metadata.size),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    if total.saturating_add(required_bytes) > maximum_bytes {
        bail!("workflow cache does not have enough space after eviction");
    }
    Ok(())
}

fn scan_cache_entries(root: &Path) -> Result<Vec<CacheEntry>> {
    let mut entries = Vec::new();
    if !root.exists() {
        return Ok(entries);
    }
    for entry in WalkDir::new(root).follow_links(false) {
        let entry = entry?;
        if entry.file_type().is_file() && entry.file_name() == "metadata.json" {
            let metadata: StoredCache = serde_json::from_slice(&std::fs::read(entry.path())?)?;
            let directory = entry
                .path()
                .parent()
                .context("cache metadata has no parent directory")?
                .to_owned();
            if directory.join("archive").is_file() {
                entries.push(CacheEntry {
                    directory,
                    metadata,
                });
            }
        }
    }
    Ok(entries)
}

fn cache_root_lock(root: &Path) -> Arc<Mutex<()>> {
    CACHE_ROOT_LOCKS
        .get_or_init(|| StdMutex::new(BTreeMap::new()))
        .lock()
        .expect("workflow cache root lock map was poisoned")
        .entry(root.to_owned())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

fn hex_digest(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::Client;
    use tempfile::TempDir;

    struct TestService {
        service: CacheService,
        client: Client,
    }

    impl TestService {
        async fn start(fixture: &TempDir, scope: &str, maximum_bytes: u64) -> Self {
            Self::start_with_mode(fixture, scope, maximum_bytes, CacheMode::Write).await
        }

        async fn start_with_mode(
            fixture: &TempDir,
            scope: &str,
            maximum_bytes: u64,
            mode: CacheMode,
        ) -> Self {
            let service = CacheService::start(
                fixture.path(),
                &fixture.path().join(format!("uploads-{}", Uuid::new_v4())),
                "fixture/repository",
                scope,
                "fixture.runtime.token",
                mode,
                CacheLimits {
                    maximum_bytes,
                    maximum_entry_bytes: maximum_bytes.min(1_024),
                },
            )
            .await
            .expect("start cache service");
            Self {
                service,
                client: Client::new(),
            }
        }

        fn url(&self, resource: &str) -> String {
            format!("{}{}", self.service.base_url, resource)
        }

        fn request(&self, method: reqwest::Method, resource: &str) -> reqwest::RequestBuilder {
            self.client
                .request(method, self.url(resource))
                .bearer_auth(self.service.runtime_token())
        }

        async fn save(&self, key: &str, version: &str, data: &[u8]) -> StatusCode {
            let response = self
                .request(reqwest::Method::POST, "_apis/artifactcache/caches")
                .json(&json!({ "key": key, "version": version, "cacheSize": data.len() }))
                .send()
                .await
                .expect("reserve cache");
            assert_eq!(response.status(), StatusCode::CREATED);
            let id = response
                .json::<serde_json::Value>()
                .await
                .expect("reserve response")["cacheId"]
                .as_u64()
                .expect("cache id");
            let split = data.len() / 2;
            for (start, chunk) in [(split, &data[split..]), (0, &data[..split])] {
                if chunk.is_empty() {
                    continue;
                }
                let end = start + chunk.len() - 1;
                let response = self
                    .request(
                        reqwest::Method::PATCH,
                        &format!("_apis/artifactcache/caches/{id}"),
                    )
                    .header("Content-Range", format!("bytes {start}-{end}/*"))
                    .body(chunk.to_vec())
                    .send()
                    .await
                    .expect("upload cache chunk");
                assert_eq!(response.status(), StatusCode::NO_CONTENT);
            }
            let response = self
                .request(
                    reqwest::Method::POST,
                    &format!("_apis/artifactcache/caches/{id}"),
                )
                .json(&json!({ "size": data.len() }))
                .send()
                .await
                .expect("commit cache");
            let status = response.status();
            if status == StatusCode::NO_CONTENT {
                let retry = self
                    .request(
                        reqwest::Method::POST,
                        &format!("_apis/artifactcache/caches/{id}"),
                    )
                    .json(&json!({ "size": data.len() }))
                    .send()
                    .await
                    .expect("retry cache commit");
                assert_eq!(retry.status(), StatusCode::NO_CONTENT);
            }
            status
        }

        async fn lookup(&self, keys: &str, version: &str) -> reqwest::Response {
            self.request(reqwest::Method::GET, "_apis/artifactcache/cache")
                .query(&[("keys", keys), ("version", version)])
                .send()
                .await
                .expect("lookup cache")
        }
    }

    #[test]
    fn cache_modes_parse_and_expose_the_github_lattice() {
        for (value, mode, readable, writable) in [
            ("none", CacheMode::None, false, false),
            ("read", CacheMode::Read, true, false),
            ("write", CacheMode::Write, true, true),
            ("write-only", CacheMode::WriteOnly, false, true),
        ] {
            assert_eq!(value.parse::<CacheMode>().unwrap(), mode);
            assert_eq!(mode.to_string(), value);
            assert_eq!(mode.allows_read(), readable);
            assert_eq!(mode.allows_write(), writable);
        }
        assert!("READ".parse::<CacheMode>().is_err());
        assert!("read-write".parse::<CacheMode>().is_err());
    }

    #[tokio::test]
    async fn cache_service_enforces_effective_read_and_write_modes() {
        let fixture = TempDir::new().expect("fixture");
        let scope = "refs/pull/8/head";
        let seed = TestService::start(&fixture, scope, 4_096).await;
        assert_eq!(
            seed.save("seed", "v1", b"seed bytes").await,
            StatusCode::NO_CONTENT
        );
        seed.service
            .shutdown()
            .await
            .expect("shutdown seed service");
        let digest = scan_cache_entries(&fixture.path().join("_workflow-cache"))
            .expect("scan seeded cache")
            .into_iter()
            .next()
            .and_then(|entry| {
                entry
                    .directory
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
            .expect("seeded cache digest");

        let read = TestService::start_with_mode(&fixture, scope, 4_096, CacheMode::Read).await;
        assert_eq!(read.lookup("seed", "v1").await.status(), StatusCode::OK);
        let download = read
            .client
            .get(read.url(&format!(
                "_gitzero/cache/{digest}?token={}",
                read.service.runtime_token()
            )))
            .send()
            .await
            .expect("read-mode download");
        assert_eq!(download.status(), StatusCode::OK);
        assert_eq!(download.bytes().await.unwrap(), "seed bytes");
        let denied_write = read
            .request(reqwest::Method::POST, "_apis/artifactcache/caches")
            .json(&json!({ "key": "denied", "version": "v1", "cacheSize": 4 }))
            .send()
            .await
            .expect("read-mode reservation denial");
        assert_eq!(denied_write.status(), StatusCode::FORBIDDEN);
        assert!(
            denied_write.json::<serde_json::Value>().await.unwrap()["message"]
                .as_str()
                .unwrap()
                .starts_with("cache write denied:")
        );
        read.service
            .shutdown()
            .await
            .expect("shutdown read service");

        let write_only =
            TestService::start_with_mode(&fixture, scope, 4_096, CacheMode::WriteOnly).await;
        assert_eq!(
            write_only.lookup("seed", "v1").await.status(),
            StatusCode::FORBIDDEN
        );
        let denied_download = write_only
            .client
            .get(write_only.url(&format!(
                "_gitzero/cache/{digest}?token={}",
                write_only.service.runtime_token()
            )))
            .send()
            .await
            .expect("write-only download denial");
        assert_eq!(denied_download.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            write_only.save("write-only", "v1", b"new bytes").await,
            StatusCode::NO_CONTENT
        );
        write_only
            .service
            .shutdown()
            .await
            .expect("shutdown write-only service");

        let none = TestService::start_with_mode(&fixture, scope, 4_096, CacheMode::None).await;
        assert_eq!(
            none.lookup("seed", "v1").await.status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            none.request(reqwest::Method::GET, "_apis/artifactcache/caches")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            none.request(reqwest::Method::PATCH, "_apis/artifactcache/caches/1")
                .header("Content-Range", "bytes 0-0/*")
                .body(vec![0])
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            none.request(reqwest::Method::POST, "_apis/artifactcache/caches/1")
                .json(&json!({ "size": 1 }))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        none.service
            .shutdown()
            .await
            .expect("shutdown none service");
    }

    #[tokio::test]
    async fn cache_service_streams_immutable_entries_and_prefix_matches() {
        let fixture = TempDir::new().expect("fixture");
        let test = TestService::start(&fixture, "refs/pull/7/head", 4_096).await;
        assert_eq!(
            test.lookup("macos-cargo-", "v1").await.status(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            test.save("macos-cargo-one", "v1", b"cached bytes").await,
            StatusCode::NO_CONTENT
        );
        let response = test.lookup("missing,macos-cargo-", "v1").await;
        assert_eq!(response.status(), StatusCode::OK);
        let result = response
            .json::<serde_json::Value>()
            .await
            .expect("lookup body");
        assert_eq!(result["cacheKey"], "macos-cargo-one");
        assert_eq!(result["scope"], "refs/pull/7/head");
        let archive = test
            .client
            .get(result["archiveLocation"].as_str().expect("archive URL"))
            .send()
            .await
            .expect("download cache");
        assert_eq!(
            archive.bytes().await.expect("archive bytes"),
            "cached bytes"
        );

        let duplicate = test
            .request(reqwest::Method::POST, "_apis/artifactcache/caches")
            .json(&json!({ "key": "macos-cargo-one", "version": "v1" }))
            .send()
            .await
            .expect("duplicate reservation");
        assert_eq!(duplicate.status(), StatusCode::CONFLICT);
        test.service.shutdown().await.expect("shutdown service");
    }

    #[tokio::test]
    async fn cache_service_isolates_scopes_and_rejects_unauthenticated_requests() {
        let fixture = TempDir::new().expect("fixture");
        let first = TestService::start(&fixture, "refs/pull/1/head", 4_096).await;
        assert_eq!(
            first.save("shared-key", "v1", b"scope one").await,
            StatusCode::NO_CONTENT
        );
        let unauthorized = first
            .client
            .get(first.url("_apis/artifactcache/cache?keys=shared-key&version=v1"))
            .send()
            .await
            .expect("unauthorized lookup");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let second = TestService::start(&fixture, "refs/pull/2/head", 4_096).await;
        assert_eq!(
            second.lookup("shared-key", "v1").await.status(),
            StatusCode::NO_CONTENT
        );
        first.service.shutdown().await.expect("shutdown first");
        second.service.shutdown().await.expect("shutdown second");
    }

    #[tokio::test]
    async fn cache_service_evicts_the_least_recent_entry_under_its_disk_limit() {
        let fixture = TempDir::new().expect("fixture");
        let test = TestService::start(&fixture, "refs/pull/3/head", 12).await;
        assert_eq!(
            test.save("first", "v1", b"12345678").await,
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            test.save("second", "v1", b"abcdefgh").await,
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            test.lookup("first", "v1").await.status(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(test.lookup("second", "v1").await.status(), StatusCode::OK);
        test.service.shutdown().await.expect("shutdown service");
    }

    #[tokio::test]
    async fn cache_service_expires_idle_entries_and_allows_the_key_to_be_saved_again() {
        let fixture = TempDir::new().expect("fixture");
        let test = TestService::start(&fixture, "refs/pull/4/head", 4_096).await;
        assert_eq!(
            test.save("expired", "v1", b"old bytes").await,
            StatusCode::NO_CONTENT
        );
        let mut entries =
            scan_cache_entries(&fixture.path().join("_workflow-cache")).expect("scan stored cache");
        assert_eq!(entries.len(), 1);
        entries[0].metadata.last_accessed_time = 0;
        fs::write(
            entries[0].directory.join("metadata.json"),
            serde_json::to_vec(&entries[0].metadata).expect("encode stale metadata"),
        )
        .await
        .expect("age cache entry");

        assert_eq!(
            test.lookup("expired", "v1").await.status(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            test.save("expired", "v1", b"new bytes").await,
            StatusCode::NO_CONTENT
        );
        test.service.shutdown().await.expect("shutdown service");
    }
}
