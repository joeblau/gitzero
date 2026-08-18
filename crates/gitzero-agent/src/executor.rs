use crate::action::{
    ActionDefinition, ActionReference, ActionStep, RemoteReusableWorkflowReference, load_definition,
};
use crate::artifact::{ArtifactLimits, ArtifactService};
use crate::cache::{CacheLimits, CacheService};
use crate::concurrency::{ConcurrencyAcquisition, ConcurrencyClient};
use crate::problem_matcher::ProblemMatcherRegistry;
use crate::repository_access::{ExpiringToken, RepositoryAccessClient};
use anyhow::{Context, Result, bail};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use gitzero_expression::{
    EvaluationContext, ExecutionStatus, ExpressionAnalysis, analyze_expression, analyze_template,
};
use gitzero_protocol::{
    AgentMessage, CheckAnnotation, CheckAnnotationLevel, Conclusion, ConcurrencyQueue, LogStream,
    MAX_CHECK_ANNOTATION_MESSAGE_BYTES, MAX_CHECK_ANNOTATION_PATH_BYTES,
    MAX_CHECK_ANNOTATION_TITLE_BYTES, MAX_CHECK_ANNOTATIONS, MAX_RUNNER_LABELS,
    MAX_RUNNER_REQUIREMENTS, MAX_RUNNER_SELECTOR_BYTES, RepositoryTokenPurpose, RunSpec,
    RunnerRequirement,
};
use gitzero_workflow::{
    ExecutionPlan, MAX_UNIQUE_REUSABLE_WORKFLOWS, PlannedConcurrency, PlannedConcurrencyQueue,
    PlannedJob, PlannedPermissions, PlannedStep, PlannedVirtualJob, ReusableInputType, StepKind,
    Workflow, compile_with_reusables, expand_dynamic_reusable_call, expand_matrix_definition,
    matches_pull_request, parse, requires_pull_request_changed_paths,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use serde_yaml_ng::Value as YamlValue;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, Mutex as StdMutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader},
    process::Command,
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, watch},
};
use tracing::info;
use uuid::Uuid;

const MAX_LOG_CHUNK_BYTES: usize = 32 * 1_024;
const MAX_PULL_REQUEST_FILES: usize = 3_000;
const PULL_REQUEST_FILES_PER_PAGE: usize = 100;
const MAX_STEP_SUMMARY_BYTES: usize = 1_024 * 1_024;
const MAX_STEP_SUMMARIES_PER_JOB: usize = 20;
const MAX_CHECK_SUMMARY_UTF16_UNITS: usize = 60_000;
const MAX_DYNAMIC_MASKS_PER_JOB: usize = 1_024;
const MAX_DYNAMIC_MASK_BYTES_PER_JOB: usize = 1_024 * 1_024;
const MAX_LEGACY_COMMAND_VALUES_PER_STEP: usize = 1_024;
const MAX_LEGACY_COMMAND_BYTES_PER_STEP: usize = 1_024 * 1_024;
const MAX_ERROR_ANNOTATIONS_PER_STEP: usize = 10;
const MAX_WARNING_ANNOTATIONS_PER_STEP: usize = 10;
const MAX_NOTICE_ANNOTATIONS_PER_STEP: usize = 10;
const DEFAULT_JOB_TIMEOUT: Duration = Duration::from_secs(360 * 60);
const ENVIRONMENT_VARIABLE_PAGE_SIZE: usize = 30;
const MAX_ENVIRONMENT_VARIABLES: usize = 100;
const MAX_CONFIGURATION_VARIABLE_BYTES: usize = 48 * 1_024;
const DEPLOYMENT_BRANCH_POLICY_PAGE_SIZE: usize = 100;
const MAX_DEPLOYMENT_BRANCH_POLICIES: usize = 1_000;
const MAX_DEPLOYMENT_BRANCH_PATTERN_BYTES: usize = 1_024;
const MAX_DEPLOYMENT_BRANCH_PATTERN_TOTAL_BYTES: usize = 256 * 1_024;
const MAX_CHECKOUT_FILTER_BYTES: usize = 1_024;
const MAX_CHECKOUT_REF_BYTES: usize = 1_024;
const MAX_GIT_CAPTURE_BYTES: usize = 64 * 1_024;
const MAX_CHECKOUT_SSH_KNOWN_HOSTS_BYTES: usize = 256 * 1_024;
const MAX_CHECKOUT_SSH_USER_BYTES: usize = 64;
const MAX_SPARSE_CHECKOUT_PATTERNS: usize = 4_096;
const MAX_SPARSE_CHECKOUT_BYTES: usize = 128 * 1_024;
// A page can contain 30 values at 48 KiB each. JSON escaping can expand each
// byte to a six-character Unicode escape, so keep the response bounded above
// that legitimate worst case.
const MAX_GITHUB_API_RESPONSE_BYTES: usize = 10 * 1_024 * 1_024;
const MAX_REPOSITORY_TOKENS_PER_RUN: usize = 256;
const MAX_WORKFLOW_TOKEN_SCOPES_PER_RUN: usize = 256;
const MAX_MANAGED_SECRETS_PER_JOB: usize = 300;
const MAX_MANAGED_SECRET_BYTES: usize = 48 * 1_024;

type EnvironmentVariableCache = Arc<Mutex<BTreeMap<String, BTreeMap<String, String>>>>;

#[derive(Clone)]
struct RunRepositoryAccess {
    client: RepositoryAccessClient,
    tokens: Arc<Mutex<BTreeMap<(RepositoryTokenPurpose, String), ExpiringToken>>>,
    workflow_tokens: Arc<Mutex<BTreeMap<PlannedPermissions, ExpiringToken>>>,
    workflow_commands: Arc<StdMutex<WorkflowCommandProcessor>>,
}

impl RunRepositoryAccess {
    fn new(
        client: RepositoryAccessClient,
        workflow_commands: Arc<StdMutex<WorkflowCommandProcessor>>,
    ) -> Self {
        Self {
            client,
            tokens: Arc::new(Mutex::new(BTreeMap::new())),
            workflow_tokens: Arc::new(Mutex::new(BTreeMap::new())),
            workflow_commands,
        }
    }

    async fn cached_token(
        &self,
        purpose: RepositoryTokenPurpose,
        owner: &str,
        repository: &str,
    ) -> Option<String> {
        let key = (purpose, repository_access_key(owner, repository));
        let mut tokens = self.tokens.lock().await;
        match tokens.get(&key) {
            Some(token) if token.is_usable() => Some(token.token.clone()),
            Some(_) => {
                if let Some(mut token) = tokens.remove(&key) {
                    token.clear();
                }
                None
            }
            None => None,
        }
    }

    async fn request_token(
        &self,
        run_id: Uuid,
        purpose: RepositoryTokenPurpose,
        owner: &str,
        repository: &str,
        refresh: bool,
        cancel: &watch::Receiver<bool>,
    ) -> Result<String> {
        let key = (purpose, repository_access_key(owner, repository));
        let mut tokens = self.tokens.lock().await;
        if refresh {
            if let Some(mut token) = tokens.remove(&key) {
                token.clear();
            }
        } else if let Some(token) = tokens.get(&key)
            && token.is_usable()
        {
            return Ok(token.token.clone());
        } else if let Some(mut token) = tokens.remove(&key) {
            token.clear();
        }
        if tokens.len() >= MAX_REPOSITORY_TOKENS_PER_RUN {
            bail!(
                "workflow requests more than {MAX_REPOSITORY_TOKENS_PER_RUN} private repository tokens"
            );
        }
        let token = self
            .client
            .request_token(run_id, purpose, owner, repository, cancel)
            .await?;
        register_repository_token_masks(&self.workflow_commands, &token.token);
        let value = token.token.clone();
        tokens.insert(key, token);
        Ok(value)
    }

    async fn source_token(
        &self,
        run: &RunSpec,
        refresh: bool,
        cancel: &watch::Receiver<bool>,
    ) -> Result<Option<String>> {
        if !refresh {
            if let Some(token) = self
                .cached_token(
                    RepositoryTokenPurpose::Source,
                    &run.repository.owner,
                    &run.repository.name,
                )
                .await
            {
                return Ok(Some(token));
            }
            if !run.checkout_token.is_empty() {
                let initial = ExpiringToken {
                    token: run.checkout_token.clone(),
                    expires_at_epoch_seconds: run
                        .checkout_token_expires_at_epoch_seconds
                        .unwrap_or_default(),
                };
                if initial.is_usable() {
                    return Ok(Some(initial.token));
                }
            }
        }
        if run.installation_id == 0 {
            return Ok(None);
        }
        self.request_token(
            run.id,
            RepositoryTokenPurpose::Source,
            &run.repository.owner,
            &run.repository.name,
            refresh,
            cancel,
        )
        .await
        .map(Some)
    }

    async fn workflow_token(
        &self,
        run: &RunSpec,
        permissions: &PlannedPermissions,
        cancel: &watch::Receiver<bool>,
    ) -> Result<Option<String>> {
        if permissions.read.is_empty() && permissions.write.is_empty() {
            return Ok(None);
        }
        if permissions == &PlannedPermissions::default()
            && let Some(token) = self.source_token(run, false, cancel).await?
        {
            return Ok(Some(token));
        }
        if run.installation_id == 0 {
            return Ok(None);
        }
        let mut tokens = self.workflow_tokens.lock().await;
        if let Some(token) = tokens.get(permissions)
            && token.is_usable()
        {
            return Ok(Some(token.token.clone()));
        } else if let Some(mut token) = tokens.remove(permissions) {
            token.clear();
        }
        if tokens.len() >= MAX_WORKFLOW_TOKEN_SCOPES_PER_RUN {
            bail!(
                "workflow uses more than {MAX_WORKFLOW_TOKEN_SCOPES_PER_RUN} distinct token permission sets"
            );
        }
        let token = self
            .client
            .request_workflow_token(run.id, &permissions.read, &permissions.write, cancel)
            .await?;
        register_repository_token_masks(&self.workflow_commands, &token.token);
        let value = token.token.clone();
        tokens.insert(permissions.clone(), token);
        Ok(Some(value))
    }

    async fn managed_secrets(
        &self,
        run: &RunSpec,
        unit_id: &str,
        environment: Option<&str>,
        cancel: &watch::Receiver<bool>,
    ) -> Result<BTreeMap<String, String>> {
        if !run.managed_secrets {
            return Ok(BTreeMap::new());
        }
        let secrets = self
            .client
            .request_managed_secrets(run.id, unit_id, environment, cancel)
            .await?;
        if secrets.len() > MAX_MANAGED_SECRETS_PER_JOB {
            bail!("control plane returned more than {MAX_MANAGED_SECRETS_PER_JOB} managed secrets");
        }
        for (name, value) in &secrets {
            if !valid_managed_secret_name(name) || value.len() > MAX_MANAGED_SECRET_BYTES {
                bail!("control plane returned an invalid managed secret");
            }
        }
        register_managed_secret_masks(&self.workflow_commands, &secrets);
        Ok(secrets)
    }

    async fn clear(&self) {
        let mut tokens = self.tokens.lock().await;
        for token in tokens.values_mut() {
            token.clear();
        }
        tokens.clear();
        drop(tokens);
        let mut workflow_tokens = self.workflow_tokens.lock().await;
        for token in workflow_tokens.values_mut() {
            token.clear();
        }
        workflow_tokens.clear();
    }
}

fn repository_access_key(owner: &str, repository: &str) -> String {
    format!("{owner}/{repository}").to_ascii_lowercase()
}

#[derive(Clone, Debug)]
pub struct ExecutorConfig {
    pub work_root: PathBuf,
    pub runner_name: String,
    pub keep_failed_workspaces: bool,
    pub max_parallelism: usize,
    pub cache_max_bytes: u64,
    pub cache_max_entry_bytes: u64,
    pub artifact_max_bytes: u64,
    pub artifact_max_entry_bytes: u64,
}

pub struct Executor {
    config: ExecutorConfig,
    execution_slots: Arc<Semaphore>,
    runner_targeting: RunnerTargeting,
}

#[derive(Clone, Debug)]
struct RunnerTargeting {
    labels: BTreeSet<String>,
    group: Option<String>,
}

enum ExecutionDisposition {
    Completed {
        conclusion: Conclusion,
        summary: String,
    },
    Rejected(Vec<RunnerRequirement>),
}

impl Executor {
    pub fn new(config: ExecutorConfig) -> Self {
        assert!(
            (1..=64).contains(&config.max_parallelism),
            "executor max_parallelism must be between 1 and 64"
        );
        assert!(
            config.artifact_max_entry_bytes > 0
                && config.artifact_max_bytes >= config.artifact_max_entry_bytes,
            "executor artifact limits must be positive and internally consistent"
        );
        assert!(
            config.cache_max_entry_bytes > 0
                && config.cache_max_bytes >= config.cache_max_entry_bytes,
            "executor cache limits must be positive and internally consistent"
        );
        let execution_slots = Arc::new(Semaphore::new(config.max_parallelism));
        Self {
            config,
            execution_slots,
            runner_targeting: default_runner_targeting(),
        }
    }

    pub fn with_runner_targeting(
        mut self,
        labels: Vec<String>,
        runner_group: Option<String>,
    ) -> Self {
        self.runner_targeting = RunnerTargeting {
            labels: labels
                .into_iter()
                .map(|label| label.to_ascii_lowercase())
                .collect(),
            group: runner_group.map(|group| group.to_ascii_lowercase()),
        };
        self
    }

    #[cfg(test)]
    pub async fn execute(
        &self,
        run: RunSpec,
        cancel: watch::Receiver<bool>,
        outbound: mpsc::Sender<AgentMessage>,
    ) -> Result<()> {
        self.execute_with_services(
            run,
            cancel,
            outbound,
            ConcurrencyClient::local(),
            RepositoryAccessClient::local(),
        )
        .await
    }

    pub(crate) async fn execute_with_services(
        &self,
        mut run: RunSpec,
        cancel: watch::Receiver<bool>,
        outbound: mpsc::Sender<AgentMessage>,
        concurrency: ConcurrencyClient,
        repository_access: RepositoryAccessClient,
    ) -> Result<()> {
        let head_object_format = validate_sha(&run.pull_request.head_sha)?;
        if validate_sha(&run.pull_request.base_sha)? != head_object_format
            || validate_sha(&run.pull_request.merge_sha)? != head_object_format
        {
            bail!("pull request snapshot object IDs must use one Git object format");
        }
        validate_execution_ref(&run)?;
        validate_checkout_token_metadata(&run)?;
        let run_dir = self.config.work_root.join(run.id.to_string());
        let repository_dir = run_dir.join("repository");
        prepare_directory(&run_dir).await?;
        tokio::fs::create_dir_all(&repository_dir).await?;
        let repository = format!("{}/{}", run.repository.owner, run.repository.name);
        let cache_scope = run.pull_request.execution_ref.clone();
        let workflow_run_backend_id = run.id.to_string();
        let workflow_job_run_backend_id = Uuid::new_v4().to_string();
        let runtime_token =
            actions_runtime_token(&workflow_run_backend_id, &workflow_job_run_backend_id)?;
        let cache_service = CacheService::start(
            &self.config.work_root,
            &run_dir.join("_cache-uploads"),
            &repository,
            &cache_scope,
            &runtime_token,
            CacheLimits {
                maximum_bytes: self.config.cache_max_bytes,
                maximum_entry_bytes: self.config.cache_max_entry_bytes,
            },
        )
        .await?;
        let artifact_service = match ArtifactService::start(
            &run_dir.join("_workflow-artifacts"),
            &runtime_token,
            &workflow_run_backend_id,
            &workflow_job_run_backend_id,
            ArtifactLimits {
                maximum_bytes: self.config.artifact_max_bytes,
                maximum_entry_bytes: self.config.artifact_max_entry_bytes,
            },
        )
        .await
        {
            Ok(service) => service,
            Err(error) => {
                let _ = cache_service.shutdown().await;
                return Err(error.context("start workflow artifact service"));
            }
        };
        let mut secrets = secret_values(&run);
        secrets.push(runtime_token);
        let workflow_commands =
            Arc::new(StdMutex::new(WorkflowCommandProcessor::with_global_masks(
                secrets.clone(),
                repository_dir.clone(),
                run_dir.clone(),
            )));
        let repository_access =
            RunRepositoryAccess::new(repository_access, workflow_commands.clone());
        let (masked_outbound, masked_events) = mpsc::channel(512);
        let relay = tokio::spawn(relay_masked_events(
            masked_events,
            outbound,
            secrets,
            workflow_commands.clone(),
        ));
        let outbound = masked_outbound;

        let sequence = Arc::new(AtomicU64::new(0));
        let job_summaries = Arc::new(Mutex::new(Vec::new()));
        let environment_variable_cache: EnvironmentVariableCache =
            Arc::new(Mutex::new(BTreeMap::new()));
        let execution: Result<ExecutionDisposition> = async {
            let source_token = repository_access.source_token(&run, false, &cancel).await?;
            checkout(
                &run,
                source_token.as_deref(),
                &repository_dir,
                &cancel,
                &outbound,
                &sequence,
            )
            .await?;
            let plans = discover_workflows(
                &repository_dir,
                &run_dir,
                &run,
                &cancel,
                &outbound,
                &sequence,
                &repository_access,
            )
            .await?;
            let runner_requirements =
                statically_resolvable_runner_requirements(&plans, &run, &repository_dir, &run_dir)?;
            if !runner_satisfies_requirements(&runner_requirements, &self.runner_targeting) {
                return Ok(ExecutionDisposition::Rejected(runner_requirements));
            }

            send(
                &outbound,
                AgentMessage::JobStarted {
                    message_id: Uuid::new_v4(),
                    job_id: run.id,
                },
            )
            .await?;
            if plans.is_empty() {
                return Ok(ExecutionDisposition::Completed {
                    conclusion: Conclusion::Neutral,
                    summary:
                        "No workflows matched this pull_request activity at the execution SHA."
                            .to_owned(),
                });
            }

            let tool_cache = self.config.work_root.join("_toolcache");
            tokio::fs::create_dir_all(&tool_cache).await?;
            let mut defaults = github_environment(
                &run,
                &repository_dir,
                &run_dir,
                &tool_cache,
                &self.config.runner_name,
            )
            .await?;
            defaults.extend(cache_service.environment());
            defaults.extend(artifact_service.environment());
            let mut environment = run.environment.clone();
            environment.extend(defaults);
            let mut completed_steps = 0usize;
            let mut failed_jobs = Vec::new();
            let mut timed_out_jobs = Vec::new();
            let mut cancelled_jobs = Vec::new();
            let mut workflow_errors = Vec::new();
            let mut running_plans = FuturesUnordered::new();
            for (plan_index, plan) in plans.iter().enumerate() {
                let workflow_name = plan.workflow_name.clone();
                let execution = execute_plan(
                    run.id,
                    plan,
                    plan_index,
                    &run,
                    &repository_dir,
                    &run_dir,
                    &environment,
                    &cancel,
                    &outbound,
                    &sequence,
                    &self.execution_slots,
                    &self.runner_targeting,
                    &job_summaries,
                    &environment_variable_cache,
                    &workflow_commands,
                    &concurrency,
                    &repository_access,
                );
                running_plans.push(async move { (workflow_name, execution.await) });
            }
            while let Some((workflow_name, result)) = running_plans.next().await {
                match result {
                    Ok(result) => {
                        completed_steps += result.completed_steps;
                        failed_jobs.extend(result.failed_jobs);
                        timed_out_jobs.extend(result.timed_out_jobs);
                        cancelled_jobs.extend(result.cancelled_jobs);
                    }
                    Err(error) => {
                        workflow_errors.push(format!("{workflow_name}: {error:#}"));
                    }
                }
            }
            if *cancel.borrow() {
                bail!("run cancelled");
            }
            if !workflow_errors.is_empty() {
                workflow_errors.sort();
                bail!("workflow execution errors: {}", workflow_errors.join("; "));
            }
            if !failed_jobs.is_empty() {
                failed_jobs.sort();
                bail!("workflow jobs failed: {}", failed_jobs.join(", "));
            }
            if !timed_out_jobs.is_empty() {
                timed_out_jobs.sort();
                return Err(RunTimedOut(timed_out_jobs.join(", ")).into());
            }
            if !cancelled_jobs.is_empty() {
                cancelled_jobs.sort();
                return Err(RunConcurrencyCancelled(cancelled_jobs.join(", ")).into());
            }
            Ok(ExecutionDisposition::Completed {
                conclusion: Conclusion::Success,
                summary: format!("GitZero completed {completed_steps} step(s) successfully."),
            })
        }
        .await;
        let artifact_shutdown = artifact_service.shutdown().await;
        let cache_shutdown = cache_service.shutdown().await;
        let execution = match (execution, artifact_shutdown, cache_shutdown) {
            (Err(error), _, _) => Err(error),
            (Ok(_), Err(error), _) => Err(error.context("shut down workflow artifact service")),
            (Ok(_), Ok(()), Err(error)) => Err(error.context("shut down workflow cache service")),
            (Ok(summary), Ok(()), Ok(())) => Ok(summary),
        };
        repository_access.clear().await;
        run.checkout_token.clear();

        let execution = match execution {
            Ok(ExecutionDisposition::Completed {
                conclusion,
                summary,
            }) => Ok((conclusion, summary)),
            Ok(ExecutionDisposition::Rejected(requirements)) => {
                let rejection_send = send(
                    &outbound,
                    AgentMessage::JobRejected {
                        message_id: Uuid::new_v4(),
                        job_id: run.id,
                        requirements,
                        reason:
                            "This Mac does not satisfy every statically resolvable runner selector."
                                .to_owned(),
                    },
                )
                .await;
                if let Err(error) = tokio::fs::remove_dir_all(&run_dir).await {
                    info!(path = %run_dir.display(), %error, "failed to clean rejected run directory");
                }
                drop(outbound);
                let relay_result = relay.await.context("join event masking relay")?;
                rejection_send?;
                relay_result?;
                return Ok(());
            }
            Err(error) => Err(error),
        };

        let (conclusion, summary) = match execution {
            Ok((conclusion, summary)) => (conclusion, summary),
            Err(_error) if *cancel.borrow() => (Conclusion::Cancelled, "Run cancelled.".to_owned()),
            Err(error) => {
                if let Some(timeout) = error.downcast_ref::<RunTimedOut>() {
                    (Conclusion::TimedOut, timeout.to_string())
                } else if let Some(cancelled) = error.downcast_ref::<RunConcurrencyCancelled>() {
                    (Conclusion::Cancelled, cancelled.to_string())
                } else {
                    (Conclusion::Failure, format!("{error:#}"))
                }
            }
        };
        let summaries = std::mem::take(&mut *job_summaries.lock().await);
        let summary = compose_check_summary(summary, summaries);

        let completion_send = send(
            &outbound,
            AgentMessage::JobFinished {
                message_id: Uuid::new_v4(),
                job_id: run.id,
                conclusion,
                summary,
                annotations: Vec::new(),
            },
        )
        .await;

        if (conclusion == Conclusion::Success || !self.config.keep_failed_workspaces)
            && let Err(error) = tokio::fs::remove_dir_all(&run_dir).await
        {
            info!(path = %run_dir.display(), %error, "failed to clean run directory");
        }
        drop(outbound);
        let relay_result = relay.await.context("join event masking relay")?;
        completion_send?;
        relay_result?;
        Ok(())
    }
}

async fn checkout(
    run: &RunSpec,
    checkout_token: Option<&str>,
    repository_dir: &Path,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
) -> Result<()> {
    let step_id = "gitzero-checkout";
    send(
        outbound,
        AgentMessage::StepStarted {
            message_id: Uuid::new_v4(),
            job_id: run.id,
            step_id: step_id.to_owned(),
            name: "Checkout repository".to_owned(),
        },
    )
    .await?;

    let mut initialize = Command::new("git");
    initialize.args(["init", "--quiet"]);
    configure_git_init_object_format(&mut initialize, &run.pull_request.merge_sha)?;
    initialize.current_dir(repository_dir);
    run_process(
        run.id,
        step_id,
        &mut initialize,
        None,
        cancel.clone(),
        outbound.clone(),
        sequence.clone(),
    )
    .await
    .context("initialize repository")?;

    run_process(
        run.id,
        step_id,
        Command::new("git")
            .args(["remote", "add", "origin", &run.repository.clone_url])
            .current_dir(repository_dir),
        None,
        cancel.clone(),
        outbound.clone(),
        sequence.clone(),
    )
    .await
    .context("configure repository remote")?;

    let mut fetch = Command::new("git");
    fetch
        .args([
            "fetch",
            "--quiet",
            "--no-tags",
            "--depth=1",
            "origin",
            &run.pull_request.merge_sha,
        ])
        .env("GIT_TERMINAL_PROMPT", "0")
        .current_dir(repository_dir);
    if let Some(checkout_token) = checkout_token {
        let credential = STANDARD.encode(format!("x-access-token:{checkout_token}"));
        fetch
            .env("GIT_CONFIG_COUNT", "1")
            .env("GIT_CONFIG_KEY_0", "http.https://github.com/.extraheader")
            .env(
                "GIT_CONFIG_VALUE_0",
                format!("AUTHORIZATION: basic {credential}"),
            );
    }
    run_process(
        run.id,
        step_id,
        &mut fetch,
        None,
        cancel.clone(),
        outbound.clone(),
        sequence.clone(),
    )
    .await
    .context("fetch exact pull request merge snapshot")?;

    run_process(
        run.id,
        step_id,
        Command::new("git")
            .args(["checkout", "--quiet", "--detach", "FETCH_HEAD"])
            .current_dir(repository_dir),
        None,
        cancel.clone(),
        outbound.clone(),
        sequence.clone(),
    )
    .await
    .context("checkout pull request merge snapshot")?;

    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(repository_dir)
        .output()
        .await
        .context("verify checkout")?;
    let actual = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if !output.status.success() || !actual.eq_ignore_ascii_case(&run.pull_request.merge_sha) {
        bail!(
            "checkout verification failed: expected {}, got {actual}",
            run.pull_request.merge_sha
        );
    }

    send(
        outbound,
        AgentMessage::StepFinished {
            message_id: Uuid::new_v4(),
            job_id: run.id,
            step_id: step_id.to_owned(),
            conclusion: Conclusion::Success,
            exit_code: Some(0),
        },
    )
    .await
}

async fn discover_workflows(
    repository_dir: &Path,
    run_dir: &Path,
    run: &RunSpec,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
    repository_access: &RunRepositoryAccess,
) -> Result<Vec<ExecutionPlan>> {
    let directory = repository_dir.join(".github/workflows");
    let mut entries = match tokio::fs::read_dir(&directory).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error).context("read workflow directory"),
    };
    let mut paths = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| matches!(extension, "yml" | "yaml"))
        {
            paths.push(path);
        }
    }
    paths.sort();

    let mut workflows = Vec::new();
    for path in paths {
        let source = tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("read workflow {}", path.display()))?;
        let workflow = parse(&source).with_context(|| format!("parse {}", path.display()))?;
        workflows.push((path, workflow));
    }

    let needs_changed_paths = workflows
        .iter()
        .any(|(_, workflow)| requires_pull_request_changed_paths(workflow));
    let changed_paths = if needs_changed_paths {
        let paths = match &run.changed_paths {
            Some(paths) => paths.clone(),
            None => {
                let source_token = repository_access.source_token(run, false, cancel).await?;
                fetch_pull_request_changed_paths(run, source_token.as_deref(), cancel).await?
            }
        };
        validate_changed_paths(&paths)?;
        Some(paths)
    } else {
        None
    };

    let local_reusable_workflows = workflows
        .iter()
        .map(|(path, workflow)| {
            let relative = path
                .strip_prefix(repository_dir)
                .expect("workflow directory is inside repository");
            (
                relative.to_string_lossy().replace('\\', "/"),
                workflow.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();

    let matching = workflows
        .iter()
        .filter_map(|(path, workflow)| {
            matches_pull_request(
                workflow,
                &run.pull_request.action,
                &run.pull_request.base_ref,
                changed_paths.as_deref(),
            )
            .map(|matches| matches.then(|| path.clone()))
            .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let root_keys = matching
        .iter()
        .map(|path| {
            path.strip_prefix(repository_dir)
                .expect("workflow directory is inside repository")
                .to_string_lossy()
                .replace('\\', "/")
        })
        .collect::<Vec<_>>();
    let reusable_workflows = resolve_reusable_workflow_catalog(
        local_reusable_workflows,
        &root_keys,
        run_dir,
        run,
        cancel,
        outbound,
        sequence,
        repository_access,
    )
    .await?;

    let mut plans = Vec::new();
    for (path, key) in matching.into_iter().zip(root_keys) {
        let workflow = &reusable_workflows[&key];
        plans.push(
            compile_with_reusables(workflow, Path::new(&key), &reusable_workflows)
                .with_context(|| format!("compile {}", path.display()))?,
        );
    }
    Ok(plans)
}

#[derive(Clone)]
enum ReusableWorkflowOrigin {
    Local,
    Remote(RemoteReusableWorkflowReference),
}

#[derive(Clone)]
struct ReusableWorkflowSource {
    workflow: Workflow,
    origin: ReusableWorkflowOrigin,
}

#[allow(clippy::too_many_arguments)]
async fn resolve_reusable_workflow_catalog(
    local_workflows: BTreeMap<String, Workflow>,
    roots: &[String],
    run_dir: &Path,
    run: &RunSpec,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
    repository_access: &RunRepositoryAccess,
) -> Result<BTreeMap<String, Workflow>> {
    let mut sources = local_workflows
        .into_iter()
        .map(|(key, workflow)| {
            (
                key,
                ReusableWorkflowSource {
                    workflow,
                    origin: ReusableWorkflowOrigin::Local,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut pending = roots.iter().cloned().collect::<VecDeque<_>>();
    let mut processed = BTreeSet::new();
    let mut reachable_calls = BTreeSet::new();
    while let Some(source_key) = pending.pop_front() {
        if !processed.insert(source_key.clone()) {
            continue;
        }
        let Some(source) = sources.get(&source_key) else {
            continue;
        };
        let origin = source.origin.clone();
        let source_name = source.workflow.name.clone();
        let calls = source
            .workflow
            .jobs
            .iter()
            .filter_map(|(job_id, job)| {
                job.uses.as_ref().map(|uses| (job_id.clone(), uses.clone()))
            })
            .collect::<Vec<_>>();
        for (job_id, uses) in calls {
            let local_path = local_reusable_reference_path(&uses)
                .transpose()
                .with_context(|| {
                    format!(
                        "resolve reusable workflow called by '{}' job '{job_id}'",
                        source_name
                    )
                })?;
            let (target, remote) = match (&origin, local_path) {
                (ReusableWorkflowOrigin::Local, Some(path)) => (path, None),
                (ReusableWorkflowOrigin::Remote(remote), Some(path)) => {
                    let target = remote.canonical_reference(&path);
                    sources
                        .get_mut(&source_key)
                        .expect("source remains in catalog")
                        .workflow
                        .jobs
                        .get_mut(&job_id)
                        .expect("call job remains in workflow")
                        .uses = Some(target.clone());
                    let reference = RemoteReusableWorkflowReference::parse(&target)?;
                    (target, Some(reference))
                }
                (_, None) => {
                    let reference =
                        RemoteReusableWorkflowReference::parse(&uses).with_context(|| {
                            format!(
                                "resolve reusable workflow called by '{}' job '{job_id}'",
                                source_name
                            )
                        })?;
                    (uses, Some(reference))
                }
            };
            reachable_calls.insert(target.clone());
            if reachable_calls.len() > MAX_UNIQUE_REUSABLE_WORKFLOWS {
                bail!(
                    "workflow tree calls more than {MAX_UNIQUE_REUSABLE_WORKFLOWS} unique reusable workflows"
                );
            }
            if sources.contains_key(&target) {
                pending.push_back(target);
                continue;
            }
            let Some(remote) = remote else {
                continue;
            };
            let workflow = load_remote_reusable_workflow(
                &remote,
                run_dir,
                run,
                cancel,
                outbound,
                sequence,
                repository_access,
            )
            .await
            .with_context(|| format!("load remote reusable workflow '{target}'"))?;
            sources.insert(
                target.clone(),
                ReusableWorkflowSource {
                    workflow,
                    origin: ReusableWorkflowOrigin::Remote(remote),
                },
            );
            pending.push_back(target);
        }
    }
    Ok(sources
        .into_iter()
        .map(|(key, source)| (key, source.workflow))
        .collect())
}

fn local_reusable_reference_path(source: &str) -> Option<Result<String>> {
    source
        .strip_prefix("./")
        .or_else(|| source.strip_prefix("$/"))
        .map(|path| {
            let path = Path::new(path);
            if path.is_absolute()
                || path.components().any(|component| {
                    matches!(
                        component,
                        std::path::Component::ParentDir | std::path::Component::RootDir
                    )
                })
                || path.parent() != Some(Path::new(".github/workflows"))
                || !path
                    .extension()
                    .and_then(|extension| extension.to_str())
                    .is_some_and(|extension| matches!(extension, "yml" | "yaml"))
            {
                bail!(
                    "reusable workflow path '{source}' must name a file directly in .github/workflows"
                );
            }
            Ok(path.to_string_lossy().replace('\\', "/"))
        })
}

async fn load_remote_reusable_workflow(
    reference: &RemoteReusableWorkflowReference,
    run_dir: &Path,
    run: &RunSpec,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
    repository_access: &RunRepositoryAccess,
) -> Result<Workflow> {
    let same_repository = reference.owner.eq_ignore_ascii_case(&run.repository.owner)
        && reference
            .repository
            .eq_ignore_ascii_case(&run.repository.name);
    let remote = if same_repository {
        run.repository.clone_url.clone()
    } else {
        format!(
            "https://github.com/{}/{}.git",
            reference.owner, reference.repository
        )
    };
    let checkout = materialize_remote_repository_with_shared_access(
        run.id,
        "gitzero-reusable-workflows",
        &reference.owner,
        &reference.repository,
        &reference.git_ref,
        &remote,
        run,
        repository_access,
        run_dir,
        cancel,
        outbound,
        sequence,
    )
    .await?;
    let path = checkout.join(&reference.path);
    ensure_within(&checkout, &path).await?;
    let source = tokio::fs::read_to_string(&path)
        .await
        .with_context(|| format!("read reusable workflow {}", path.display()))?;
    parse(&source).with_context(|| format!("parse reusable workflow {}", path.display()))
}

#[derive(Deserialize)]
struct PullRequestFile {
    filename: String,
}

#[derive(Deserialize)]
struct GitHubEnvironmentMetadata {
    name: String,
    #[serde(default)]
    protection_rules: Vec<JsonValue>,
    #[serde(default)]
    deployment_branch_policy: Option<GitHubDeploymentBranchPolicySettings>,
}

#[derive(Deserialize)]
struct GitHubDeploymentBranchPolicySettings {
    protected_branches: bool,
    custom_branch_policies: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeploymentBranchPolicyMode {
    Unrestricted,
    ProtectedBranches,
    Custom,
}

#[derive(Clone, Copy)]
struct GitHubEnvironmentRequest<'a> {
    api_base: &'a str,
    owner: &'a str,
    repository: &'a str,
    environment_name: &'a str,
    github_ref: &'a str,
    api_version: &'a str,
    token: &'a str,
}

#[derive(Deserialize)]
struct DeploymentBranchPolicyPage {
    total_count: usize,
    branch_policies: Vec<DeploymentBranchPolicy>,
}

#[derive(Deserialize)]
struct DeploymentBranchPolicy {
    name: String,
    #[serde(rename = "type")]
    policy_type: String,
}

#[derive(Deserialize)]
struct GitHubBranch {
    name: String,
    protected: bool,
}

#[derive(Deserialize)]
struct ActionsVariablePage {
    total_count: usize,
    variables: Vec<ActionsVariable>,
}

#[derive(Deserialize)]
struct ActionsVariable {
    name: String,
    value: String,
}

async fn environment_variables_for_job(
    run: &RunSpec,
    environment_name: &str,
    cache: &EnvironmentVariableCache,
    repository_access: &RunRepositoryAccess,
    cancel: &watch::Receiver<bool>,
) -> Result<BTreeMap<String, String>> {
    let cache_key = environment_name.to_lowercase();
    let mut cache = cache.lock().await;
    if let Some(variables) = cache.get(&cache_key) {
        return Ok(variables.clone());
    }
    if run.installation_id == 0 {
        bail!(
            "job references GitHub environment '{environment_name}', but no GitHub App installation is available"
        );
    }
    let environment_token = repository_access
        .request_token(
            run.id,
            RepositoryTokenPurpose::Environment,
            &run.repository.owner,
            &run.repository.name,
            false,
            cancel,
        )
        .await
        .with_context(|| {
            format!("issue environment-read token for GitHub environment '{environment_name}'")
        })?;
    let variables = fetch_github_environment_variables_from(
        GitHubEnvironmentRequest {
            api_base: "https://api.github.com",
            owner: &run.repository.owner,
            repository: &run.repository.name,
            environment_name,
            github_ref: &run.pull_request.execution_ref,
            api_version: &run.github_api_version,
            token: &environment_token,
        },
        cancel,
    )
    .await?;
    cache.insert(cache_key, variables.clone());
    Ok(variables)
}

fn merge_configuration_variables(
    base: &BTreeMap<String, String>,
    environment: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut selected = base.clone();
    for (name, value) in environment {
        if let Some(existing) = selected
            .keys()
            .find(|existing| existing.eq_ignore_ascii_case(&name))
            .cloned()
        {
            selected.remove(&existing);
        }
        selected.insert(name, value);
    }
    selected
}

async fn fetch_github_environment_variables_from(
    request: GitHubEnvironmentRequest<'_>,
    cancel: &watch::Receiver<bool>,
) -> Result<BTreeMap<String, String>> {
    let client = reqwest::Client::builder()
        .user_agent(format!("gitzero-agent/{}", env!("CARGO_PKG_VERSION")))
        .timeout(Duration::from_secs(30))
        .build()
        .context("build GitHub environment API client")?;
    let metadata_endpoint = github_repository_api_url(
        request.api_base,
        request.owner,
        request.repository,
        &["environments", request.environment_name],
    )?;
    let metadata: GitHubEnvironmentMetadata = github_api_get_json(
        &client,
        metadata_endpoint,
        request.api_version,
        request.token,
        cancel,
        &format!("get environment '{}'", request.environment_name),
    )
    .await?;
    let branch_policy = validate_environment_metadata(request.environment_name, &metadata)?;
    enforce_environment_branch_policy(&client, request, branch_policy, cancel).await?;

    let mut endpoint = github_repository_api_url(
        request.api_base,
        request.owner,
        request.repository,
        &["environments", request.environment_name, "variables"],
    )?;
    let maximum_pages = MAX_ENVIRONMENT_VARIABLES.div_ceil(ENVIRONMENT_VARIABLE_PAGE_SIZE);
    let mut selected = BTreeMap::new();
    let mut normalized_names = BTreeSet::new();
    for page in 1..=maximum_pages {
        endpoint
            .query_pairs_mut()
            .clear()
            .append_pair("per_page", &ENVIRONMENT_VARIABLE_PAGE_SIZE.to_string())
            .append_pair("page", &page.to_string());
        let response: ActionsVariablePage = github_api_get_json(
            &client,
            endpoint.clone(),
            request.api_version,
            request.token,
            cancel,
            &format!(
                "list environment '{}' variables page {page}",
                request.environment_name
            ),
        )
        .await?;
        if response.total_count > MAX_ENVIRONMENT_VARIABLES {
            bail!(
                "GitHub environment '{}' reports more than {MAX_ENVIRONMENT_VARIABLES} variables",
                request.environment_name
            );
        }
        if response.variables.len() > ENVIRONMENT_VARIABLE_PAGE_SIZE {
            bail!(
                "GitHub environment '{}' returned too many variables on page {page}",
                request.environment_name
            );
        }
        let page_len = response.variables.len();
        for variable in response.variables {
            insert_environment_variable(
                request.environment_name,
                variable,
                &mut selected,
                &mut normalized_names,
            )?;
        }
        if page_len < ENVIRONMENT_VARIABLE_PAGE_SIZE || selected.len() >= response.total_count {
            break;
        }
    }
    Ok(selected)
}

fn validate_environment_metadata(
    requested_name: &str,
    metadata: &GitHubEnvironmentMetadata,
) -> Result<DeploymentBranchPolicyMode> {
    if !metadata.name.eq_ignore_ascii_case(requested_name) {
        bail!(
            "GitHub returned environment '{}' while '{}' was requested",
            metadata.name,
            requested_name
        );
    }
    if !metadata.protection_rules.is_empty() {
        bail!(
            "GitHub environment '{requested_name}' has reviewer, wait-timer, or custom deployment protection rules that GitZero cannot safely emulate"
        );
    }
    match &metadata.deployment_branch_policy {
        None => Ok(DeploymentBranchPolicyMode::Unrestricted),
        Some(policy) => match (policy.protected_branches, policy.custom_branch_policies) {
            (true, false) => Ok(DeploymentBranchPolicyMode::ProtectedBranches),
            (false, true) => Ok(DeploymentBranchPolicyMode::Custom),
            _ => bail!(
                "GitHub environment '{requested_name}' returned an invalid deployment branch policy"
            ),
        },
    }
}

async fn enforce_environment_branch_policy(
    client: &reqwest::Client,
    request: GitHubEnvironmentRequest<'_>,
    policy: DeploymentBranchPolicyMode,
    cancel: &watch::Receiver<bool>,
) -> Result<()> {
    match policy {
        DeploymentBranchPolicyMode::Unrestricted => Ok(()),
        DeploymentBranchPolicyMode::ProtectedBranches => {
            enforce_protected_branch_policy(client, request, cancel).await
        }
        DeploymentBranchPolicyMode::Custom => {
            enforce_custom_branch_policy(client, request, cancel).await
        }
    }
}

async fn enforce_custom_branch_policy(
    client: &reqwest::Client,
    request: GitHubEnvironmentRequest<'_>,
    cancel: &watch::Receiver<bool>,
) -> Result<()> {
    let (reference_type, reference_name) = deployment_policy_reference(request.github_ref)?;
    let mut endpoint = github_repository_api_url(
        request.api_base,
        request.owner,
        request.repository,
        &[
            "environments",
            request.environment_name,
            "deployment-branch-policies",
        ],
    )?;
    let maximum_pages = MAX_DEPLOYMENT_BRANCH_POLICIES.div_ceil(DEPLOYMENT_BRANCH_POLICY_PAGE_SIZE);
    let mut received = 0usize;
    let mut pattern_bytes = 0usize;
    let mut matched = false;
    let mut reported_total = None;
    for page in 1..=maximum_pages {
        endpoint
            .query_pairs_mut()
            .clear()
            .append_pair("per_page", &DEPLOYMENT_BRANCH_POLICY_PAGE_SIZE.to_string())
            .append_pair("page", &page.to_string());
        let response: DeploymentBranchPolicyPage = github_api_get_json(
            client,
            endpoint.clone(),
            request.api_version,
            request.token,
            cancel,
            &format!(
                "list environment '{}' deployment branch policies page {page}",
                request.environment_name
            ),
        )
        .await?;
        if response.total_count > MAX_DEPLOYMENT_BRANCH_POLICIES {
            bail!(
                "GitHub environment '{}' reports more than {MAX_DEPLOYMENT_BRANCH_POLICIES} deployment branch policies",
                request.environment_name
            );
        }
        if reported_total
            .replace(response.total_count)
            .is_some_and(|total| total != response.total_count)
        {
            bail!(
                "GitHub environment '{}' changed its deployment branch policy count during pagination",
                request.environment_name
            );
        }
        if response.branch_policies.len() > DEPLOYMENT_BRANCH_POLICY_PAGE_SIZE {
            bail!(
                "GitHub environment '{}' returned too many deployment branch policies on page {page}",
                request.environment_name
            );
        }
        let page_len = response.branch_policies.len();
        for branch_policy in response.branch_policies {
            if branch_policy.name.is_empty()
                || branch_policy.name.len() > MAX_DEPLOYMENT_BRANCH_PATTERN_BYTES
                || branch_policy.name.contains(['\0', '\n', '\r'])
                || !matches!(branch_policy.policy_type.as_str(), "branch" | "tag")
            {
                bail!(
                    "GitHub environment '{}' returned an invalid deployment branch policy",
                    request.environment_name
                );
            }
            pattern_bytes = pattern_bytes
                .checked_add(branch_policy.name.len())
                .context("deployment branch policy size overflow")?;
            if pattern_bytes > MAX_DEPLOYMENT_BRANCH_PATTERN_TOTAL_BYTES {
                bail!(
                    "GitHub environment '{}' deployment branch policies exceed the size limit",
                    request.environment_name
                );
            }
            matched |= branch_policy.policy_type == reference_type
                && deployment_pattern_matches(&branch_policy.name, reference_name);
            received += 1;
        }
        if page_len < DEPLOYMENT_BRANCH_POLICY_PAGE_SIZE || received >= response.total_count {
            break;
        }
    }
    let expected = reported_total.unwrap_or_default();
    if received != expected {
        bail!(
            "GitHub environment '{}' returned {received} of {expected} deployment branch policies",
            request.environment_name
        );
    }
    if !matched {
        bail!(
            "GitHub environment '{}' deployment branch policies do not allow ref '{}'",
            request.environment_name,
            request.github_ref
        );
    }
    Ok(())
}

async fn enforce_protected_branch_policy(
    client: &reqwest::Client,
    request: GitHubEnvironmentRequest<'_>,
    cancel: &watch::Receiver<bool>,
) -> Result<()> {
    let mut protected_endpoint = github_repository_api_url(
        request.api_base,
        request.owner,
        request.repository,
        &["branches"],
    )?;
    protected_endpoint
        .query_pairs_mut()
        .append_pair("protected", "true")
        .append_pair("per_page", "1")
        .append_pair("page", "1");
    let protected: Vec<GitHubBranch> = github_api_get_json(
        client,
        protected_endpoint,
        request.api_version,
        request.token,
        cancel,
        "list protected repository branches",
    )
    .await?;
    if protected.len() > 1 {
        bail!("GitHub returned too many protected repository branches");
    }
    if protected.iter().any(|branch| !branch.protected) {
        bail!("GitHub returned an invalid protected repository branch");
    }
    if protected.is_empty() {
        return Ok(());
    }
    let Some(branch) = request.github_ref.strip_prefix("refs/heads/") else {
        bail!(
            "GitHub environment '{}' allows only protected branches, and ref '{}' is not a repository branch",
            request.environment_name,
            request.github_ref
        );
    };
    if branch.is_empty() {
        bail!("GitHub returned an invalid workflow ref");
    }
    let branch_endpoint = github_repository_api_url(
        request.api_base,
        request.owner,
        request.repository,
        &["branches", branch],
    )?;
    let selected: GitHubBranch = github_api_get_json(
        client,
        branch_endpoint,
        request.api_version,
        request.token,
        cancel,
        &format!("get repository branch '{branch}'"),
    )
    .await?;
    if selected.name != branch {
        bail!(
            "GitHub returned branch '{}' while '{}' was requested",
            selected.name,
            branch
        );
    }
    if !selected.protected {
        bail!(
            "GitHub environment '{}' allows only protected branches, and branch '{branch}' is not protected",
            request.environment_name
        );
    }
    Ok(())
}

fn deployment_policy_reference(github_ref: &str) -> Result<(&'static str, &str)> {
    if let Some(branch) = github_ref.strip_prefix("refs/heads/")
        && !branch.is_empty()
    {
        return Ok(("branch", branch));
    }
    if let Some(tag) = github_ref.strip_prefix("refs/tags/")
        && !tag.is_empty()
    {
        return Ok(("tag", tag));
    }
    if github_ref.starts_with("refs/pull/") {
        return Ok(("branch", github_ref));
    }
    bail!("workflow ref '{github_ref}' cannot be evaluated against deployment branch policies")
}

fn deployment_pattern_matches(pattern: &str, candidate: &str) -> bool {
    fn character_class(pattern: &[char], start: usize, value: char) -> Option<(usize, bool)> {
        let mut index = start + 1;
        let negated = pattern
            .get(index)
            .is_some_and(|character| matches!(character, '!' | '^'));
        if negated {
            index += 1;
        }
        let mut values = Vec::new();
        while index < pattern.len() && pattern[index] != ']' {
            let (character, escaped) = if pattern[index] == '\\' {
                index += 1;
                (*pattern.get(index)?, true)
            } else {
                (pattern[index], false)
            };
            values.push((character, escaped));
            index += 1;
        }
        if index >= pattern.len() || values.is_empty() {
            return None;
        }
        let mut matched = false;
        let mut value_index = 0;
        while value_index < values.len() {
            if value_index + 2 < values.len() && values[value_index + 1] == ('-', false) {
                let start = values[value_index].0;
                let end = values[value_index + 2].0;
                matched |= start <= end && (start..=end).contains(&value);
                value_index += 3;
            } else {
                matched |= values[value_index].0 == value;
                value_index += 1;
            }
        }
        Some((index + 1, if negated { !matched } else { matched }))
    }

    fn matches(
        pattern: &[char],
        candidate: &[char],
        pattern_index: usize,
        candidate_index: usize,
        memo: &mut HashMap<(usize, usize), bool>,
    ) -> bool {
        if let Some(result) = memo.get(&(pattern_index, candidate_index)) {
            return *result;
        }
        let result = if pattern_index == pattern.len() {
            candidate_index == candidate.len()
        } else {
            let at_segment_start = candidate_index == 0
                || candidate
                    .get(candidate_index.wrapping_sub(1))
                    .is_some_and(|character| *character == '/');
            match pattern[pattern_index] {
                '\\' => pattern.get(pattern_index + 1).is_some_and(|literal| {
                    candidate.get(candidate_index) == Some(literal)
                        && matches(
                            pattern,
                            candidate,
                            pattern_index + 2,
                            candidate_index + 1,
                            memo,
                        )
                }),
                '/' => {
                    candidate.get(candidate_index) == Some(&'/')
                        && matches(
                            pattern,
                            candidate,
                            pattern_index + 1,
                            candidate_index + 1,
                            memo,
                        )
                }
                '?' => candidate.get(candidate_index).is_some_and(|character| {
                    *character != '/'
                        && !(at_segment_start && *character == '.')
                        && matches(
                            pattern,
                            candidate,
                            pattern_index + 1,
                            candidate_index + 1,
                            memo,
                        )
                }),
                '[' => candidate.get(candidate_index).is_some_and(|character| {
                    *character != '/'
                        && !(at_segment_start && *character == '.')
                        && character_class(pattern, pattern_index, *character).is_some_and(
                            |(next_pattern, class_matches)| {
                                class_matches
                                    && matches(
                                        pattern,
                                        candidate,
                                        next_pattern,
                                        candidate_index + 1,
                                        memo,
                                    )
                            },
                        )
                }),
                '*' => {
                    let mut after_stars = pattern_index + 1;
                    while pattern.get(after_stars) == Some(&'*') {
                        after_stars += 1;
                    }
                    let globstar_directory = after_stars >= pattern_index + 2
                        && pattern.get(after_stars) == Some(&'/')
                        && (pattern_index == 0 || pattern[pattern_index - 1] == '/');
                    if globstar_directory {
                        matches(pattern, candidate, after_stars + 1, candidate_index, memo)
                            || candidate.get(candidate_index).is_some_and(|first| {
                                if *first == '.' && at_segment_start {
                                    return false;
                                }
                                let Some(relative_slash) = candidate[candidate_index..]
                                    .iter()
                                    .position(|character| *character == '/')
                                else {
                                    return false;
                                };
                                matches(
                                    pattern,
                                    candidate,
                                    pattern_index,
                                    candidate_index + relative_slash + 1,
                                    memo,
                                )
                            })
                    } else if at_segment_start && candidate.get(candidate_index) == Some(&'.') {
                        false
                    } else {
                        matches(pattern, candidate, after_stars, candidate_index, memo)
                            || candidate.get(candidate_index).is_some_and(|character| {
                                *character != '/'
                                    && matches(
                                        pattern,
                                        candidate,
                                        pattern_index,
                                        candidate_index + 1,
                                        memo,
                                    )
                            })
                    }
                }
                literal => {
                    candidate.get(candidate_index) == Some(&literal)
                        && matches(
                            pattern,
                            candidate,
                            pattern_index + 1,
                            candidate_index + 1,
                            memo,
                        )
                }
            }
        };
        memo.insert((pattern_index, candidate_index), result);
        result
    }

    if pattern.is_empty()
        || pattern.len() > MAX_DEPLOYMENT_BRANCH_PATTERN_BYTES
        || pattern.contains(['\0', '\n', '\r'])
    {
        return false;
    }
    matches(
        &pattern.chars().collect::<Vec<_>>(),
        &candidate.chars().collect::<Vec<_>>(),
        0,
        0,
        &mut HashMap::new(),
    )
}

fn insert_environment_variable(
    environment_name: &str,
    variable: ActionsVariable,
    selected: &mut BTreeMap<String, String>,
    normalized_names: &mut BTreeSet<String>,
) -> Result<()> {
    if variable.name.is_empty()
        || variable.name.contains(['\0', '\n', '\r'])
        || variable.value.len() > MAX_CONFIGURATION_VARIABLE_BYTES
    {
        bail!("GitHub environment '{environment_name}' returned an invalid configuration variable");
    }
    let normalized = variable.name.to_uppercase();
    if !normalized_names.insert(normalized) {
        bail!(
            "GitHub environment '{environment_name}' returned duplicate variable '{}'",
            variable.name
        );
    }
    selected.insert(variable.name, variable.value);
    Ok(())
}

fn github_repository_api_url(
    api_base: &str,
    owner: &str,
    repository: &str,
    suffix: &[&str],
) -> Result<reqwest::Url> {
    let mut endpoint = reqwest::Url::parse(api_base)?;
    endpoint
        .path_segments_mut()
        .map_err(|_| anyhow::anyhow!("GitHub API base URL cannot accept path segments"))?
        .pop_if_empty()
        .extend(["repos", owner, repository])
        .extend(suffix.iter().copied());
    Ok(endpoint)
}

async fn github_api_get_json<T: DeserializeOwned>(
    client: &reqwest::Client,
    endpoint: reqwest::Url,
    api_version: &str,
    token: &str,
    cancel: &watch::Receiver<bool>,
    operation: &str,
) -> Result<T> {
    let request = client
        .get(endpoint)
        .header("Accept", "application/vnd.github+json")
        .header("X-GitHub-Api-Version", api_version)
        .bearer_auth(token);
    let mut cancellation = cancel.clone();
    let response = tokio::select! {
        response = request.send() => response.with_context(|| operation.to_owned())?,
        _ = cancellation.changed() => bail!("run cancelled while attempting to {operation}"),
    };
    let status = response.status();
    if response
        .content_length()
        .is_some_and(|length| length > MAX_GITHUB_API_RESPONSE_BYTES as u64)
    {
        bail!("GitHub API response for {operation} exceeded the size limit");
    }
    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("read GitHub API response for {operation}"))?;
    if bytes.len() > MAX_GITHUB_API_RESPONSE_BYTES {
        bail!("GitHub API response for {operation} exceeded the size limit");
    }
    if !status.is_success() {
        let body = String::from_utf8_lossy(&bytes);
        bail!(
            "GitHub API {operation} failed ({status}): {}",
            truncate_text(&body, 4_096)
        );
    }
    serde_json::from_slice(&bytes)
        .with_context(|| format!("decode GitHub API {operation} response"))
}

async fn fetch_pull_request_changed_paths(
    run: &RunSpec,
    token: Option<&str>,
    cancel: &watch::Receiver<bool>,
) -> Result<Vec<String>> {
    let mut endpoint = reqwest::Url::parse("https://api.github.com")?;
    endpoint
        .path_segments_mut()
        .map_err(|_| anyhow::anyhow!("GitHub API base URL cannot accept path segments"))?
        .extend([
            "repos",
            &run.repository.owner,
            &run.repository.name,
            "pulls",
            &run.pull_request.number.to_string(),
            "files",
        ]);
    let client = reqwest::Client::builder()
        .user_agent(format!("gitzero-agent/{}", env!("CARGO_PKG_VERSION")))
        .timeout(Duration::from_secs(30))
        .build()
        .context("build GitHub API client")?;
    let mut paths = Vec::new();
    let maximum_pages = MAX_PULL_REQUEST_FILES / PULL_REQUEST_FILES_PER_PAGE;
    for page in 1..=maximum_pages {
        endpoint
            .query_pairs_mut()
            .clear()
            .append_pair("per_page", &PULL_REQUEST_FILES_PER_PAGE.to_string())
            .append_pair("page", &page.to_string());
        let mut request = client
            .get(endpoint.clone())
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", &run.github_api_version);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let mut cancellation = cancel.clone();
        let response = tokio::select! {
            response = request.send() => response
                .with_context(|| format!("list pull request files page {page}"))?,
            _ = cancellation.changed() => bail!("run cancelled while listing pull request files"),
        };
        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "response body unavailable".to_owned());
            bail!(
                "GitHub API list pull request files page {page} failed ({status}): {}",
                truncate_text(&body, 4_096)
            );
        }
        let files = response
            .json::<Vec<PullRequestFile>>()
            .await
            .with_context(|| format!("decode pull request files page {page}"))?;
        if files.len() > PULL_REQUEST_FILES_PER_PAGE {
            bail!("GitHub API returned too many pull request files on page {page}");
        }
        let page_len = files.len();
        for file in files {
            if file.filename.is_empty() || file.filename.contains(['\0', '\n', '\r']) {
                bail!("GitHub API returned an invalid pull request file path");
            }
            paths.push(file.filename);
        }
        if page_len < PULL_REQUEST_FILES_PER_PAGE {
            break;
        }
    }
    Ok(paths)
}

fn validate_changed_paths(paths: &[String]) -> Result<()> {
    if paths.len() > MAX_PULL_REQUEST_FILES {
        bail!(
            "pull request changed-file list exceeds GitHub's {MAX_PULL_REQUEST_FILES}-file filter limit"
        );
    }
    if paths
        .iter()
        .any(|path| path.is_empty() || path.contains(['\0', '\n', '\r']))
    {
        bail!("pull request changed-file list contains an invalid path");
    }
    Ok(())
}

fn truncate_text(value: &str, maximum_chars: usize) -> String {
    let mut characters = value.chars();
    let prefix = characters.by_ref().take(maximum_chars).collect::<String>();
    if characters.next().is_some() {
        format!("{prefix}…")
    } else {
        prefix
    }
}

fn compose_check_summary(mut status: String, mut jobs: Vec<JobSummary>) -> String {
    jobs.sort_by_key(|job| job.completed_order);
    for job in jobs {
        status.push_str("\n\n---\n\n## ");
        status.push_str(&summary_heading(&job.job_name));
        for step in job.steps {
            status.push_str("\n\n### ");
            status.push_str(&summary_heading(&step.name));
            status.push_str("\n\n");
            status.push_str(&step.markdown);
            if !step.markdown.ends_with('\n') {
                status.push('\n');
            }
        }
        if job.omitted_steps {
            status.push_str(
                "\n_Additional step summaries were omitted after GitHub's 20-summary job limit._\n",
            );
        }
    }
    truncate_utf16(&status, MAX_CHECK_SUMMARY_UTF16_UNITS)
}

fn summary_heading(value: &str) -> String {
    value
        .split(['\r', '\n'])
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn truncate_utf16(value: &str, maximum_units: usize) -> String {
    if value.encode_utf16().count() <= maximum_units {
        return value.to_owned();
    }
    const MARKER: &str = "\n\n_Step summaries truncated to fit the GitHub Check limit._";
    let budget = maximum_units.saturating_sub(MARKER.encode_utf16().count());
    let mut units = 0;
    let mut output = String::new();
    for character in value.chars() {
        let character_units = character.len_utf16();
        if units + character_units > budget {
            break;
        }
        output.push(character);
        units += character_units;
    }
    output.push_str(MARKER);
    output
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JobConclusion {
    Success,
    Failure,
    TimedOut,
    Cancelled,
    Skipped,
}

impl JobConclusion {
    fn as_github_result(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure | Self::TimedOut => "failure",
            Self::Cancelled => "cancelled",
            Self::Skipped => "skipped",
        }
    }
}

#[derive(Debug)]
struct PlanExecution {
    completed_steps: usize,
    failed_jobs: Vec<String>,
    timed_out_jobs: Vec<String>,
    cancelled_jobs: Vec<String>,
}

fn cancelled_plan_execution(plan: &ExecutionPlan) -> PlanExecution {
    PlanExecution {
        completed_steps: 0,
        failed_jobs: Vec::new(),
        timed_out_jobs: Vec::new(),
        cancelled_jobs: vec![plan.workflow_name.clone()],
    }
}

struct JobSummary {
    completed_order: u64,
    job_name: String,
    steps: Vec<StepSummary>,
    omitted_steps: bool,
}

struct StepSummary {
    name: String,
    markdown: String,
}

struct JobSummaryBuilder {
    job_name: String,
    steps: Vec<StepSummary>,
    omitted_steps: bool,
}

impl JobSummaryBuilder {
    fn new(job_name: String) -> Self {
        Self {
            job_name,
            steps: Vec::new(),
            omitted_steps: false,
        }
    }

    fn push(&mut self, name: String, markdown: String) {
        if self.steps.len() < MAX_STEP_SUMMARIES_PER_JOB {
            self.steps.push(StepSummary { name, markdown });
        } else {
            self.omitted_steps = true;
        }
    }

    fn finish(self, completed_order: u64) -> Option<JobSummary> {
        if self.steps.is_empty() && !self.omitted_steps {
            return None;
        }
        Some(JobSummary {
            completed_order,
            job_name: self.job_name,
            steps: self.steps,
            omitted_steps: self.omitted_steps,
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("workflow jobs timed out: {0}")]
struct RunTimedOut(String);

#[derive(Debug, thiserror::Error)]
#[error("workflow jobs cancelled by concurrency: {0}")]
struct RunConcurrencyCancelled(String);

struct JobExecution {
    conclusion: JobConclusion,
    completed_steps: usize,
    outputs: BTreeMap<String, String>,
}

impl JobExecution {
    fn skipped() -> Self {
        Self {
            conclusion: JobConclusion::Skipped,
            completed_steps: 0,
            outputs: BTreeMap::new(),
        }
    }

    fn cancelled() -> Self {
        Self {
            conclusion: JobConclusion::Cancelled,
            completed_steps: 0,
            outputs: BTreeMap::new(),
        }
    }

    fn timed_out() -> Self {
        Self {
            conclusion: JobConclusion::TimedOut,
            completed_steps: 0,
            outputs: BTreeMap::new(),
        }
    }
}

struct BaseExecution {
    base_id: String,
    conclusion: JobConclusion,
    completed_steps: usize,
    outputs: BTreeMap<String, String>,
    expanded_jobs: Vec<PlannedJob>,
}

struct ActiveConcurrencyScope {
    lease: crate::concurrency::ConcurrencyLease,
    cancel: watch::Receiver<bool>,
}

type ActiveConcurrencyScopes = Arc<Mutex<BTreeMap<String, ActiveConcurrencyScope>>>;

struct ReusableMatrixRuntime {
    invocations: Vec<String>,
    max_parallel: usize,
    fail_fast: bool,
    active: BTreeSet<String>,
    started: BTreeSet<String>,
    failed: bool,
    cancel: watch::Sender<bool>,
}

struct StepExecution {
    conclusion: JobConclusion,
    outcome: JobConclusion,
    outputs: BTreeMap<String, String>,
    ran: bool,
}

struct ActionPost {
    name: String,
    step_id: String,
    action_directory: PathBuf,
    script: String,
    environment: BTreeMap<String, String>,
    state: BTreeMap<String, String>,
    condition: String,
    context: EvaluationContext,
}

struct ActionPhaseExecution {
    conclusion: JobConclusion,
    outputs: BTreeMap<String, String>,
    state: BTreeMap<String, String>,
    exit_code: Option<i32>,
}

impl StepExecution {
    fn skipped() -> Self {
        Self {
            conclusion: JobConclusion::Skipped,
            outcome: JobConclusion::Skipped,
            outputs: BTreeMap::new(),
            ran: false,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_plan(
    job_id: Uuid,
    plan: &ExecutionPlan,
    plan_index: usize,
    run: &RunSpec,
    repository_dir: &Path,
    run_dir: &Path,
    environment: &BTreeMap<String, String>,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
    execution_slots: &Arc<Semaphore>,
    runner_targeting: &RunnerTargeting,
    job_summaries: &Arc<Mutex<Vec<JobSummary>>>,
    environment_variable_cache: &EnvironmentVariableCache,
    workflow_commands: &Arc<StdMutex<WorkflowCommandProcessor>>,
    concurrency: &ConcurrencyClient,
    repository_access: &RunRepositoryAccess,
) -> Result<PlanExecution> {
    let Some(configuration) = &plan.concurrency else {
        return execute_plan_inner(
            job_id,
            plan,
            plan_index,
            run,
            repository_dir,
            run_dir,
            environment,
            cancel,
            outbound,
            sequence,
            execution_slots,
            runner_targeting,
            job_summaries,
            environment_variable_cache,
            workflow_commands,
            concurrency,
            repository_access,
        )
        .await;
    };
    let template = plan
        .jobs
        .first()
        .context("workflow concurrency requires at least one planned job")?;
    let mut plan_environment = environment.clone();
    plan_environment.extend(github_workflow_environment(run, plan));
    let empty_bases = BTreeMap::new();
    let empty_outputs = BTreeMap::new();
    let empty_steps = JsonValue::Object(Default::default());
    let mut context = expression_context(
        run,
        template,
        &empty_bases,
        &empty_outputs,
        &plan_environment,
        &empty_steps,
        &empty_steps,
        ExecutionStatus::Success,
        repository_dir,
        run_dir,
        None,
    )?;
    context.insert_json("inputs", JsonValue::Object(Default::default()))?;
    let (group, cancel_in_progress, queue) =
        resolve_concurrency(configuration, &context, &plan.workflow_name)?;
    let lease = match concurrency
        .acquire(
            run.id,
            format!("workflow:{}", plan.workflow_path),
            group,
            cancel_in_progress,
            queue,
            cancel,
        )
        .await?
    {
        ConcurrencyAcquisition::Acquired(lease) => lease,
        ConcurrencyAcquisition::Cancelled(reason) => {
            info!(workflow = %plan.workflow_name, %reason, "workflow cancelled while waiting for concurrency");
            return Ok(cancelled_plan_execution(plan));
        }
    };
    let lease_cancel = lease.cancellation();
    let (effective_cancel, relays) = combine_cancellations(cancel, vec![lease_cancel.clone()]);
    let execution = execute_plan_inner(
        job_id,
        plan,
        plan_index,
        run,
        repository_dir,
        run_dir,
        environment,
        &effective_cancel,
        outbound,
        sequence,
        execution_slots,
        runner_targeting,
        job_summaries,
        environment_variable_cache,
        workflow_commands,
        concurrency,
        repository_access,
    )
    .await;
    for relay in relays {
        relay.abort();
    }
    let concurrency_cancelled = *lease_cancel.borrow() && !*cancel.borrow();
    let release = lease.release().await;
    if concurrency_cancelled {
        return Ok(cancelled_plan_execution(plan));
    }
    match execution {
        Ok(execution) => {
            release?;
            Ok(execution)
        }
        Err(error) => Err(error),
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_plan_inner(
    job_id: Uuid,
    plan: &ExecutionPlan,
    plan_index: usize,
    run: &RunSpec,
    repository_dir: &Path,
    run_dir: &Path,
    environment: &BTreeMap<String, String>,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
    execution_slots: &Arc<Semaphore>,
    runner_targeting: &RunnerTargeting,
    job_summaries: &Arc<Mutex<Vec<JobSummary>>>,
    environment_variable_cache: &EnvironmentVariableCache,
    workflow_commands: &Arc<StdMutex<WorkflowCommandProcessor>>,
    concurrency: &ConcurrencyClient,
    repository_access: &RunRepositoryAccess,
) -> Result<PlanExecution> {
    let mut plan_environment = environment.clone();
    plan_environment.extend(github_workflow_environment(run, plan));
    let environment = &plan_environment;
    let mut jobs = plan.jobs.clone();
    let mut base_order = Vec::new();
    let mut seen = BTreeSet::new();
    for job in &jobs {
        if seen.insert(job.base_id.clone()) {
            base_order.push(job.base_id.clone());
        }
    }

    let mut completed_bases = BTreeMap::<String, JobConclusion>::new();
    let mut completed_outputs = BTreeMap::<String, BTreeMap<String, String>>::new();
    let mut completion_order = Vec::<String>::new();
    let mut pending_bases = base_order.iter().cloned().collect::<BTreeSet<_>>();
    let mut reusable_matrices = reusable_matrix_runtimes(&jobs);
    let active_concurrency_scopes: ActiveConcurrencyScopes = Arc::new(Mutex::new(BTreeMap::new()));
    let mut running_bases = FuturesUnordered::new();
    let mut completed_steps = 0;
    let mut cancellation_error = None;
    while completed_bases.len() < base_order.len() {
        let ready = if *cancel.borrow() {
            Vec::new()
        } else {
            base_order
                .iter()
                .filter(|base_id| pending_bases.contains(*base_id))
                .filter(|base_id| reusable_matrix_gate_can_start(&reusable_matrices, base_id))
                .filter(|base_id| {
                    jobs.iter()
                        .find(|job| &job.base_id == *base_id)
                        .expect("base job exists")
                        .needs
                        .iter()
                        .all(|dependency| completed_bases.contains_key(dependency))
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        for base_id in ready {
            if !reusable_matrix_gate_can_start(&reusable_matrices, &base_id) {
                continue;
            }
            pending_bases.remove(&base_id);
            start_reusable_matrix_invocation(&mut reusable_matrices, &base_id);
            let base_jobs = jobs
                .iter()
                .filter(|job| job.base_id == base_id)
                .cloned()
                .collect::<Vec<_>>();
            let mut matrix_cancellations =
                reusable_matrix_cancellations(&reusable_matrices, &base_id);
            matrix_cancellations.extend(
                concurrency_scope_cancellations(&active_concurrency_scopes, &base_jobs).await?,
            );
            let completed_bases = completed_bases.clone();
            let completed_outputs = completed_outputs.clone();
            let completion_order = completion_order.clone();
            let active_concurrency_scopes = active_concurrency_scopes.clone();
            running_bases.push(async move {
                let (effective_cancel, relays) =
                    combine_cancellations(cancel, matrix_cancellations);
                let execution = execute_base_group(
                    job_id,
                    &plan.workflow_name,
                    plan_index,
                    base_id.clone(),
                    base_jobs,
                    run,
                    completed_bases,
                    completed_outputs,
                    completion_order,
                    repository_dir,
                    run_dir,
                    environment,
                    &effective_cancel,
                    outbound,
                    sequence,
                    execution_slots,
                    runner_targeting,
                    job_summaries,
                    environment_variable_cache,
                    workflow_commands,
                    concurrency,
                    repository_access,
                    &active_concurrency_scopes,
                )
                .await;
                for relay in relays {
                    relay.abort();
                }
                match execution {
                    Err(_error) if *effective_cancel.borrow() && !*cancel.borrow() => {
                        Ok(BaseExecution {
                            base_id,
                            conclusion: JobConclusion::Cancelled,
                            completed_steps: 0,
                            outputs: BTreeMap::new(),
                            expanded_jobs: Vec::new(),
                        })
                    }
                    execution => execution,
                }
            });
        }

        let Some(execution) = running_bases.next().await else {
            if let Some(error) = cancellation_error {
                return Err(error);
            }
            if *cancel.borrow() {
                bail!("run cancelled");
            }
            bail!("workflow job dependency graph could not make progress");
        };
        let execution = match execution {
            Ok(execution) => execution,
            Err(error) if *cancel.borrow() => {
                cancellation_error.get_or_insert(error);
                continue;
            }
            Err(error) => return Err(error),
        };
        if !execution.expanded_jobs.is_empty() {
            let template_base = execution.base_id;
            jobs.retain(|job| job.base_id != template_base);
            let mut expansion_bases = Vec::new();
            for job in &execution.expanded_jobs {
                if !expansion_bases.contains(&job.base_id) {
                    expansion_bases.push(job.base_id.clone());
                }
            }
            jobs.extend(execution.expanded_jobs);
            for base_id in expansion_bases {
                if seen.insert(base_id.clone()) {
                    base_order.push(base_id.clone());
                }
                pending_bases.insert(base_id);
            }
            for (group_id, runtime) in reusable_matrix_runtimes(&jobs) {
                reusable_matrices.entry(group_id).or_insert(runtime);
            }
            continue;
        }
        completed_steps += execution.completed_steps;
        completed_outputs.insert(execution.base_id.clone(), execution.outputs);
        completion_order.push(execution.base_id.clone());
        completed_bases.insert(execution.base_id.clone(), execution.conclusion);
        let failed_groups = finish_reusable_matrix_invocation(
            &mut reusable_matrices,
            &execution.base_id,
            execution.conclusion,
        );
        for group_id in failed_groups {
            let group = &reusable_matrices[&group_id];
            for invocation in group
                .invocations
                .iter()
                .filter(|invocation| !group.started.contains(*invocation))
            {
                let gate_id = format!("{invocation}::gate");
                if pending_bases.contains(&gate_id)
                    && let Some(gate) = jobs.iter().find(|job| job.base_id == gate_id)
                {
                    report_skipped(
                        job_id,
                        &format!("{plan_index}/{gate_id}"),
                        &format!("{} / {}", plan.workflow_name, gate.name),
                        "Skipped by reusable-call matrix fail-fast.",
                        outbound,
                        sequence,
                    )
                    .await?;
                }
                for skipped in base_order.iter().filter(|base_id| {
                    *base_id == invocation
                        || base_id
                            .strip_prefix(invocation.as_str())
                            .is_some_and(|suffix| suffix.starts_with("::"))
                }) {
                    if pending_bases.remove(skipped) {
                        completed_bases.insert(skipped.clone(), JobConclusion::Skipped);
                        completed_outputs.insert(skipped.clone(), BTreeMap::new());
                        completion_order.push(skipped.clone());
                    }
                }
            }
        }
    }

    let failed_jobs = completed_bases
        .iter()
        .filter(|(job, conclusion)| **conclusion == JobConclusion::Failure && !job.contains("::"))
        .map(|(job, _)| format!("{} / {job}", plan.workflow_name))
        .collect();
    let timed_out_jobs = completed_bases
        .iter()
        .filter(|(job, conclusion)| **conclusion == JobConclusion::TimedOut && !job.contains("::"))
        .map(|(job, _)| format!("{} / {job}", plan.workflow_name))
        .collect();
    let cancelled_jobs = completed_bases
        .iter()
        .filter(|(job, conclusion)| **conclusion == JobConclusion::Cancelled && !job.contains("::"))
        .map(|(job, _)| format!("{} / {job}", plan.workflow_name))
        .collect();
    let remaining_scopes = {
        let mut active = active_concurrency_scopes.lock().await;
        std::mem::take(&mut *active)
    };
    if !remaining_scopes.is_empty() {
        for scope in remaining_scopes.into_values() {
            scope.lease.release().await?;
        }
        bail!("reusable workflow concurrency scopes did not reach their release gates");
    }
    Ok(PlanExecution {
        completed_steps,
        failed_jobs,
        timed_out_jobs,
        cancelled_jobs,
    })
}

fn reusable_matrix_runtimes(jobs: &[PlannedJob]) -> BTreeMap<String, ReusableMatrixRuntime> {
    jobs.iter()
        .filter_map(|job| match &job.virtual_job {
            Some(PlannedVirtualJob::ReusableMatrixResult {
                invocations,
                max_parallel,
                fail_fast,
            }) => {
                let (cancel, _) = watch::channel(false);
                Some((
                    job.base_id.clone(),
                    ReusableMatrixRuntime {
                        invocations: invocations.clone(),
                        max_parallel: *max_parallel,
                        fail_fast: *fail_fast,
                        active: BTreeSet::new(),
                        started: BTreeSet::new(),
                        failed: false,
                        cancel,
                    },
                ))
            }
            _ => None,
        })
        .collect()
}

fn reusable_matrix_gate_can_start(
    groups: &BTreeMap<String, ReusableMatrixRuntime>,
    base_id: &str,
) -> bool {
    groups.values().all(|group| {
        if group
            .invocations
            .iter()
            .any(|invocation| base_id == format!("{invocation}::gate"))
        {
            !group.failed && group.active.len() < group.max_parallel
        } else {
            true
        }
    })
}

fn start_reusable_matrix_invocation(
    groups: &mut BTreeMap<String, ReusableMatrixRuntime>,
    base_id: &str,
) {
    for group in groups.values_mut() {
        if let Some(invocation) = group
            .invocations
            .iter()
            .find(|invocation| base_id == format!("{invocation}::gate"))
            .cloned()
        {
            group.started.insert(invocation.clone());
            group.active.insert(invocation);
        }
    }
}

fn reusable_matrix_cancellations(
    groups: &BTreeMap<String, ReusableMatrixRuntime>,
    base_id: &str,
) -> Vec<watch::Receiver<bool>> {
    groups
        .values()
        .filter(|group| {
            group.active.iter().any(|invocation| {
                base_id == invocation
                    || base_id
                        .strip_prefix(invocation.as_str())
                        .is_some_and(|suffix| suffix.starts_with("::"))
            })
        })
        .map(|group| group.cancel.subscribe())
        .collect()
}

fn finish_reusable_matrix_invocation(
    groups: &mut BTreeMap<String, ReusableMatrixRuntime>,
    base_id: &str,
    conclusion: JobConclusion,
) -> Vec<String> {
    let mut failed = Vec::new();
    for (group_id, group) in groups.iter_mut() {
        if group
            .invocations
            .iter()
            .any(|invocation| invocation == base_id)
        {
            group.active.remove(base_id);
            if matches!(conclusion, JobConclusion::Failure | JobConclusion::TimedOut)
                && group.fail_fast
                && !group.failed
            {
                group.failed = true;
                group.cancel.send_replace(true);
                failed.push(group_id.clone());
            }
        }
    }
    failed
}

fn combine_cancellations(
    global: &watch::Receiver<bool>,
    matrix: Vec<watch::Receiver<bool>>,
) -> (watch::Receiver<bool>, Vec<tokio::task::JoinHandle<()>>) {
    let mut sources = Vec::with_capacity(matrix.len() + 1);
    sources.push(global.clone());
    sources.extend(matrix);
    let cancelled = sources.iter().any(|source| *source.borrow());
    let (combined, receiver) = watch::channel(cancelled);
    let relays = sources
        .into_iter()
        .map(|mut source| {
            let combined = combined.clone();
            tokio::spawn(async move {
                loop {
                    if *source.borrow() {
                        combined.send_replace(true);
                        return;
                    }
                    if source.changed().await.is_err() {
                        return;
                    }
                }
            })
        })
        .collect();
    (receiver, relays)
}

#[allow(clippy::too_many_arguments)]
fn expand_dynamic_matrix_job(
    workflow_name: &str,
    template: &PlannedJob,
    run: &RunSpec,
    completed_bases: &BTreeMap<String, JobConclusion>,
    completed_outputs: &BTreeMap<String, BTreeMap<String, String>>,
    repository_dir: &Path,
    run_dir: &Path,
    environment: &BTreeMap<String, String>,
    reusable_inputs: &JsonValue,
) -> Result<Vec<PlannedJob>> {
    let matrix_template = template
        .dynamic_matrix
        .as_ref()
        .context("dynamic matrix template is missing")?;
    let context = expression_context(
        run,
        template,
        completed_bases,
        completed_outputs,
        environment,
        &JsonValue::Object(Default::default()),
        reusable_inputs,
        dependency_status(&template.needs, completed_bases),
        repository_dir,
        run_dir,
        None,
    )?;
    let resolved = resolve_matrix_value(matrix_template, &context)
        .with_context(|| format!("evaluate dynamic matrix for job '{}'", template.base_id))?;
    let resolved = serde_yaml_ng::to_value(resolved)
        .context("convert evaluated dynamic matrix to workflow data")?;
    let combinations = expand_matrix_definition(workflow_name, &template.base_id, &resolved)
        .context("expand evaluated dynamic matrix")?;
    let mut instances = Vec::with_capacity(combinations.len());
    let matrix_total = combinations.len();
    for (index, matrix) in combinations.into_iter().enumerate() {
        let mut instance = template.clone();
        instance.id = format!("{}[{}]", template.base_id, index + 1);
        instance.matrix = matrix;
        instance.dynamic_matrix = None;
        instance.strategy_job_index = Some(index);
        instance.strategy_job_total = Some(matrix_total);
        instances.push(instance);
    }
    Ok(instances)
}

fn resolve_matrix_value(value: &YamlValue, context: &EvaluationContext) -> Result<JsonValue> {
    match value {
        YamlValue::String(value) if is_exact_expression(value) => context
            .evaluate_json(value)
            .with_context(|| format!("evaluate matrix expression '{value}'")),
        YamlValue::String(value) => context
            .render(value)
            .map(JsonValue::String)
            .with_context(|| format!("render matrix value '{value}'")),
        YamlValue::Sequence(values) => values
            .iter()
            .map(|value| resolve_matrix_value(value, context))
            .collect::<Result<Vec<_>>>()
            .map(JsonValue::Array),
        YamlValue::Mapping(entries) => {
            let mut resolved = serde_json::Map::new();
            for (key, value) in entries {
                let key = key
                    .as_str()
                    .context("dynamic matrix keys must be strings")?;
                resolved.insert(key.to_owned(), resolve_matrix_value(value, context)?);
            }
            Ok(JsonValue::Object(resolved))
        }
        YamlValue::Tagged(value) => resolve_matrix_value(&value.value, context),
        YamlValue::Null | YamlValue::Bool(_) | YamlValue::Number(_) => {
            serde_json::to_value(value).context("convert static matrix value to JSON")
        }
    }
}

fn resolve_reusable_inputs(job: &PlannedJob, context: &EvaluationContext) -> Result<JsonValue> {
    let mut resolved_inputs = JsonValue::Object(Default::default());
    for scope in &job.reusable_input_scopes {
        let mut scope_context = context.clone();
        scope_context.insert_json("inputs", resolved_inputs)?;
        scope_context.insert_json("matrix", serde_json::to_value(&scope.matrix)?)?;
        let mut inputs = serde_json::Map::new();
        for (name, input) in &scope.inputs {
            let value = resolve_matrix_value(&input.value, &scope_context)
                .with_context(|| format!("evaluate reusable workflow input '{name}'"))?;
            let matches = match input.input_type {
                ReusableInputType::String => value.is_string(),
                ReusableInputType::Boolean => value.is_boolean(),
                ReusableInputType::Number => value.is_number(),
            };
            if !matches {
                bail!(
                    "reusable workflow input '{name}' for job '{}' resolved to the wrong type",
                    job.base_id
                );
            }
            inputs.insert(name.clone(), value);
        }
        resolved_inputs = JsonValue::Object(inputs);
    }
    Ok(resolved_inputs)
}

#[cfg(test)]
fn resolve_reusable_secret_names(job: &PlannedJob) -> Result<BTreeSet<String>> {
    Ok(resolve_reusable_secrets(
        job,
        &BTreeMap::from([("GITHUB_TOKEN".to_owned(), String::new())]),
    )?
    .into_keys()
    .collect())
}

fn resolve_reusable_secrets(
    job: &PlannedJob,
    base_secrets: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    let mut available = base_secrets.clone();
    for scope in &job.reusable_secret_scopes {
        let previous = available;
        let mut current = BTreeMap::new();
        if let Some((name, value)) = case_insensitive_secret(&previous, "GITHUB_TOKEN") {
            current.insert(name.clone(), value.clone());
        }
        if scope.inherit {
            current.extend(
                previous
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone())),
            );
        }
        for (target, source) in &scope.mappings {
            if let Some((_, value)) = case_insensitive_secret(&previous, source) {
                current.insert(target.clone(), value.clone());
            }
        }
        if let Some(missing) = scope
            .required
            .iter()
            .find(|required| case_insensitive_secret(&current, required).is_none())
        {
            bail!(
                "required reusable workflow secret '{missing}' is unavailable for job '{}'",
                job.base_id
            );
        }
        available = current;
    }
    Ok(available)
}

fn authorized_checkout_secret_values(
    job: &PlannedJob,
    managed_secrets: &BTreeMap<String, String>,
    github_token: Option<&str>,
) -> Result<BTreeSet<String>> {
    let mut base_secrets = managed_secrets.clone();
    if let Some(github_token) = github_token {
        base_secrets.insert("GITHUB_TOKEN".to_owned(), github_token.to_owned());
    }
    Ok(resolve_reusable_secrets(job, &base_secrets)?
        .into_values()
        .filter(|value| {
            !value.is_empty()
                && managed_secrets
                    .values()
                    .any(|managed_value| managed_value == value)
        })
        .collect())
}

fn case_insensitive_secret<'a>(
    secrets: &'a BTreeMap<String, String>,
    requested: &str,
) -> Option<(&'a String, &'a String)> {
    secrets
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(requested))
}

fn is_exact_expression(value: &str) -> bool {
    let value = value.trim();
    let Some(expression) = value.strip_prefix("${{") else {
        return false;
    };
    let mut quoted = false;
    let mut characters = expression.char_indices().peekable();
    while let Some((offset, character)) = characters.next() {
        if character == '\'' {
            if quoted && characters.peek().is_some_and(|(_, next)| *next == '\'') {
                characters.next();
            } else {
                quoted = !quoted;
            }
        } else if character == '}'
            && !quoted
            && characters.peek().is_some_and(|(_, next)| *next == '}')
        {
            let end = "${{".len() + offset + 2;
            return value[end..].trim().is_empty();
        }
    }
    false
}

async fn concurrency_scope_cancellations(
    active: &ActiveConcurrencyScopes,
    jobs: &[PlannedJob],
) -> Result<Vec<watch::Receiver<bool>>> {
    let scope_ids = jobs
        .iter()
        .flat_map(|job| job.concurrency_scope_ids.iter())
        .collect::<BTreeSet<_>>();
    let active = active.lock().await;
    Ok(scope_ids
        .into_iter()
        .filter_map(|scope_id| active.get(scope_id).map(|scope| scope.cancel.clone()))
        .collect())
}

#[allow(clippy::too_many_arguments)]
async fn acquire_concurrency_scopes(
    job: &PlannedJob,
    base_context: &EvaluationContext,
    run: &RunSpec,
    cancel: &watch::Receiver<bool>,
    concurrency: &ConcurrencyClient,
    active: &ActiveConcurrencyScopes,
) -> Result<bool> {
    let mut acquired = Vec::new();
    for scope in &job.concurrency_acquire {
        if active.lock().await.contains_key(&scope.id) {
            release_concurrency_scopes(&acquired, active).await?;
            bail!(
                "concurrency scope '{}' was acquired more than once",
                scope.id
            );
        }
        let mut scope_job = job.clone();
        scope_job.reusable_input_scopes = scope.reusable_input_scopes.clone();
        let inputs = match resolve_reusable_inputs(&scope_job, base_context) {
            Ok(inputs) => inputs,
            Err(error) => {
                release_concurrency_scopes(&acquired, active).await?;
                return Err(error);
            }
        };
        let mut context = base_context.clone();
        if let Err(error) = context.insert_json("inputs", inputs) {
            release_concurrency_scopes(&acquired, active).await?;
            return Err(error.into());
        }
        let (group, cancel_in_progress, queue) =
            match resolve_concurrency(&scope.configuration, &context, &scope.id) {
                Ok(resolved) => resolved,
                Err(error) => {
                    release_concurrency_scopes(&acquired, active).await?;
                    return Err(error);
                }
            };
        let existing_cancellations = {
            let active = active.lock().await;
            job.concurrency_scope_ids
                .iter()
                .filter_map(|scope_id| active.get(scope_id).map(|scope| scope.cancel.clone()))
                .collect::<Vec<_>>()
        };
        let (acquire_cancel, relays) = combine_cancellations(cancel, existing_cancellations);
        let acquisition = concurrency
            .acquire(
                run.id,
                format!("scope:{}", scope.id),
                group,
                cancel_in_progress,
                queue,
                &acquire_cancel,
            )
            .await;
        for relay in relays {
            relay.abort();
        }
        let lease = match acquisition {
            Ok(ConcurrencyAcquisition::Acquired(lease)) => lease,
            Ok(ConcurrencyAcquisition::Cancelled(reason)) => {
                info!(scope = %scope.id, %reason, "reusable workflow scope cancelled while waiting for concurrency");
                release_concurrency_scopes(&acquired, active).await?;
                return Ok(false);
            }
            Err(error) => {
                release_concurrency_scopes(&acquired, active).await?;
                return Err(error);
            }
        };
        let cancellation = lease.cancellation();
        active.lock().await.insert(
            scope.id.clone(),
            ActiveConcurrencyScope {
                lease,
                cancel: cancellation,
            },
        );
        acquired.push(scope.id.clone());
    }
    Ok(true)
}

async fn release_concurrency_scopes(
    scope_ids: &[String],
    active: &ActiveConcurrencyScopes,
) -> Result<()> {
    let mut leases = Vec::new();
    {
        let mut active = active.lock().await;
        for scope_id in scope_ids {
            if let Some(scope) = active.remove(scope_id) {
                leases.push(scope.lease);
            }
        }
    }
    let mut first_error = None;
    for lease in leases {
        if let Err(error) = lease.release().await {
            first_error.get_or_insert(error);
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
fn resolve_job_concurrency(
    job: &PlannedJob,
    run: &RunSpec,
    completed_bases: &BTreeMap<String, JobConclusion>,
    completed_outputs: &BTreeMap<String, BTreeMap<String, String>>,
    environment: &BTreeMap<String, String>,
    reusable_inputs: &JsonValue,
    workspace: &Path,
    run_dir: &Path,
) -> Result<Option<(String, bool, ConcurrencyQueue)>> {
    let Some(configuration) = &job.concurrency else {
        return Ok(None);
    };
    let empty_steps = JsonValue::Object(Default::default());
    let context = expression_context(
        run,
        job,
        completed_bases,
        completed_outputs,
        environment,
        &empty_steps,
        reusable_inputs,
        dependency_status(&job.needs, completed_bases),
        workspace,
        run_dir,
        None,
    )?;
    resolve_concurrency(configuration, &context, &job.id).map(Some)
}

#[allow(clippy::too_many_arguments)]
async fn execute_base_group(
    job_id: Uuid,
    workflow_name: &str,
    plan_index: usize,
    base_id: String,
    mut instances: Vec<PlannedJob>,
    run: &RunSpec,
    completed_bases: BTreeMap<String, JobConclusion>,
    completed_outputs: BTreeMap<String, BTreeMap<String, String>>,
    completion_order: Vec<String>,
    repository_dir: &Path,
    run_dir: &Path,
    environment: &BTreeMap<String, String>,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
    execution_slots: &Arc<Semaphore>,
    runner_targeting: &RunnerTargeting,
    job_summaries: &Arc<Mutex<Vec<JobSummary>>>,
    environment_variable_cache: &EnvironmentVariableCache,
    workflow_commands: &Arc<StdMutex<WorkflowCommandProcessor>>,
    concurrency: &ConcurrencyClient,
    repository_access: &RunRepositoryAccess,
    active_concurrency_scopes: &ActiveConcurrencyScopes,
) -> Result<BaseExecution> {
    let mut condition_template = instances
        .first()
        .context("base job has no instances")?
        .clone();
    condition_template.matrix.clear();
    let dependency_status = dependency_status(&condition_template.needs, &completed_bases);
    let mut condition_context = expression_context(
        run,
        &condition_template,
        &completed_bases,
        &completed_outputs,
        environment,
        &JsonValue::Object(Default::default()),
        &JsonValue::Object(Default::default()),
        dependency_status,
        repository_dir,
        run_dir,
        None,
    )?;
    let reusable_inputs = resolve_reusable_inputs(&condition_template, &condition_context)?;
    condition_context.insert_json("inputs", reusable_inputs.clone())?;
    if !condition_allows(
        condition_template.condition.as_deref(),
        dependency_status,
        &condition_context,
    )? {
        let name = condition_context
            .render(&condition_template.name)
            .unwrap_or_else(|_| condition_template.name.clone());
        report_skipped(
            job_id,
            &format!("{plan_index}/{base_id}"),
            &format!("{workflow_name} / {name}"),
            "Job condition evaluated to false before matrix expansion.",
            outbound,
            sequence,
        )
        .await?;
        return Ok(BaseExecution {
            base_id,
            conclusion: JobConclusion::Skipped,
            completed_steps: 0,
            outputs: BTreeMap::new(),
            expanded_jobs: Vec::new(),
        });
    }
    if !condition_template.concurrency_acquire.is_empty()
        && !acquire_concurrency_scopes(
            &condition_template,
            &condition_context,
            run,
            cancel,
            concurrency,
            active_concurrency_scopes,
        )
        .await?
    {
        return Ok(BaseExecution {
            base_id,
            conclusion: JobConclusion::Cancelled,
            completed_steps: 0,
            outputs: BTreeMap::new(),
            expanded_jobs: Vec::new(),
        });
    }
    if let Some(PlannedVirtualJob::ReusableDynamicCall(call)) = &condition_template.virtual_job {
        if instances.len() != 1 {
            bail!("dynamic reusable-call job '{base_id}' has multiple templates");
        }
        let mut matrix_instances = expand_dynamic_matrix_job(
            workflow_name,
            &instances[0],
            run,
            &completed_bases,
            &completed_outputs,
            repository_dir,
            run_dir,
            environment,
            &reusable_inputs,
        )?;
        for instance in &mut matrix_instances {
            instance.condition = Some("${{ always() }}".to_owned());
        }
        return Ok(BaseExecution {
            base_id,
            conclusion: JobConclusion::Success,
            completed_steps: 0,
            outputs: BTreeMap::new(),
            expanded_jobs: expand_dynamic_reusable_call(
                &condition_template,
                matrix_instances,
                call,
            ),
        });
    }
    if let Some(virtual_job) = &condition_template.virtual_job {
        let execution = execute_virtual_job(
            base_id,
            virtual_job,
            &completed_bases,
            &completed_outputs,
            &completion_order,
            condition_context,
        );
        let release = release_concurrency_scopes(
            &condition_template.concurrency_release,
            active_concurrency_scopes,
        )
        .await;
        return match (execution, release) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(execution), Ok(())) => Ok(execution),
        };
    }
    if instances
        .first()
        .is_some_and(|job| job.dynamic_matrix.is_some())
    {
        if instances.len() != 1 {
            bail!("dynamic matrix job '{base_id}' has multiple templates");
        }
        instances = expand_dynamic_matrix_job(
            workflow_name,
            &instances[0],
            run,
            &completed_bases,
            &completed_outputs,
            repository_dir,
            run_dir,
            environment,
            &reusable_inputs,
        )?;
    }
    for instance in &mut instances {
        instance.condition = Some("${{ always() }}".to_owned());
    }
    let first = instances.first().context("base job has no instances")?;
    let matrix_parallelism = first
        .matrix_max_parallel
        .unwrap_or(instances.len())
        .min(instances.len())
        .max(1);
    let matrix_slots = Arc::new(Semaphore::new(matrix_parallelism));
    let (fail_fast, _) = watch::channel(false);
    let completed_bases = Arc::new(completed_bases);
    let completed_outputs = Arc::new(completed_outputs);
    let mut running_instances = FuturesUnordered::new();

    for (instance_index, workflow_job) in instances.into_iter().enumerate() {
        let event_job_id = format!("{plan_index}/{}", workflow_job.id);
        let workspace = run_dir
            .join("jobs")
            .join(format!("{plan_index}-{}", sanitize_id(&workflow_job.id)));
        let matrix_slots = matrix_slots.clone();
        let execution_slots = execution_slots.clone();
        let fail_fast = fail_fast.clone();
        let completed_bases = completed_bases.clone();
        let completed_outputs = completed_outputs.clone();
        let job_summaries = job_summaries.clone();
        let environment_variable_cache = environment_variable_cache.clone();
        let workflow_commands = workflow_commands.clone();
        let concurrency = concurrency.clone();
        let repository_access = repository_access.clone();
        let mut instance_cancel = cancel.clone();
        let outbound = outbound.clone();
        let sequence = sequence.clone();
        let reusable_inputs = reusable_inputs.clone();
        running_instances.push(async move {
            if *instance_cancel.borrow() {
                bail!("run cancelled");
            }
            if *fail_fast.borrow() {
                report_skipped(
                    job_id,
                    &event_job_id,
                    &format!("{workflow_name} / {}", workflow_job.name),
                    "Skipped by matrix fail-fast.",
                    &outbound,
                    &sequence,
                )
                .await?;
                return Ok((instance_index, JobExecution::skipped()));
            }
            let _matrix_permit =
                match acquire_execution_slot(matrix_slots, &mut instance_cancel).await {
                    Ok(permit) => permit,
                    Err(_error) if *instance_cancel.borrow() => {
                        return Ok((instance_index, JobExecution::cancelled()));
                    }
                    Err(error) => return Err(error),
                };
            if *fail_fast.borrow() {
                report_skipped(
                    job_id,
                    &event_job_id,
                    &format!("{workflow_name} / {}", workflow_job.name),
                    "Skipped by matrix fail-fast.",
                    &outbound,
                    &sequence,
                )
                .await?;
                return Ok((instance_index, JobExecution::skipped()));
            }
            let (base_cancel, base_relays) =
                combine_cancellations(&instance_cancel, vec![fail_fast.subscribe()]);
            let mut concurrency_lease = None;
            let mut concurrency_cancel = None;
            let (effective_cancel, concurrency_relays) =
                if let Some((group, cancel_in_progress, queue)) = resolve_job_concurrency(
                    &workflow_job,
                    run,
                    &completed_bases,
                    &completed_outputs,
                    environment,
                    &reusable_inputs,
                    &workspace,
                    run_dir,
                )? {
                    let acquisition = concurrency
                        .acquire(
                            run.id,
                            format!("job:{event_job_id}"),
                            group,
                            cancel_in_progress,
                            queue,
                            &base_cancel,
                        )
                        .await;
                    let lease = match acquisition {
                        Ok(ConcurrencyAcquisition::Acquired(lease)) => lease,
                        Ok(ConcurrencyAcquisition::Cancelled(reason)) => {
                            for relay in base_relays {
                                relay.abort();
                            }
                            info!(job = %workflow_job.id, %reason, "job cancelled while waiting for concurrency");
                            return Ok((instance_index, JobExecution::cancelled()));
                        }
                        Err(error) => {
                            for relay in base_relays {
                                relay.abort();
                            }
                            return Err(error);
                        }
                    };
                    let lease_cancel = lease.cancellation();
                    let (effective_cancel, relays) =
                        combine_cancellations(&base_cancel, vec![lease_cancel.clone()]);
                    concurrency_cancel = Some(lease_cancel);
                    concurrency_lease = Some(lease);
                    (effective_cancel, relays)
                } else {
                    (base_cancel.clone(), Vec::new())
                };
            let mut slot_cancel = effective_cancel.clone();
            let _execution_permit =
                match acquire_execution_slot(execution_slots, &mut slot_cancel).await {
                    Ok(permit) => permit,
                    Err(_error) if *slot_cancel.borrow() => {
                        for relay in base_relays.into_iter().chain(concurrency_relays) {
                            relay.abort();
                        }
                        if let Some(lease) = concurrency_lease {
                            lease.release().await?;
                        }
                        return Ok((instance_index, JobExecution::cancelled()));
                    }
                    Err(error) => {
                        for relay in base_relays.into_iter().chain(concurrency_relays) {
                            relay.abort();
                        }
                        if let Some(lease) = concurrency_lease {
                            lease.release().await?;
                        }
                        return Err(error);
                    }
                };
            if *fail_fast.borrow() {
                for relay in base_relays.into_iter().chain(concurrency_relays) {
                    relay.abort();
                }
                if let Some(lease) = concurrency_lease {
                    lease.release().await?;
                }
                report_skipped(
                    job_id,
                    &event_job_id,
                    &format!("{workflow_name} / {}", workflow_job.name),
                    "Skipped by matrix fail-fast.",
                    &outbound,
                    &sequence,
                )
                .await?;
                return Ok((instance_index, JobExecution::skipped()));
            }
            let job_timed_out = Arc::new(AtomicBool::new(false));
            let execution = execute_job(
                job_id,
                &event_job_id,
                workflow_name,
                &workflow_job,
                run,
                &completed_bases,
                &completed_outputs,
                &workspace,
                run_dir,
                environment,
                &reusable_inputs,
                &effective_cancel,
                &outbound,
                &sequence,
                runner_targeting,
                &job_timed_out,
                &job_summaries,
                &environment_variable_cache,
                &workflow_commands,
                &repository_access,
            )
            .await;
            for relay in base_relays.into_iter().chain(concurrency_relays) {
                relay.abort();
            }
            let concurrency_cancelled = concurrency_cancel
                .as_ref()
                .is_some_and(|receiver| *receiver.borrow())
                && !*instance_cancel.borrow()
                && !*fail_fast.borrow();
            if let Some(lease) = concurrency_lease {
                lease.release().await?;
            }
            let execution = match execution {
                Err(_error) if job_timed_out.load(Ordering::Acquire) => JobExecution::timed_out(),
                _ if concurrency_cancelled => JobExecution::cancelled(),
                Ok(execution) => execution,
                Err(_error) if *instance_cancel.borrow() => JobExecution::cancelled(),
                Err(_error) if *fail_fast.borrow() && !*instance_cancel.borrow() => {
                    JobExecution::cancelled()
                }
                Err(error) => return Err(error),
            };
            if matches!(
                execution.conclusion,
                JobConclusion::Failure | JobConclusion::TimedOut
            ) && workflow_job.matrix_fail_fast
            {
                fail_fast.send_replace(true);
            }
            Ok::<_, anyhow::Error>((instance_index, execution))
        });
    }

    let mut executions = BTreeMap::new();
    let mut run_cancelled = false;
    while let Some(result) = running_instances.next().await {
        let (instance_index, execution) = result?;
        if execution.conclusion == JobConclusion::Cancelled && *cancel.borrow() {
            run_cancelled = true;
        }
        executions.insert(instance_index, execution);
    }
    if run_cancelled {
        bail!("run cancelled");
    }
    let mut conclusions = Vec::with_capacity(executions.len());
    let mut outputs = BTreeMap::new();
    let mut completed_steps = 0;
    for execution in executions.into_values() {
        completed_steps += execution.completed_steps;
        outputs.extend(execution.outputs);
        conclusions.push(execution.conclusion);
    }
    Ok(BaseExecution {
        base_id,
        conclusion: aggregate_conclusions(&conclusions),
        completed_steps,
        outputs,
        expanded_jobs: Vec::new(),
    })
}

fn execute_virtual_job(
    base_id: String,
    virtual_job: &PlannedVirtualJob,
    completed_bases: &BTreeMap<String, JobConclusion>,
    completed_outputs: &BTreeMap<String, BTreeMap<String, String>>,
    completion_order: &[String],
    mut context: EvaluationContext,
) -> Result<BaseExecution> {
    match virtual_job {
        PlannedVirtualJob::ReusableGate => Ok(BaseExecution {
            base_id,
            conclusion: JobConclusion::Success,
            completed_steps: 0,
            outputs: BTreeMap::new(),
            expanded_jobs: Vec::new(),
        }),
        PlannedVirtualJob::ReusableResult { jobs, outputs } => {
            let conclusions = jobs
                .values()
                .map(|job| completed_bases[job])
                .collect::<Vec<_>>();
            let jobs = jobs
                .iter()
                .map(|(alias, job)| {
                    (
                        alias.clone(),
                        json!({
                            "result": completed_bases[job].as_github_result(),
                            "outputs": completed_outputs[job],
                        }),
                    )
                })
                .collect::<serde_json::Map<_, _>>();
            context.insert_json("jobs", JsonValue::Object(jobs))?;
            let outputs = outputs
                .iter()
                .map(|(name, value)| {
                    context
                        .render(value)
                        .with_context(|| format!("render reusable workflow output '{name}'"))
                        .map(|value| (name.clone(), value))
                })
                .collect::<Result<BTreeMap<_, _>>>()?;
            Ok(BaseExecution {
                base_id,
                conclusion: aggregate_conclusions(&conclusions),
                completed_steps: 0,
                outputs,
                expanded_jobs: Vec::new(),
            })
        }
        PlannedVirtualJob::ReusableMatrixResult { invocations, .. } => {
            let conclusions = invocations
                .iter()
                .map(|invocation| completed_bases[invocation])
                .collect::<Vec<_>>();
            let invocation_set = invocations.iter().collect::<BTreeSet<_>>();
            let mut ordered = completion_order
                .iter()
                .filter(|base_id| invocation_set.contains(base_id))
                .cloned()
                .collect::<Vec<_>>();
            let mut seen = ordered.iter().cloned().collect::<BTreeSet<_>>();
            for invocation in invocations {
                if seen.insert(invocation.clone()) {
                    ordered.push(invocation.clone());
                }
            }
            let mut outputs = BTreeMap::new();
            for invocation in ordered {
                if completed_bases[&invocation] != JobConclusion::Success {
                    continue;
                }
                for (name, value) in &completed_outputs[&invocation] {
                    outputs.entry(name.clone()).or_insert_with(String::new);
                    if !value.is_empty() {
                        outputs.insert(name.clone(), value.clone());
                    }
                }
            }
            Ok(BaseExecution {
                base_id,
                conclusion: aggregate_conclusions(&conclusions),
                completed_steps: 0,
                outputs,
                expanded_jobs: Vec::new(),
            })
        }
        PlannedVirtualJob::ReusableDynamicCall(_) => {
            unreachable!("dynamic reusable calls expand before generic virtual execution")
        }
    }
}

async fn acquire_execution_slot(
    slots: Arc<Semaphore>,
    cancel: &mut watch::Receiver<bool>,
) -> Result<OwnedSemaphorePermit> {
    loop {
        if *cancel.borrow() {
            bail!("run cancelled");
        }
        tokio::select! {
            permit = slots.clone().acquire_owned() => {
                return permit.context("workflow execution slot pool closed");
            }
            changed = cancel.changed() => {
                if changed.is_ok() && *cancel.borrow() {
                    bail!("run cancelled");
                }
                if changed.is_err() {
                    return slots.acquire_owned().await
                        .context("workflow execution slot pool closed");
                }
            }
        }
    }
}

fn deadline_cancellation(
    outer: &watch::Receiver<bool>,
    timeout: Option<Duration>,
    timed_out: Arc<AtomicBool>,
) -> (watch::Receiver<bool>, Option<tokio::task::JoinHandle<()>>) {
    let Some(timeout) = timeout else {
        return (outer.clone(), None);
    };
    let mut outer = outer.clone();
    let (cancel, receiver) = watch::channel(*outer.borrow());
    if *outer.borrow() {
        return (receiver, None);
    }
    let task = tokio::spawn(async move {
        let deadline = tokio::time::sleep(timeout);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                biased;
                changed = outer.changed() => {
                    match changed {
                        Ok(()) if *outer.borrow() => {
                            cancel.send_replace(true);
                            return;
                        }
                        Ok(()) => {}
                        Err(_) => {
                            deadline.await;
                            timed_out.store(true, Ordering::Release);
                            cancel.send_replace(true);
                            return;
                        }
                    }
                }
                _ = &mut deadline => {
                    timed_out.store(true, Ordering::Release);
                    cancel.send_replace(true);
                    return;
                }
            }
        }
    });
    (receiver, Some(task))
}

struct AbortTaskOnDrop(Option<tokio::task::JoinHandle<()>>);

impl Drop for AbortTaskOnDrop {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

struct SensitiveDirectoryGuard(PathBuf);

impl Drop for SensitiveDirectoryGuard {
    fn drop(&mut self) {
        match std::fs::remove_dir_all(&self.0) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                info!(
                    path = %self.0.display(),
                    %error,
                    "failed to remove sensitive temporary directory"
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_job(
    job_id: Uuid,
    event_job_id: &str,
    workflow_name: &str,
    job: &PlannedJob,
    run: &RunSpec,
    completed_bases: &BTreeMap<String, JobConclusion>,
    completed_outputs: &BTreeMap<String, BTreeMap<String, String>>,
    workspace: &Path,
    run_dir: &Path,
    environment: &BTreeMap<String, String>,
    reusable_inputs: &JsonValue,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
    runner_targeting: &RunnerTargeting,
    job_timed_out: &Arc<AtomicBool>,
    job_summaries: &Arc<Mutex<Vec<JobSummary>>>,
    environment_variable_cache: &EnvironmentVariableCache,
    workflow_commands: &Arc<StdMutex<WorkflowCommandProcessor>>,
    repository_access: &RunRepositoryAccess,
) -> Result<JobExecution> {
    workflow_commands
        .lock()
        .expect("workflow command processor was poisoned")
        .register_workspace(event_job_id, workspace.to_path_buf());
    let job_temp_directory = run_dir
        .join("_temp")
        .join(format!("job-{}", Uuid::new_v4()));
    tokio::fs::create_dir_all(&job_temp_directory)
        .await
        .with_context(|| {
            format!(
                "create isolated job temp directory {}",
                job_temp_directory.display()
            )
        })?;
    let _checkout_credential_guard =
        SensitiveDirectoryGuard(job_temp_directory.join("_checkout-credentials"));
    let mut workflow_token = repository_access
        .workflow_token(run, &job.permissions, cancel)
        .await
        .with_context(|| {
            format!(
                "issue scoped workflow token for workflow '{workflow_name}' job '{}'",
                job.id
            )
        })?;
    let mut job_environment = environment.clone();
    job_environment.insert("GITHUB_JOB".to_owned(), job.base_id.clone());
    job_environment.insert(
        "GITHUB_WORKSPACE".to_owned(),
        workspace.display().to_string(),
    );
    job_environment.insert(
        "RUNNER_TEMP".to_owned(),
        job_temp_directory.display().to_string(),
    );
    let empty_steps = JsonValue::Object(Default::default());
    let dependency_status = dependency_status(&job.needs, completed_bases);
    let mut context = expression_context(
        run,
        job,
        completed_bases,
        completed_outputs,
        &job_environment,
        &empty_steps,
        reusable_inputs,
        dependency_status,
        workspace,
        run_dir,
        workflow_token.as_deref(),
    )?;
    if !condition_allows(job.condition.as_deref(), dependency_status, &context)? {
        report_skipped(
            job_id,
            event_job_id,
            &format!("{workflow_name} / {}", context.render(&job.name)?),
            "Job condition evaluated to false.",
            outbound,
            sequence,
        )
        .await?;
        return Ok(JobExecution::skipped());
    }

    let runner_selector = render_runner_selector(&job.runs_on, &context)?;
    ensure_runner_eligible(
        &runner_selector,
        &runner_targeting.labels,
        runner_targeting.group.as_deref(),
    )
    .with_context(|| {
        format!(
            "workflow '{workflow_name}' job '{}' cannot run on this Mac agent",
            job.id
        )
    })?;

    let rendered_job_name = context.render(&job.name)?;
    let selected_environment_name = match &job.deployment_environment {
        Some(deployment) => {
            let environment_name = context
                .render(&deployment.name)
                .context("render deployment environment name")?;
            if environment_name.trim().is_empty()
                || environment_name.len() > 255
                || environment_name.contains(['\0', '\n', '\r'])
            {
                bail!(
                    "workflow '{workflow_name}' job '{}' resolved to an invalid deployment environment name",
                    job.id
                );
            }
            Some(environment_name)
        }
        None => None,
    };
    let runtime_variables = match selected_environment_name.as_deref() {
        Some(environment_name) => {
            let environment_variables = environment_variables_for_job(
                run,
                environment_name,
                environment_variable_cache,
                repository_access,
                cancel,
            )
            .await?;
            merge_configuration_variables(&run.variables, environment_variables)
        }
        None => run.variables.clone(),
    };
    let managed_secrets = repository_access
        .managed_secrets(
            run,
            event_job_id,
            selected_environment_name.as_deref(),
            cancel,
        )
        .await
        .with_context(|| {
            format!(
                "load managed secrets for workflow '{workflow_name}' job '{}'",
                job.id
            )
        })?;
    let checkout_secret_values =
        authorized_checkout_secret_values(job, &managed_secrets, workflow_token.as_deref())?;
    context = expression_context_with_variables(
        run,
        &runtime_variables,
        job,
        completed_bases,
        completed_outputs,
        &job_environment,
        &empty_steps,
        reusable_inputs,
        dependency_status,
        workspace,
        run_dir,
        workflow_token.as_deref(),
        &managed_secrets,
    )?;
    job_environment.extend(render_environment(&job.environment, &context)?);
    restore_default_environment(&mut job_environment, environment);
    job_environment.insert("GITHUB_JOB".to_owned(), job.base_id.clone());
    job_environment.insert(
        "GITHUB_WORKSPACE".to_owned(),
        workspace.display().to_string(),
    );
    job_environment.insert(
        "RUNNER_TEMP".to_owned(),
        job_temp_directory.display().to_string(),
    );
    context = expression_context_with_variables(
        run,
        &runtime_variables,
        job,
        completed_bases,
        completed_outputs,
        &job_environment,
        &empty_steps,
        reusable_inputs,
        dependency_status,
        workspace,
        run_dir,
        workflow_token.as_deref(),
        &managed_secrets,
    )?;
    let tracked_environment = job
        .deployment_environment
        .as_ref()
        .filter(|deployment| deployment.deployment)
        .and(selected_environment_name.as_deref());
    if let Some(environment) = tracked_environment {
        send(
            outbound,
            AgentMessage::DeploymentStarted {
                message_id: Uuid::new_v4(),
                job_id,
                unit_id: event_job_id.to_owned(),
                environment: environment.to_owned(),
            },
        )
        .await?;
    }

    let mut deployment_url = None;
    let execution = async {
        prepare_workspace(
            job_id,
            event_job_id,
            workflow_name,
            &rendered_job_name,
            workspace,
            outbound,
        )
        .await?;

        let job_timeout =
            parse_timeout(job.timeout_minutes.as_deref(), &context)?.unwrap_or(DEFAULT_JOB_TIMEOUT);
        let (effective_cancel, timeout_task) =
            deadline_cancellation(cancel, Some(job_timeout), job_timed_out.clone());
        let mut job_summary =
            JobSummaryBuilder::new(format!("{workflow_name} / {rendered_job_name}"));

        let step_execution = async {
            let cancel = &effective_cancel;
            let job_timed_out = job_timed_out.as_ref();
            let mut steps = serde_json::Map::new();
            let mut posts = Vec::new();
            let mut status = ExecutionStatus::Success;
            let mut completed_steps = 0;
            for step in &job.steps {
                if *cancel.borrow() {
                    if job_timed_out.load(Ordering::Acquire) {
                        return Ok(JobExecution {
                            conclusion: JobConclusion::TimedOut,
                            completed_steps,
                            outputs: BTreeMap::new(),
                        });
                    }
                    bail!("run cancelled");
                }
                workflow_token = repository_access
                    .workflow_token(run, &job.permissions, cancel)
                    .await
                    .with_context(|| {
                        format!(
                            "refresh scoped workflow token for workflow '{workflow_name}' job '{}'",
                            job.id
                        )
                    })?;
                context = expression_context_with_variables(
                    run,
                    &runtime_variables,
                    job,
                    completed_bases,
                    completed_outputs,
                    &job_environment,
                    &JsonValue::Object(steps.clone()),
                    reusable_inputs,
                    status,
                    workspace,
                    run_dir,
                    workflow_token.as_deref(),
                    &managed_secrets,
                )?;
                let execution = execute_step(
                    job_id,
                    workflow_name,
                    event_job_id,
                    step,
                    run,
                    workspace,
                    run_dir,
                    &mut job_environment,
                    &context,
                    status,
                    &mut posts,
                    cancel,
                    outbound,
                    sequence,
                    job_timed_out,
                    &mut job_summary,
                    workflow_commands,
                    repository_access,
                    &checkout_secret_values,
                )
                .await?;
                if execution.ran {
                    completed_steps += 1;
                }
                steps.insert(
                    step.id.clone(),
                    json!({
                        "outputs": execution.outputs,
                        "outcome": execution.outcome.as_github_result(),
                        "conclusion": execution.conclusion.as_github_result(),
                    }),
                );
                match execution.conclusion {
                    JobConclusion::Failure | JobConclusion::TimedOut => {
                        status = ExecutionStatus::Failure
                    }
                    JobConclusion::Cancelled => status = ExecutionStatus::Cancelled,
                    JobConclusion::Success | JobConclusion::Skipped => {}
                }
                if status == ExecutionStatus::Cancelled || job_timed_out.load(Ordering::Acquire) {
                    break;
                }
            }

            if job_timed_out.load(Ordering::Acquire) {
                return Ok(JobExecution {
                    conclusion: JobConclusion::TimedOut,
                    completed_steps,
                    outputs: BTreeMap::new(),
                });
            }

            for mut post in posts.into_iter().rev() {
                if status == ExecutionStatus::Cancelled {
                    break;
                }
                post.context.set_status(status);
                if !post.context.evaluate_condition(&post.condition)? {
                    continue;
                }
                let post_step_id = format!("{}/post", post.step_id);
                send(
                    outbound,
                    AgentMessage::StepStarted {
                        message_id: Uuid::new_v4(),
                        job_id,
                        step_id: post_step_id.clone(),
                        name: format!("{workflow_name} / Post {}", post.name),
                    },
                )
                .await?;
                let summary_name = format!("{} (post)", post.name);
                let execution = execute_node_action_phase(
                    job_id,
                    &post_step_id,
                    "post",
                    &post.action_directory,
                    &post.script,
                    None,
                    workspace,
                    run_dir,
                    &mut job_environment,
                    &post.environment,
                    &post.state,
                    cancel,
                    outbound,
                    sequence,
                    job_timed_out,
                    &mut job_summary,
                    &summary_name,
                    workflow_commands,
                )
                .await?;
                let conclusion = match execution.conclusion {
                    JobConclusion::Success => Conclusion::Success,
                    JobConclusion::Failure => Conclusion::Failure,
                    JobConclusion::TimedOut => Conclusion::TimedOut,
                    JobConclusion::Cancelled => Conclusion::Cancelled,
                    JobConclusion::Skipped => Conclusion::Neutral,
                };
                send(
                    outbound,
                    AgentMessage::StepFinished {
                        message_id: Uuid::new_v4(),
                        job_id,
                        step_id: post_step_id,
                        conclusion,
                        exit_code: execution.exit_code,
                    },
                )
                .await?;
                if matches!(
                    execution.conclusion,
                    JobConclusion::Failure | JobConclusion::TimedOut
                ) {
                    status = ExecutionStatus::Failure;
                }
                if job_timed_out.load(Ordering::Acquire) {
                    break;
                }
            }

            if job_timed_out.load(Ordering::Acquire) {
                return Ok(JobExecution {
                    conclusion: JobConclusion::TimedOut,
                    completed_steps,
                    outputs: BTreeMap::new(),
                });
            }

            workflow_token = repository_access
            .workflow_token(run, &job.permissions, cancel)
            .await
            .with_context(|| {
                format!(
                    "refresh scoped workflow token for workflow '{workflow_name}' job '{}' outputs",
                    job.id
                )
            })?;
            let output_context = expression_context_with_variables(
                run,
                &runtime_variables,
                job,
                completed_bases,
                completed_outputs,
                &job_environment,
                &JsonValue::Object(steps),
                reusable_inputs,
                status,
                workspace,
                run_dir,
                workflow_token.as_deref(),
                &managed_secrets,
            )?;
            if let (Some(deployment), Some(environment_name)) = (
                &job.deployment_environment,
                selected_environment_name.as_deref(),
            ) && let Some(url) = &deployment.url
            {
                let rendered_url = output_context
                    .render(url)
                    .context("render deployment environment URL")?;
                if !rendered_url.is_empty() {
                    if rendered_url.len() > 2_048 || rendered_url.contains(['\0', '\n', '\r']) {
                        bail!("deployment environment URL exceeds the protocol boundary");
                    }
                    let parsed = reqwest::Url::parse(&rendered_url)
                        .context("deployment environment URL is invalid")?;
                    if !matches!(parsed.scheme(), "http" | "https") {
                        bail!("deployment environment URL must use HTTP or HTTPS");
                    }
                    deployment_url = Some(parsed.as_str().to_owned());
                    job_summary.push(
                        "Deployment environment".to_owned(),
                        format!(
                            "Environment: `{}`\n\nURL: {}\n",
                            environment_name.replace('`', "\\`"),
                            parsed.as_str()
                        ),
                    );
                }
            }
            let outputs = render_environment(&job.outputs, &output_context)?;
            let outcome = match status {
                ExecutionStatus::Success => JobConclusion::Success,
                ExecutionStatus::Failure => JobConclusion::Failure,
                ExecutionStatus::Cancelled => JobConclusion::Cancelled,
                ExecutionStatus::Skipped => JobConclusion::Skipped,
            };
            let continue_on_error = match job.continue_on_error.as_deref() {
                Some(condition) => output_context
                    .evaluate_condition(condition)
                    .with_context(|| format!("evaluate job continue-on-error '{condition}'"))?,
                None => false,
            };

            Ok(JobExecution {
                conclusion: if continue_on_error && outcome == JobConclusion::Failure {
                    JobConclusion::Success
                } else {
                    outcome
                },
                completed_steps,
                outputs,
            })
        }
        .await;
        let timed_out = job_timed_out.load(Ordering::Acquire);
        if let Some(timeout_task) = timeout_task {
            timeout_task.abort();
        }
        if let Some(summary) = job_summary.finish(sequence.fetch_add(1, Ordering::Relaxed)) {
            job_summaries.lock().await.push(summary);
        }
        match step_execution {
            Ok(mut execution) if timed_out => {
                execution.conclusion = JobConclusion::TimedOut;
                execution.outputs.clear();
                Ok(execution)
            }
            execution => execution,
        }
    }
    .await;

    if tracked_environment.is_some() {
        let conclusion = match &execution {
            Ok(execution) => protocol_conclusion(execution.conclusion),
            Err(_) if job_timed_out.load(Ordering::Acquire) => Conclusion::TimedOut,
            Err(_) if *cancel.borrow() => Conclusion::Cancelled,
            Err(_) => Conclusion::Failure,
        };
        send(
            outbound,
            AgentMessage::DeploymentFinished {
                message_id: Uuid::new_v4(),
                job_id,
                unit_id: event_job_id.to_owned(),
                conclusion,
                environment_url: deployment_url,
            },
        )
        .await?;
    }
    execution
}

#[allow(clippy::too_many_arguments)]
async fn prepare_workspace(
    job_id: Uuid,
    event_job_id: &str,
    workflow_name: &str,
    job_name: &str,
    workspace: &Path,
    outbound: &mpsc::Sender<AgentMessage>,
) -> Result<()> {
    let step_id = format!("{event_job_id}/gitzero-prepare");
    send(
        outbound,
        AgentMessage::StepStarted {
            message_id: Uuid::new_v4(),
            job_id,
            step_id: step_id.clone(),
            name: format!("{workflow_name} / {job_name} / Prepare isolated workspace"),
        },
    )
    .await?;
    let result = prepare_directory(workspace)
        .await
        .context("create empty isolated job workspace");
    send(
        outbound,
        AgentMessage::StepFinished {
            message_id: Uuid::new_v4(),
            job_id,
            step_id: step_id.clone(),
            conclusion: if result.is_ok() {
                Conclusion::Success
            } else {
                Conclusion::Failure
            },
            exit_code: result.as_ref().ok().map(|_| 0),
        },
    )
    .await?;
    result
}

#[allow(clippy::too_many_arguments)]
async fn execute_step(
    job_id: Uuid,
    workflow_name: &str,
    workflow_job_id: &str,
    step: &PlannedStep,
    run: &RunSpec,
    repository_dir: &Path,
    run_dir: &Path,
    environment: &mut BTreeMap<String, String>,
    context: &EvaluationContext,
    status: ExecutionStatus,
    posts: &mut Vec<ActionPost>,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
    job_timed_out: &AtomicBool,
    job_summary: &mut JobSummaryBuilder,
    workflow_commands: &Arc<StdMutex<WorkflowCommandProcessor>>,
    repository_access: &RunRepositoryAccess,
    checkout_secret_values: &BTreeSet<String>,
) -> Result<StepExecution> {
    let mut step_context = context.clone();
    step_context.extend_json_object(
        "github",
        serde_json::Map::from_iter([(
            "action".to_owned(),
            JsonValue::String(step.github_action.clone()),
        )]),
    )?;
    let mut step_environment = render_environment(&step.environment, &step_context)?;
    let mut visible_environment = environment.clone();
    visible_environment.append(&mut step_environment);
    restore_default_environment(&mut visible_environment, environment);
    visible_environment.insert("GITHUB_ACTION".to_owned(), step.github_action.clone());
    step_context.insert_json("env", serde_json::to_value(&visible_environment)?)?;
    let step_id = format!("{workflow_job_id}/{}", step.id);
    let step_name = step_context.render(&step.name)?;
    let continue_on_error = match step.continue_on_error.as_deref() {
        Some(condition) => step_context
            .evaluate_condition(condition)
            .with_context(|| format!("evaluate continue-on-error '{condition}'"))?,
        None => false,
    };
    let timeout = parse_timeout(step.timeout_minutes.as_deref(), &step_context)?;
    if !condition_allows(step.condition.as_deref(), status, &step_context)? {
        report_skipped(
            job_id,
            &step_id,
            &format!("{workflow_name} / {step_name}"),
            "Step condition evaluated to false.",
            outbound,
            sequence,
        )
        .await?;
        return Ok(StepExecution::skipped());
    }

    send(
        outbound,
        AgentMessage::StepStarted {
            message_id: Uuid::new_v4(),
            job_id,
            step_id: step_id.clone(),
            name: format!("{workflow_name} / {step_name}"),
        },
    )
    .await?;

    let (shell, script) = match &step.kind {
        StepKind::Checkout { inputs } => {
            let source_repository_dir = run_dir.join("repository");
            let checkout = execute_checkout_step(
                job_id,
                &step_id,
                inputs,
                run,
                &source_repository_dir,
                repository_dir,
                run_dir,
                environment,
                &step_context,
                timeout,
                cancel,
                outbound,
                sequence,
                repository_access,
                checkout_secret_values,
            )
            .await;
            let outputs = match checkout {
                Ok(outputs) => outputs,
                Err(error) => {
                    let conclusion = if job_timed_out.load(Ordering::Acquire) {
                        JobConclusion::TimedOut
                    } else if *cancel.borrow() {
                        JobConclusion::Cancelled
                    } else {
                        return Err(error);
                    };
                    send(
                        outbound,
                        AgentMessage::StepFinished {
                            message_id: Uuid::new_v4(),
                            job_id,
                            step_id,
                            conclusion: protocol_conclusion(conclusion),
                            exit_code: None,
                        },
                    )
                    .await?;
                    return Ok(StepExecution {
                        conclusion,
                        outcome: conclusion,
                        outputs: BTreeMap::new(),
                        ran: true,
                    });
                }
            };
            send(
                outbound,
                AgentMessage::LogChunk {
                    message_id: Uuid::new_v4(),
                    job_id,
                    step_id: step_id.clone(),
                    sequence: sequence.fetch_add(1, Ordering::Relaxed),
                    stream: LogStream::System,
                    data: "Checked out the exact pinned repository snapshot.\n".to_owned(),
                },
            )
            .await?;
            send(
                outbound,
                AgentMessage::StepFinished {
                    message_id: Uuid::new_v4(),
                    job_id,
                    step_id,
                    conclusion: Conclusion::Success,
                    exit_code: Some(0),
                },
            )
            .await?;
            return Ok(StepExecution {
                conclusion: JobConclusion::Success,
                outcome: JobConclusion::Success,
                outputs,
                ran: true,
            });
        }
        StepKind::Uses { action, inputs } => {
            return execute_action_step(
                job_id,
                &step_id,
                &step_name,
                action,
                inputs,
                run,
                repository_dir,
                run_dir,
                environment,
                &visible_environment,
                &step_context,
                continue_on_error,
                timeout,
                posts,
                cancel,
                outbound,
                sequence,
                job_timed_out,
                job_summary,
                workflow_commands,
                repository_access,
                checkout_secret_values,
            )
            .await;
        }
        StepKind::Run { shell, script } => (shell, script),
    };
    let shell = step_context.render(shell)?;
    let script = step_context.render(script)?;
    let temp_dir = environment
        .get("RUNNER_TEMP")
        .map(PathBuf::from)
        .unwrap_or_else(|| run_dir.join("_temp").join(sanitize_id(workflow_job_id)));
    tokio::fs::create_dir_all(&temp_dir).await?;
    let env_file = temp_dir.join(format!("env-{}.txt", sanitize_id(&step_id)));
    let output_file = temp_dir.join(format!("output-{}.txt", sanitize_id(&step_id)));
    let path_file = temp_dir.join(format!("path-{}.txt", sanitize_id(&step_id)));
    let summary_file = temp_dir.join(format!("summary-{}.md", sanitize_id(&step_id)));
    let script_file = temp_dir.join(format!(
        "script-{}.{}",
        sanitize_id(&step_id),
        shell_script_extension(&shell)?
    ));
    for path in [&env_file, &output_file, &path_file, &summary_file] {
        tokio::fs::write(path, b"").await?;
    }
    tokio::fs::write(&script_file, &script).await?;

    let working_directory = match &step.working_directory {
        Some(relative) => repository_dir.join(step_context.render(relative)?),
        None => repository_dir.to_owned(),
    };
    ensure_within(repository_dir, &working_directory).await?;

    let shell_arguments = shell_arguments_for(&shell, &script_file)?;
    let mut command = Command::new(shell_program(&shell)?);
    command
        .args(shell_arguments)
        .current_dir(&working_directory)
        .envs(visible_environment.iter())
        .env("GITHUB_ENV", &env_file)
        .env("GITHUB_OUTPUT", &output_file)
        .env("GITHUB_PATH", &path_file)
        .env("GITHUB_STEP_SUMMARY", &summary_file);

    let result = run_workflow_process(
        job_id,
        &step_id,
        &mut command,
        timeout,
        cancel.clone(),
        outbound.clone(),
        sequence.clone(),
        workflow_commands.clone(),
    )
    .await;
    capture_step_summary(
        job_summary,
        &summary_file,
        &step_name,
        job_id,
        &step_id,
        outbound,
        sequence,
    )
    .await?;
    let timed_out = job_timed_out.load(Ordering::Acquire);
    let (_, exit_code) = process_conclusion(&result, timed_out);
    let outcome = match &result {
        Ok(_) => JobConclusion::Success,
        Err(ProcessError::Cancelled) if timed_out => JobConclusion::TimedOut,
        Err(ProcessError::Cancelled) => JobConclusion::Cancelled,
        Err(ProcessError::Exit(_) | ProcessError::Io(_) | ProcessError::TimedOut) => {
            JobConclusion::Failure
        }
    };
    let conclusion = if continue_on_error && outcome == JobConclusion::Failure {
        JobConclusion::Success
    } else {
        outcome
    };
    apply_environment_file(environment, &env_file).await?;
    apply_path_file(environment, &path_file).await?;
    let mut outputs = workflow_commands
        .lock()
        .expect("workflow command processor was poisoned")
        .take_legacy_outputs(&step_id)?;
    outputs.extend(parse_command_file(&output_file).await?);
    let outputs = filter_masked_outputs(
        outputs,
        workflow_commands,
        job_id,
        &step_id,
        outbound,
        sequence,
    )
    .await?;
    send(
        outbound,
        AgentMessage::StepFinished {
            message_id: Uuid::new_v4(),
            job_id,
            step_id,
            conclusion: protocol_conclusion(conclusion),
            exit_code,
        },
    )
    .await?;
    Ok(StepExecution {
        conclusion,
        outcome,
        outputs,
        ran: true,
    })
}

#[allow(clippy::too_many_arguments)]
async fn execute_checkout_step(
    job_id: Uuid,
    step_id: &str,
    inputs: &BTreeMap<String, String>,
    run: &RunSpec,
    source_repository_dir: &Path,
    workspace: &Path,
    run_dir: &Path,
    environment: &mut BTreeMap<String, String>,
    context: &EvaluationContext,
    timeout: Option<Duration>,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
    repository_access: &RunRepositoryAccess,
    checkout_secret_values: &BTreeSet<String>,
) -> Result<BTreeMap<String, String>> {
    let builtin_token = match context.evaluate_json("github.token")? {
        JsonValue::String(token) => token,
        JsonValue::Null => String::new(),
        _ => bail!("github.token must resolve to a string or null"),
    };
    let inputs = render_environment(inputs, context)?
        .into_iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), value))
        .collect::<BTreeMap<_, _>>();
    let supported = [
        "clean",
        "filter",
        "fetch-depth",
        "fetch-tags",
        "github-server-url",
        "lfs",
        "ssh-key",
        "ssh-known-hosts",
        "ssh-strict",
        "ssh-user",
        "show-progress",
        "sparse-checkout",
        "sparse-checkout-cone-mode",
        "submodules",
        "persist-credentials",
        "set-safe-directory",
        "allow-unsafe-pr-checkout",
        "token",
        "path",
        "repository",
        "ref",
    ];
    if let Some(name) = inputs
        .keys()
        .find(|name| !supported.contains(&name.as_str()))
    {
        bail!("actions/checkout input '{name}' is not supported yet");
    }
    let checkout_repository = checkout_repository(inputs.get("repository"), run)?;
    execute_checkout_step_with_repository(
        job_id,
        step_id,
        &inputs,
        run,
        source_repository_dir,
        workspace,
        run_dir,
        environment,
        &builtin_token,
        checkout_repository,
        timeout,
        cancel,
        outbound,
        sequence,
        repository_access,
        checkout_secret_values,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn execute_checkout_step_with_repository(
    job_id: Uuid,
    step_id: &str,
    inputs: &BTreeMap<String, String>,
    run: &RunSpec,
    source_repository_dir: &Path,
    workspace: &Path,
    run_dir: &Path,
    environment: &mut BTreeMap<String, String>,
    builtin_token: &str,
    checkout_repository: CheckoutRepository,
    timeout: Option<Duration>,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
    repository_access: &RunRepositoryAccess,
    checkout_secret_values: &BTreeSet<String>,
) -> Result<BTreeMap<String, String>> {
    let ssh_credentials = checkout_ssh_credentials(inputs, checkout_secret_values, environment)
        .await
        .context("configure actions/checkout SSH credentials")?;
    let cross_repository_access =
        if checkout_repository.same_repository || ssh_credentials.is_some() {
            None
        } else {
            Some(select_cross_repository_checkout_access(
                inputs.get("token").map(String::as_str),
                builtin_token,
                checkout_secret_values,
            )?)
        };
    let requested_ref = inputs
        .get("ref")
        .map(|git_ref| git_ref.trim())
        .unwrap_or("");
    if let Some(server_url) = inputs.get("github-server-url") {
        let server_url = server_url.trim_end_matches('/');
        if !server_url.is_empty() && server_url != "https://github.com" {
            bail!("actions/checkout github-server-url must be https://github.com");
        }
    }
    let checkout_directory = checkout_directory(
        workspace,
        inputs.get("path").map(String::as_str).unwrap_or(""),
    )?;
    let safe_checkout_directory =
        validate_checkout_directory(workspace, &checkout_directory).await?;
    let clean = checkout_boolean(inputs, "clean", true)?;
    let fetch_tags = checkout_boolean(inputs, "fetch-tags", false)?;
    let lfs = checkout_boolean(inputs, "lfs", false)?;
    let show_progress = checkout_boolean(inputs, "show-progress", true)?;
    let persist_credentials = checkout_boolean(inputs, "persist-credentials", true)?;
    let set_safe_directory = checkout_boolean(inputs, "set-safe-directory", true)?;
    let _allow_unsafe_pr_checkout = checkout_boolean(inputs, "allow-unsafe-pr-checkout", false)?;
    let sparse_checkout_cone_mode = checkout_boolean(inputs, "sparse-checkout-cone-mode", true)?;
    let sparse_checkout = checkout_sparse_patterns(inputs.get("sparse-checkout"))?;
    let filter = checkout_filter(inputs.get("filter"))?;
    let fetch_filter = filter
        .as_deref()
        .or_else(|| sparse_checkout.as_ref().map(|_| "blob:none"));
    let fetch_depth = inputs
        .get("fetch-depth")
        .map(String::as_str)
        .unwrap_or("1")
        .trim()
        .parse::<u32>()
        .context("actions/checkout fetch-depth must be a nonnegative integer")?;
    let submodules = inputs
        .get("submodules")
        .map(String::as_str)
        .unwrap_or("false")
        .trim()
        .to_ascii_lowercase();
    if !matches!(submodules.as_str(), "false" | "true" | "recursive") {
        bail!("actions/checkout submodules must be false, true, or recursive");
    }
    let checkout_remote = ssh_credentials.as_ref().map_or_else(
        || checkout_repository.clone_url.clone(),
        |credentials| checkout_repository.ssh_url(&credentials.user),
    );

    let mut managed_checkout_token = None;
    let (expected_sha, immutable_snapshot, output_ref, checkout_branch) = if checkout_repository
        .same_repository
    {
        let selected_target = checkout_target(requested_ref, run).context(
            "actions/checkout ref must identify an authenticated pull-request merge, head, or base snapshot",
        )?;
        (
            selected_target.commit(run).to_owned(),
            (selected_target == CheckoutTarget::Execution)
                .then(|| source_repository_dir.to_owned()),
            checkout_output_ref(inputs.get("ref"), run),
            checkout_local_branch(requested_ref, selected_target, run)?,
        )
    } else {
        let git_ref = public_checkout_ref(requested_ref)?;
        let (commit, snapshot, token) = if let Some(credentials) = &ssh_credentials {
            let snapshot = materialize_remote_repository_with_fetch(
                job_id,
                step_id,
                &checkout_repository.owner,
                &checkout_repository.name,
                &git_ref,
                &checkout_repository.clone_url,
                &checkout_remote,
                RemoteRepositoryMaterializationScope::CheckoutSsh,
                None,
                Some(&credentials.environment),
                run_dir,
                cancel,
                outbound,
                sequence,
            )
            .await
            .with_context(|| {
                format!(
                    "checkout repository {}/{}@{git_ref} with an explicit managed SSH key",
                    checkout_repository.owner, checkout_repository.name
                )
            })?;
            let (commit, snapshot, _) = finish_cross_repository_checkout(snapshot, None).await?;
            (commit, snapshot, None)
        } else {
            materialize_cross_repository_checkout_snapshot(
                job_id,
                step_id,
                &checkout_repository,
                &git_ref,
                run,
                repository_access,
                cross_repository_access.expect("cross-repository access mode"),
                run_dir,
                cancel,
                outbound,
                sequence,
            )
            .await?
        };
        let checkout_branch = cross_repository_checkout_branch(
            &checkout_repository,
            &git_ref,
            &commit,
            &checkout_remote,
            token.as_deref(),
            ssh_credentials.as_ref(),
            run_dir,
            cancel,
        )
        .await?;
        let output_ref = public_checkout_output_ref(&git_ref, checkout_branch.as_deref());
        managed_checkout_token = token;
        (commit, Some(snapshot), output_ref, checkout_branch)
    };
    let checkout_token = if checkout_repository.same_repository || ssh_credentials.is_some() {
        select_checkout_token(
            inputs.get("token").map(String::as_str),
            builtin_token,
            checkout_secret_values,
        )?
    } else {
        managed_checkout_token.as_deref().unwrap_or("")
    };

    let mut checkout_environment = environment.clone();
    let _safe_directory_guard = configure_checkout_safe_directory(
        &mut checkout_environment,
        &safe_checkout_directory,
        run_dir,
        set_safe_directory,
    )
    .await?;
    let rewrite_ssh_submodule_urls = submodules != "false" && ssh_credentials.is_none();
    configure_checkout_credentials(
        &mut checkout_environment,
        checkout_token,
        rewrite_ssh_submodule_urls,
    )?;
    configure_checkout_ssh(
        &mut checkout_environment,
        ssh_credentials
            .as_ref()
            .map(|credentials| credentials.command.as_str()),
    );
    if lfs {
        checkout_environment.remove("GIT_LFS_SKIP_SMUDGE");
    } else {
        checkout_environment.insert("GIT_LFS_SKIP_SMUDGE".to_owned(), "1".to_owned());
    }
    configure_checkout_credentials(
        environment,
        if persist_credentials {
            checkout_token
        } else {
            ""
        },
        persist_credentials && rewrite_ssh_submodule_urls,
    )?;
    configure_checkout_ssh(
        environment,
        persist_credentials
            .then_some(ssh_credentials.as_ref())
            .flatten()
            .map(|credentials| credentials.command.as_str()),
    );

    let mut existing_repository =
        git_repository_exists(&checkout_directory, &checkout_environment).await?;
    if existing_repository
        && git_repository_object_format(&checkout_directory, &checkout_environment).await?
            != GitObjectFormat::from_object_id(&expected_sha)?
    {
        existing_repository = false;
    }
    if !existing_repository {
        clear_checkout_directory(&checkout_directory).await?;
        tokio::fs::create_dir_all(&checkout_directory)
            .await
            .with_context(|| {
                format!("create checkout directory {}", checkout_directory.display())
            })?;
        let mut command = Command::new("git");
        command.args(["init", "--quiet"]);
        configure_git_init_object_format(&mut command, &expected_sha)?;
        command
            .current_dir(&checkout_directory)
            .envs(checkout_environment.iter());
        run_process(
            job_id,
            step_id,
            &mut command,
            timeout,
            cancel.clone(),
            outbound.clone(),
            sequence.clone(),
        )
        .await
        .map_err(anyhow::Error::from)
        .context("initialize checkout repository")?;
    }

    let has_origin = Command::new("git")
        .args(["remote", "get-url", "origin"])
        .current_dir(&checkout_directory)
        .envs(checkout_environment.iter())
        .output()
        .await
        .context("inspect checkout origin")?
        .status
        .success();
    let mut remote = Command::new("git");
    if has_origin {
        remote.args(["remote", "set-url", "origin", &checkout_remote]);
    } else {
        remote.args(["remote", "add", "origin", &checkout_remote]);
    }
    remote
        .current_dir(&checkout_directory)
        .envs(checkout_environment.iter());
    run_process(
        job_id,
        step_id,
        &mut remote,
        timeout,
        cancel.clone(),
        outbound.clone(),
        sequence.clone(),
    )
    .await
    .map_err(anyhow::Error::from)
    .context("configure checkout origin")?;

    if clean && existing_repository {
        let has_head = Command::new("git")
            .args(["rev-parse", "--verify", "HEAD"])
            .current_dir(&checkout_directory)
            .envs(checkout_environment.iter())
            .output()
            .await
            .context("inspect existing checkout HEAD")?
            .status
            .success();
        let mut commands = Vec::new();
        if has_head {
            commands.push(vec![
                "reset".to_owned(),
                "--hard".to_owned(),
                "HEAD".to_owned(),
            ]);
        }
        commands.push(vec!["clean".to_owned(), "-ffdx".to_owned()]);
        for arguments in commands {
            let mut command = Command::new("git");
            command
                .args(arguments)
                .current_dir(&checkout_directory)
                .envs(checkout_environment.iter());
            run_process(
                job_id,
                step_id,
                &mut command,
                timeout,
                cancel.clone(),
                outbound.clone(),
                sequence.clone(),
            )
            .await
            .map_err(anyhow::Error::from)
            .context("clean checkout workspace")?;
        }
    }

    fetch_exact_checkout_commit(
        job_id,
        step_id,
        &expected_sha,
        immutable_snapshot.as_deref(),
        &checkout_directory,
        &checkout_environment,
        fetch_filter,
        show_progress,
        timeout,
        cancel,
        outbound,
        sequence,
    )
    .await?;

    configure_sparse_checkout(
        job_id,
        step_id,
        &checkout_directory,
        sparse_checkout.as_deref(),
        sparse_checkout_cone_mode,
        &checkout_environment,
        timeout,
        cancel,
        outbound,
        sequence,
    )
    .await?;

    if fetch_depth == 0 || fetch_depth > 1 || fetch_tags {
        let shallow = if fetch_depth == 0 {
            let output = Command::new("git")
                .args(["rev-parse", "--is-shallow-repository"])
                .current_dir(&checkout_directory)
                .envs(checkout_environment.iter())
                .output()
                .await
                .context("inspect checkout depth")?;
            output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "true"
        } else {
            false
        };
        let mut command = Command::new("git");
        command.arg("fetch");
        if show_progress {
            command.arg("--progress");
        } else {
            command.arg("--quiet");
        }
        if let Some(filter) = fetch_filter {
            command.arg(format!("--filter={filter}"));
        }
        if fetch_tags {
            command.arg("--tags");
        }
        if shallow {
            command.arg("--unshallow");
        } else if fetch_depth > 1 {
            command.arg(format!("--deepen={}", fetch_depth - 1));
        }
        command.arg("origin");
        if fetch_depth == 0 {
            command.arg("+refs/heads/*:refs/remotes/origin/*");
            if checkout_repository.same_repository
                && run.pull_request.execution_ref
                    == format!("refs/pull/{}/merge", run.pull_request.number)
            {
                command.arg(format!(
                    "+refs/pull/{}/head:refs/remotes/pull/{}/head",
                    run.pull_request.number, run.pull_request.number
                ));
                command.arg(format!(
                    "+refs/pull/{}/merge:refs/remotes/pull/{}/merge",
                    run.pull_request.number, run.pull_request.number
                ));
            }
        } else if fetch_depth > 1 {
            command.arg(&expected_sha);
        }
        command
            .current_dir(&checkout_directory)
            .envs(checkout_environment.iter())
            .env("GIT_TERMINAL_PROMPT", "0");
        run_process(
            job_id,
            step_id,
            &mut command,
            timeout,
            cancel.clone(),
            outbound.clone(),
            sequence.clone(),
        )
        .await
        .map_err(anyhow::Error::from)
        .context("apply actions/checkout fetch options")?;
    }

    let checkout_context = if let Some(branch) = checkout_branch.as_deref() {
        let remote_tracking_ref = format!("refs/remotes/origin/{branch}");
        let mut update_ref = Command::new("git");
        update_ref
            .args(["update-ref", &remote_tracking_ref, &expected_sha])
            .current_dir(&checkout_directory)
            .envs(checkout_environment.iter());
        run_process(
            job_id,
            step_id,
            &mut update_ref,
            timeout,
            cancel.clone(),
            outbound.clone(),
            sequence.clone(),
        )
        .await
        .map_err(anyhow::Error::from)
        .context("pin checkout remote-tracking branch")?;

        let mut checkout = Command::new("git");
        checkout
            .args([
                "checkout",
                "--quiet",
                "--force",
                "-B",
                branch,
                &remote_tracking_ref,
            ])
            .current_dir(&checkout_directory)
            .envs(checkout_environment.iter());
        run_process(
            job_id,
            step_id,
            &mut checkout,
            timeout,
            cancel.clone(),
            outbound.clone(),
            sequence.clone(),
        )
        .await
        .map_err(anyhow::Error::from)
        .context("checkout exact pinned repository branch")?;

        let mut upstream = Command::new("git");
        upstream
            .args(["branch", "--quiet"])
            .arg(format!("--set-upstream-to={remote_tracking_ref}"))
            .args(["--", branch])
            .current_dir(&checkout_directory)
            .envs(checkout_environment.iter());
        run_process(
            job_id,
            step_id,
            &mut upstream,
            timeout,
            cancel.clone(),
            outbound.clone(),
            sequence.clone(),
        )
        .await
        .map_err(anyhow::Error::from)
        .context("configure checkout branch upstream")?;
        "verify actions/checkout branch"
    } else {
        let mut checkout = Command::new("git");
        checkout
            .args(["checkout", "--quiet", "--detach", "--force", &expected_sha])
            .current_dir(&checkout_directory)
            .envs(checkout_environment.iter());
        run_process(
            job_id,
            step_id,
            &mut checkout,
            timeout,
            cancel.clone(),
            outbound.clone(),
            sequence.clone(),
        )
        .await
        .map_err(anyhow::Error::from)
        .context("checkout exact pinned repository snapshot")?;
        "verify actions/checkout detached HEAD"
    };

    let symbolic_head = Command::new("git")
        .args(["symbolic-ref", "--quiet", "--short", "HEAD"])
        .current_dir(&checkout_directory)
        .envs(checkout_environment.iter())
        .output()
        .await
        .with_context(|| checkout_context.to_owned())?;
    let actual_branch = String::from_utf8_lossy(&symbolic_head.stdout)
        .trim()
        .to_owned();
    match checkout_branch.as_deref() {
        Some(expected_branch)
            if !symbolic_head.status.success() || actual_branch != expected_branch =>
        {
            bail!(
                "actions/checkout branch verification failed: expected {expected_branch}, got {actual_branch}"
            );
        }
        None if symbolic_head.status.success() => {
            bail!(
                "actions/checkout detached-HEAD verification failed: attached to {actual_branch}"
            );
        }
        _ => {}
    }
    if lfs {
        let mut command = Command::new("git");
        command
            .args(["lfs", "pull"])
            .current_dir(&checkout_directory)
            .envs(checkout_environment.iter());
        run_process(
            job_id,
            step_id,
            &mut command,
            timeout,
            cancel.clone(),
            outbound.clone(),
            sequence.clone(),
        )
        .await
        .map_err(anyhow::Error::from)
        .context("pull Git LFS objects")?;
    }
    if submodules != "false" {
        let mut sync = Command::new("git");
        sync.args(["submodule", "sync"]);
        if submodules == "recursive" {
            sync.arg("--recursive");
        }
        sync.current_dir(&checkout_directory)
            .envs(checkout_environment.iter())
            .env("GIT_TERMINAL_PROMPT", "0");
        run_process(
            job_id,
            step_id,
            &mut sync,
            timeout,
            cancel.clone(),
            outbound.clone(),
            sequence.clone(),
        )
        .await
        .map_err(anyhow::Error::from)
        .context("synchronize checkout submodules")?;

        let mut update = Command::new("git");
        update.args([
            "-c",
            "protocol.version=2",
            "submodule",
            "update",
            "--init",
            "--force",
        ]);
        if fetch_depth > 0 {
            update.arg(format!("--depth={fetch_depth}"));
        }
        if submodules == "recursive" {
            update.arg("--recursive");
        }
        update
            .current_dir(&checkout_directory)
            .envs(checkout_environment.iter())
            .env("GIT_TERMINAL_PROMPT", "0");
        run_process(
            job_id,
            step_id,
            &mut update,
            timeout,
            cancel.clone(),
            outbound.clone(),
            sequence.clone(),
        )
        .await
        .map_err(anyhow::Error::from)
        .context("initialize checkout submodules")?;

        let mut configure = Command::new("git");
        configure.args(["submodule", "foreach"]);
        if submodules == "recursive" {
            configure.arg("--recursive");
        }
        configure
            .args(["git", "config", "--local", "gc.auto", "0"])
            .current_dir(&checkout_directory)
            .envs(checkout_environment.iter());
        run_process(
            job_id,
            step_id,
            &mut configure,
            timeout,
            cancel.clone(),
            outbound.clone(),
            sequence.clone(),
        )
        .await
        .map_err(anyhow::Error::from)
        .context("disable automatic garbage collection in checkout submodules")?;
    }
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(&checkout_directory)
        .envs(checkout_environment.iter())
        .output()
        .await
        .context("verify actions/checkout HEAD")?;
    let actual = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if !output.status.success() || !actual.eq_ignore_ascii_case(&expected_sha) {
        bail!(
            "actions/checkout verification failed: expected {}, got {actual}",
            expected_sha
        );
    }
    Ok(BTreeMap::from([
        ("ref".to_owned(), output_ref),
        ("commit".to_owned(), expected_sha),
    ]))
}

struct CheckoutSshCredentials {
    user: String,
    command: String,
    environment: BTreeMap<String, String>,
}

async fn checkout_ssh_credentials(
    inputs: &BTreeMap<String, String>,
    managed_secret_values: &BTreeSet<String>,
    environment: &BTreeMap<String, String>,
) -> Result<Option<CheckoutSshCredentials>> {
    let strict = checkout_boolean(inputs, "ssh-strict", true)?;
    let user = inputs
        .get("ssh-user")
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or("git");
    if user.len() > MAX_CHECKOUT_SSH_USER_BYTES
        || user.starts_with('-')
        || !user
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("actions/checkout ssh-user is invalid");
    }
    let known_hosts = inputs
        .get("ssh-known-hosts")
        .map(String::as_str)
        .unwrap_or("");
    if known_hosts.len() > MAX_CHECKOUT_SSH_KNOWN_HOSTS_BYTES || known_hosts.contains('\0') {
        bail!("actions/checkout ssh-known-hosts is invalid or too large");
    }
    let Some(key) = select_checkout_ssh_key(
        inputs.get("ssh-key").map(String::as_str),
        managed_secret_values,
    )?
    else {
        return Ok(None);
    };
    if key.contains('\0') {
        bail!("actions/checkout ssh-key is invalid");
    }

    let runner_temp = environment
        .get("RUNNER_TEMP")
        .map(PathBuf::from)
        .context("RUNNER_TEMP is not available for actions/checkout SSH credentials")?;
    let credential_directory = runner_temp.join("_checkout-credentials");
    tokio::fs::create_dir_all(&credential_directory)
        .await
        .context("create actions/checkout SSH credential directory")?;
    let nonce = Uuid::new_v4();
    let key_path = credential_directory.join(format!("{nonce}.key"));
    let known_hosts_path = credential_directory.join(format!("{nonce}.known_hosts"));
    write_private_checkout_file(&key_path, format!("{}\n", key.trim()).as_bytes())
        .context("write actions/checkout SSH key")?;
    let known_hosts_contents = format!(
        "{known_hosts}\n# Begin implicitly added github.com\n{GITHUB_SSH_RSA_KNOWN_HOST}\n# End implicitly added github.com\n"
    );
    if let Err(error) =
        write_private_checkout_file(&known_hosts_path, known_hosts_contents.as_bytes())
            .context("write actions/checkout SSH known hosts")
    {
        let _ = std::fs::remove_file(&key_path);
        return Err(error);
    }

    let ssh = ["/usr/bin/ssh", "/bin/ssh"]
        .into_iter()
        .find(|path| Path::new(path).is_file())
        .context("actions/checkout SSH authentication requires the system ssh client")?;
    let mut command = format!(
        "{} -i {}",
        shell_words::quote(ssh),
        shell_words::quote(&key_path.to_string_lossy())
    );
    if strict {
        command.push_str(" -o StrictHostKeyChecking=yes -o CheckHostIP=no");
    }
    command.push_str(&format!(
        " -o {}",
        shell_words::quote(&format!(
            "UserKnownHostsFile={}",
            known_hosts_path.display()
        ))
    ));
    let ssh_environment = BTreeMap::from([("GIT_SSH_COMMAND".to_owned(), command.clone())]);
    Ok(Some(CheckoutSshCredentials {
        user: user.to_owned(),
        command,
        environment: ssh_environment,
    }))
}

fn select_checkout_ssh_key<'a>(
    input: Option<&'a str>,
    managed_secret_values: &BTreeSet<String>,
) -> Result<Option<&'a str>> {
    match input {
        None | Some("") => Ok(None),
        Some(key) if managed_secret_values.contains(key) => Ok(Some(key)),
        Some(_) => {
            bail!("actions/checkout ssh-key must be empty or an exact managed secret value")
        }
    }
}

fn write_private_checkout_file(path: &Path, contents: &[u8]) -> Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(contents)?;
    file.sync_all()?;
    Ok(())
}

const GITHUB_SSH_RSA_KNOWN_HOST: &str = "github.com ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCj7ndNxQowgcQnjshcLrqPEiiphnt+VTTvDP6mHBL9j1aNUkY4Ue1gvwnGLVlOhGeYrnZaMgRK6+PKCUXaDbC7qtbW8gIkhL7aGCsOr/C56SJMy/BCZfxd1nWzAOxSDPgVsmerOBYfNqltV9/hWCqBywINIR+5dIg6JTJ72pcEpEjcYgXkE2YEFXV1JHnsKgbLWNlhScqb2UmyRkQyytRLtL+38TGxkxCflmO+5Z8CSSNY7GidjMIZ7Q4zMjA2n1nGrlTDkzwDCsw+wqFPGQA179cnfGWOWRVruj16z6XyvxvjJwbz0wQZ75XK5tKSb7FNyeIEs4TT4jk+S4dhPeAUC5y+bDYirYgM4GC7uEnztnZyaVWQ7B381AK4Qdrwt51ZqExKbQpTUNn+EjqoTwvqNj4kqx5QUCI0ThS/YkOxJCXmPUWZbhjpCg56i+2aB6CmK2JGhn57K5mj0MNdBXA4/WnwH6XoPWJzK5Nyu2zB3nAZp+S5hpQs+p1vN1/wsjk=";

#[derive(Clone, Debug, PartialEq, Eq)]
struct CheckoutRepository {
    owner: String,
    name: String,
    clone_url: String,
    same_repository: bool,
}

#[derive(Deserialize, Serialize)]
struct CrossRepositoryCheckoutRefResolution {
    commit: String,
    branch: Option<String>,
}

impl CheckoutRepository {
    fn ssh_url(&self, user: &str) -> String {
        format!("{user}@github.com:{}/{}.git", self.owner, self.name)
    }
}

fn checkout_repository(input: Option<&String>, run: &RunSpec) -> Result<CheckoutRepository> {
    let expected = format!("{}/{}", run.repository.owner, run.repository.name);
    let selected = input.map_or(expected.as_str(), |value| value.trim());
    if selected.eq_ignore_ascii_case(&expected) {
        return Ok(CheckoutRepository {
            owner: run.repository.owner.clone(),
            name: run.repository.name.clone(),
            clone_url: run.repository.clone_url.clone(),
            same_repository: true,
        });
    }
    let (owner, name) = selected
        .split_once('/')
        .filter(|(_, name)| !name.contains('/'))
        .context("actions/checkout repository must use owner/repository syntax")?;
    validate_checkout_repository_owner(owner)?;
    validate_checkout_repository_name(name)?;
    Ok(CheckoutRepository {
        owner: owner.to_owned(),
        name: name.to_owned(),
        clone_url: format!("https://github.com/{owner}/{name}.git"),
        same_repository: false,
    })
}

fn validate_checkout_repository_owner(owner: &str) -> Result<()> {
    if owner.is_empty()
        || owner.len() > 39
        || !owner
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        || !owner
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        || !owner
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
    {
        bail!("actions/checkout repository owner is invalid");
    }
    Ok(())
}

fn validate_checkout_repository_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 100
        || matches!(name, "." | "..")
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("actions/checkout repository name is invalid");
    }
    Ok(())
}

fn public_checkout_ref(input: &str) -> Result<String> {
    let git_ref = if input.is_empty() { "HEAD" } else { input };
    if git_ref.len() > MAX_CHECKOUT_REF_BYTES
        || git_ref.starts_with('-')
        || git_ref.ends_with(['/', '.'])
        || git_ref.contains("..")
        || git_ref.contains("@{")
        || git_ref.contains("//")
        || git_ref.split('/').any(|component| {
            component.is_empty()
                || component.ends_with(".lock")
                || component.bytes().any(|byte| {
                    byte.is_ascii_control()
                        || matches!(byte, b' ' | b'~' | b'^' | b':' | b'?' | b'*' | b'[' | b'\\')
                })
        })
    {
        bail!("actions/checkout public repository ref is invalid");
    }
    Ok(git_ref.to_owned())
}

fn public_checkout_output_ref(git_ref: &str, branch: Option<&str>) -> String {
    if validate_remote_commit(git_ref).is_ok() {
        String::new()
    } else if git_ref == "HEAD" {
        branch.map_or_else(
            || git_ref.to_owned(),
            |branch| format!("refs/heads/{branch}"),
        )
    } else {
        git_ref.to_owned()
    }
}

#[allow(clippy::too_many_arguments)]
async fn cross_repository_checkout_branch(
    repository: &CheckoutRepository,
    git_ref: &str,
    commit: &str,
    checkout_remote: &str,
    checkout_token: Option<&str>,
    ssh_credentials: Option<&CheckoutSshCredentials>,
    run_dir: &Path,
    cancel: &watch::Receiver<bool>,
) -> Result<Option<String>> {
    if let Some(branch) = git_ref.strip_prefix("refs/heads/") {
        validate_checkout_branch(branch)?;
        return Ok(Some(branch.to_owned()));
    }
    if git_ref.starts_with("refs/") || validate_remote_commit(git_ref).is_ok() {
        return Ok(None);
    }

    let resolution =
        remote_repository_resolution_path(run_dir, &repository.owner, &repository.name, git_ref)
            .with_extension("checkout-ref");
    if let Some(branch) =
        read_cross_repository_checkout_ref_resolution(&resolution, git_ref, commit).await?
    {
        return Ok(branch);
    }
    let cache = remote_repository_cache_path(run_dir, &repository.owner, &repository.name)?;
    let lock = remote_repository_lock(&cache);
    let _guard = lock.lock().await;
    if let Some(branch) =
        read_cross_repository_checkout_ref_resolution(&resolution, git_ref, commit).await?
    {
        return Ok(branch);
    }

    let mut command = Command::new("git");
    command.arg("ls-remote");
    if git_ref == "HEAD" {
        command.args(["--symref", checkout_remote, "HEAD"]);
    } else {
        command
            .args(["--heads", "--tags", checkout_remote])
            .arg(format!("refs/heads/{git_ref}"))
            .arg(format!("refs/tags/{git_ref}"));
    }
    let mut environment = BTreeMap::new();
    if let Some(credentials) = ssh_credentials {
        configure_checkout_ssh(&mut environment, Some(&credentials.command));
    } else {
        configure_checkout_credentials(&mut environment, checkout_token.unwrap_or(""), false)?;
    }
    command
        .envs(environment.iter())
        .env("GIT_TERMINAL_PROMPT", "0");
    let advertised = run_process_capture_stdout(&mut command, cancel.clone())
        .await
        .with_context(|| {
            format!(
                "resolve actions/checkout branch identity for {}/{}@{git_ref}",
                repository.owner, repository.name
            )
        })?;
    let branch = parse_cross_repository_checkout_branch(git_ref, &advertised)?;
    if git_ref == "HEAD" && branch.is_none() {
        bail!(
            "actions/checkout could not resolve the default branch for {}/{}",
            repository.owner,
            repository.name
        );
    }
    write_cross_repository_checkout_ref_resolution(&resolution, commit, branch.as_deref()).await?;
    Ok(branch)
}

fn parse_cross_repository_checkout_branch(
    git_ref: &str,
    advertised: &str,
) -> Result<Option<String>> {
    let branch = if git_ref == "HEAD" {
        advertised.lines().find_map(|line| {
            let (reference, target) = line.split_once('\t')?;
            (target == "HEAD")
                .then(|| reference.strip_prefix("ref: refs/heads/"))
                .flatten()
        })
    } else {
        let expected = format!("refs/heads/{git_ref}");
        advertised
            .lines()
            .any(|line| {
                line.split_once('\t')
                    .is_some_and(|(_, reference)| reference == expected)
            })
            .then_some(git_ref)
    };
    let Some(branch) = branch else {
        return Ok(None);
    };
    validate_checkout_branch(branch)?;
    Ok(Some(branch.to_owned()))
}

async fn read_cross_repository_checkout_ref_resolution(
    path: &Path,
    git_ref: &str,
    commit: &str,
) -> Result<Option<Option<String>>> {
    let metadata = match tokio::fs::metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("inspect cached checkout ref resolution"),
    };
    if metadata.len() > MAX_CHECKOUT_REF_BYTES as u64 * 2 {
        return Ok(None);
    }
    let source = tokio::fs::read(path)
        .await
        .context("read cached checkout ref resolution")?;
    let Ok(resolution) = serde_json::from_slice::<CrossRepositoryCheckoutRefResolution>(&source)
    else {
        return Ok(None);
    };
    if !resolution.commit.eq_ignore_ascii_case(commit) {
        return Ok(None);
    }
    if let Some(branch) = resolution.branch.as_deref()
        && (validate_checkout_branch(branch).is_err() || (git_ref != "HEAD" && branch != git_ref))
    {
        return Ok(None);
    }
    Ok(Some(resolution.branch))
}

async fn write_cross_repository_checkout_ref_resolution(
    path: &Path,
    commit: &str,
    branch: Option<&str>,
) -> Result<()> {
    let source = serde_json::to_vec(&CrossRepositoryCheckoutRefResolution {
        commit: commit.to_owned(),
        branch: branch.map(str::to_owned),
    })?;
    let temporary = path.with_extension(format!("checkout-ref-{}", Uuid::new_v4()));
    tokio::fs::write(&temporary, source)
        .await
        .context("write temporary checkout ref resolution")?;
    if let Err(error) = tokio::fs::rename(&temporary, path).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error).context("publish checkout ref resolution");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn materialize_cross_repository_checkout_snapshot(
    job_id: Uuid,
    step_id: &str,
    repository: &CheckoutRepository,
    git_ref: &str,
    run: &RunSpec,
    repository_access: &RunRepositoryAccess,
    access: CrossRepositoryCheckoutAccess,
    run_dir: &Path,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
) -> Result<(String, PathBuf, Option<String>)> {
    if let CrossRepositoryCheckoutAccess::ExplicitManagedToken(token) = &access {
        let snapshot = materialize_remote_repository(
            job_id,
            step_id,
            &repository.owner,
            &repository.name,
            git_ref,
            &repository.clone_url,
            RemoteRepositoryMaterializationScope::CheckoutManaged,
            Some(token),
            run_dir,
            cancel,
            outbound,
            sequence,
        )
        .await
        .with_context(|| {
            format!(
                "checkout repository {}/{}@{git_ref} with an explicit managed secret credential",
                repository.owner, repository.name
            )
        })?;
        return finish_cross_repository_checkout(snapshot, Some(token.clone())).await;
    }
    let anonymous = materialize_remote_repository(
        job_id,
        step_id,
        &repository.owner,
        &repository.name,
        git_ref,
        &repository.clone_url,
        RemoteRepositoryMaterializationScope::CheckoutAnonymous,
        None,
        run_dir,
        cancel,
        outbound,
        sequence,
    )
    .await;
    let (snapshot, token) = match anonymous {
        Ok(snapshot) => (snapshot, None),
        Err(error) if access == CrossRepositoryCheckoutAccess::AnonymousOnly => {
            return Err(error).with_context(|| {
                format!(
                    "checkout repository {}/{}@{git_ref} anonymously",
                    repository.owner, repository.name
                )
            });
        }
        Err(error) if !repository.owner.eq_ignore_ascii_case(&run.repository.owner) => {
            return Err(error).with_context(|| {
                format!(
                    "checkout repository {}/{}@{git_ref} anonymously; managed private checkout requires the caller owner {}",
                    repository.owner, repository.name, run.repository.owner
                )
            });
        }
        Err(anonymous_error) => {
            let cached = repository_access
                .cached_token(
                    RepositoryTokenPurpose::Checkout,
                    &repository.owner,
                    &repository.name,
                )
                .await;
            if let Some(token) = cached.as_deref()
                && let Ok(snapshot) = materialize_remote_repository(
                    job_id,
                    step_id,
                    &repository.owner,
                    &repository.name,
                    git_ref,
                    &repository.clone_url,
                    RemoteRepositoryMaterializationScope::CheckoutManaged,
                    Some(token),
                    run_dir,
                    cancel,
                    outbound,
                    sequence,
                )
                .await
            {
                return finish_cross_repository_checkout(snapshot, Some(token.to_owned())).await;
            }
            let token = repository_access
                .request_token(
                    run.id,
                    RepositoryTokenPurpose::Checkout,
                    &repository.owner,
                    &repository.name,
                    cached.is_some(),
                    cancel,
                )
                .await
                .with_context(|| {
                    format!(
                        "checkout {}/{}@{git_ref} without anonymous access: {anonymous_error:#}",
                        repository.owner, repository.name
                    )
                })?;
            let snapshot = materialize_remote_repository(
                job_id,
                step_id,
                &repository.owner,
                &repository.name,
                git_ref,
                &repository.clone_url,
                RemoteRepositoryMaterializationScope::CheckoutManaged,
                Some(&token),
                run_dir,
                cancel,
                outbound,
                sequence,
            )
            .await
            .with_context(|| {
                format!(
                    "checkout private repository {}/{}@{git_ref}",
                    repository.owner, repository.name
                )
            })?;
            (snapshot, Some(token))
        }
    };
    finish_cross_repository_checkout(snapshot, token).await
}

async fn finish_cross_repository_checkout(
    snapshot: PathBuf,
    token: Option<String>,
) -> Result<(String, PathBuf, Option<String>)> {
    let commit = git_output(
        Command::new("git")
            .args(["rev-parse", "--verify", "HEAD^{commit}"])
            .current_dir(&snapshot),
        "resolve cross-repository checkout commit",
    )
    .await?;
    validate_remote_commit(&commit)?;
    Ok((commit, snapshot, token))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CheckoutTarget {
    Execution,
    Head,
    Base,
}

impl CheckoutTarget {
    fn commit(self, run: &RunSpec) -> &str {
        match self {
            Self::Execution => &run.pull_request.merge_sha,
            Self::Head => &run.pull_request.head_sha,
            Self::Base => &run.pull_request.base_sha,
        }
    }
}

fn checkout_output_ref(input: Option<&String>, run: &RunSpec) -> String {
    match input
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        None => run.pull_request.execution_ref.clone(),
        Some(git_ref)
            if git_ref.eq_ignore_ascii_case(&run.pull_request.merge_sha)
                || git_ref.eq_ignore_ascii_case(&run.pull_request.head_sha)
                || git_ref.eq_ignore_ascii_case(&run.pull_request.base_sha) =>
        {
            String::new()
        }
        Some(git_ref) => git_ref.to_owned(),
    }
}

fn checkout_target(git_ref: &str, run: &RunSpec) -> Option<CheckoutTarget> {
    if git_ref.is_empty()
        || git_ref.eq_ignore_ascii_case(&run.pull_request.merge_sha)
        || git_ref == run.pull_request.execution_ref
    {
        Some(CheckoutTarget::Execution)
    } else if git_ref.eq_ignore_ascii_case(&run.pull_request.head_sha)
        || git_ref == run.pull_request.head_ref
        || git_ref == format!("refs/heads/{}", run.pull_request.head_ref)
        || git_ref == format!("refs/pull/{}/head", run.pull_request.number)
    {
        Some(CheckoutTarget::Head)
    } else if git_ref.eq_ignore_ascii_case(&run.pull_request.base_sha)
        || git_ref == run.pull_request.base_ref
        || git_ref == format!("refs/heads/{}", run.pull_request.base_ref)
    {
        Some(CheckoutTarget::Base)
    } else {
        None
    }
}

fn checkout_local_branch(
    git_ref: &str,
    target: CheckoutTarget,
    run: &RunSpec,
) -> Result<Option<String>> {
    let branch = if target == CheckoutTarget::Head
        && (git_ref == run.pull_request.head_ref
            || git_ref == format!("refs/heads/{}", run.pull_request.head_ref))
    {
        Some(run.pull_request.head_ref.as_str())
    } else if (target == CheckoutTarget::Base
        && (git_ref == run.pull_request.base_ref
            || git_ref == format!("refs/heads/{}", run.pull_request.base_ref)))
        || (target == CheckoutTarget::Execution
            && run.pull_request.execution_ref
                == format!("refs/heads/{}", run.pull_request.base_ref)
            && (git_ref.is_empty() || git_ref == run.pull_request.execution_ref))
    {
        Some(run.pull_request.base_ref.as_str())
    } else {
        None
    };
    let Some(branch) = branch else {
        return Ok(None);
    };
    validate_checkout_branch(branch)?;
    Ok(Some(branch.to_owned()))
}

fn validate_checkout_branch(branch: &str) -> Result<()> {
    if branch.starts_with('-')
        || branch == "HEAD"
        || !valid_git_ref(&format!("refs/heads/{branch}"))
    {
        bail!("actions/checkout branch ref is invalid");
    }
    Ok(())
}

fn checkout_filter(input: Option<&String>) -> Result<Option<String>> {
    let Some(filter) = input
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    if filter.len() > MAX_CHECKOUT_FILTER_BYTES || filter.chars().any(char::is_control) {
        bail!("actions/checkout filter is invalid or too large");
    }
    Ok(Some(filter.to_owned()))
}

fn checkout_sparse_patterns(input: Option<&String>) -> Result<Option<Vec<String>>> {
    let Some(input) = input else {
        return Ok(None);
    };
    if input.len() > MAX_SPARSE_CHECKOUT_BYTES || input.contains('\0') {
        bail!("actions/checkout sparse-checkout input is invalid or too large");
    }
    let patterns = input
        .lines()
        .map(str::trim)
        .filter(|pattern| !pattern.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if patterns.len() > MAX_SPARSE_CHECKOUT_PATTERNS {
        bail!("actions/checkout sparse-checkout contains too many patterns");
    }
    Ok((!patterns.is_empty()).then_some(patterns))
}

#[allow(clippy::too_many_arguments)]
async fn fetch_exact_checkout_commit(
    job_id: Uuid,
    step_id: &str,
    expected_sha: &str,
    immutable_snapshot: Option<&Path>,
    checkout_directory: &Path,
    environment: &BTreeMap<String, String>,
    filter: Option<&str>,
    show_progress: bool,
    timeout: Option<Duration>,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
) -> Result<()> {
    if let Some(filter) = filter {
        let mut command = Command::new("git");
        command.args([
            "-c",
            "protocol.version=2",
            "fetch",
            "--no-tags",
            "--no-recurse-submodules",
            "--depth=1",
        ]);
        if show_progress {
            command.arg("--progress");
        } else {
            command.arg("--quiet");
        }
        command
            .arg(format!("--filter={filter}"))
            .arg("origin")
            .arg(expected_sha)
            .current_dir(checkout_directory)
            .envs(environment.iter())
            .env("GIT_TERMINAL_PROMPT", "0");
        match run_process(
            job_id,
            step_id,
            &mut command,
            timeout,
            cancel.clone(),
            outbound.clone(),
            sequence.clone(),
        )
        .await
        {
            Ok(_) => return Ok(()),
            Err(ProcessError::Exit(_)) => {
                let data = if immutable_snapshot.is_some() {
                    "Filtered exact-SHA fetch was unavailable; using the immutable run snapshot.\n"
                } else {
                    "Filtered exact-SHA fetch was unavailable; retrying the authenticated snapshot without a filter.\n"
                };
                send(
                    outbound,
                    AgentMessage::LogChunk {
                        message_id: Uuid::new_v4(),
                        job_id,
                        step_id: step_id.to_owned(),
                        sequence: sequence.fetch_add(1, Ordering::Relaxed),
                        stream: LogStream::System,
                        data: data.to_owned(),
                    },
                )
                .await?;
            }
            Err(error) => return Err(error.into()),
        }
    }

    let mut command = Command::new("git");
    command.args(["fetch", "--no-tags", "--depth=1"]);
    if show_progress {
        command.arg("--progress");
    } else {
        command.arg("--quiet");
    }
    if let Some(immutable_snapshot) = immutable_snapshot {
        command.arg(immutable_snapshot);
    } else {
        command.arg("origin");
    }
    command
        .arg(expected_sha)
        .current_dir(checkout_directory)
        .envs(environment.iter())
        .env("GIT_TERMINAL_PROMPT", "0");
    run_process(
        job_id,
        step_id,
        &mut command,
        timeout,
        cancel.clone(),
        outbound.clone(),
        sequence.clone(),
    )
    .await
    .map_err(anyhow::Error::from)
    .context("fetch exact authenticated pull-request snapshot into job workspace")?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn configure_sparse_checkout(
    job_id: Uuid,
    step_id: &str,
    checkout_directory: &Path,
    patterns: Option<&[String]>,
    cone_mode: bool,
    environment: &BTreeMap<String, String>,
    timeout: Option<Duration>,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
) -> Result<()> {
    let sparse_enabled = Command::new("git")
        .args(["config", "--bool", "--get", "core.sparseCheckout"])
        .current_dir(checkout_directory)
        .output()
        .await
        .context("inspect sparse-checkout configuration")?
        .stdout
        .eq_ignore_ascii_case(b"true\n");
    let mut command = Command::new("git");
    match patterns {
        Some(patterns) => {
            command.args([
                "sparse-checkout",
                "set",
                if cone_mode { "--cone" } else { "--no-cone" },
                "--",
            ]);
            command.args(patterns);
        }
        None if sparse_enabled => {
            command.args(["sparse-checkout", "disable"]);
        }
        None => return Ok(()),
    }
    command
        .current_dir(checkout_directory)
        .envs(environment.iter());
    run_process(
        job_id,
        step_id,
        &mut command,
        timeout,
        cancel.clone(),
        outbound.clone(),
        sequence.clone(),
    )
    .await
    .map_err(anyhow::Error::from)
    .context("configure actions/checkout sparse working tree")?;
    if patterns.is_none() {
        let output = Command::new("git")
            .args([
                "config",
                "--local",
                "--unset-all",
                "extensions.worktreeConfig",
            ])
            .current_dir(checkout_directory)
            .output()
            .await
            .context("clear sparse-checkout worktree configuration")?;
        if !output.status.success() && output.status.code() != Some(5) {
            bail!("failed to clear sparse-checkout worktree configuration");
        }
    }
    Ok(())
}

fn select_checkout_token<'a>(
    input: Option<&'a str>,
    builtin: &'a str,
    managed_secret_values: &BTreeSet<String>,
) -> Result<&'a str> {
    match input {
        None => Ok(builtin),
        Some("") => Ok(""),
        Some(token) if token == builtin => Ok(token),
        Some(token) if managed_secret_values.contains(token) => Ok(token),
        Some(_) => bail!(
            "actions/checkout token must be the built-in token, empty, or an exact managed secret value"
        ),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CrossRepositoryCheckoutAccess {
    AnonymousOnly,
    ManagedFallback,
    ExplicitManagedToken(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RemoteRepositoryMaterializationScope {
    SharedSource,
    CheckoutAnonymous,
    CheckoutManaged,
    CheckoutSsh,
}

impl RemoteRepositoryMaterializationScope {
    fn directory_name(self) -> &'static str {
        match self {
            Self::SharedSource => "shared-source",
            Self::CheckoutAnonymous => "checkout-anonymous",
            Self::CheckoutManaged => "checkout-managed",
            Self::CheckoutSsh => "checkout-ssh",
        }
    }
}

fn select_cross_repository_checkout_access(
    input: Option<&str>,
    builtin: &str,
    managed_secret_values: &BTreeSet<String>,
) -> Result<CrossRepositoryCheckoutAccess> {
    match input {
        Some("") => Ok(CrossRepositoryCheckoutAccess::AnonymousOnly),
        None => Ok(CrossRepositoryCheckoutAccess::ManagedFallback),
        Some(token) if token == builtin => Ok(CrossRepositoryCheckoutAccess::ManagedFallback),
        Some(token) if managed_secret_values.contains(token) => Ok(
            CrossRepositoryCheckoutAccess::ExplicitManagedToken(token.to_owned()),
        ),
        Some(_) => bail!(
            "actions/checkout token for another repository must be the built-in token, empty, or an exact managed secret value"
        ),
    }
}

const CHECKOUT_HTTP_EXTRAHEADER: &str = "http.https://github.com/.extraheader";
const CHECKOUT_SSH_INSTEAD_OF: &str = "url.https://github.com/.insteadOf";
const CHECKOUT_SAFE_DIRECTORY: &str = "safe.directory";
const MAX_INLINE_GIT_CONFIG_PARAMETERS: usize = 256;

fn configure_checkout_credentials(
    environment: &mut BTreeMap<String, String>,
    token: &str,
    rewrite_ssh_submodule_urls: bool,
) -> Result<()> {
    let count = match environment.get("GIT_CONFIG_COUNT") {
        Some(value) => value
            .parse::<usize>()
            .context("GIT_CONFIG_COUNT must be a nonnegative integer")?,
        None => 0,
    };
    if count > MAX_INLINE_GIT_CONFIG_PARAMETERS + 2 {
        bail!("GIT_CONFIG_COUNT exceeds the checkout compatibility limit");
    }
    let mut entries = Vec::with_capacity(count.saturating_add(2));
    for index in 0..count {
        let key = environment
            .get(&format!("GIT_CONFIG_KEY_{index}"))
            .context("GIT_CONFIG_COUNT references a missing key")?;
        let value = environment
            .get(&format!("GIT_CONFIG_VALUE_{index}"))
            .context("GIT_CONFIG_COUNT references a missing value")?;
        if !key.eq_ignore_ascii_case(CHECKOUT_HTTP_EXTRAHEADER)
            && !key.eq_ignore_ascii_case(CHECKOUT_SSH_INSTEAD_OF)
        {
            entries.push((key.clone(), value.clone()));
        }
    }
    let managed_entry_count =
        usize::from(!token.is_empty()) + usize::from(rewrite_ssh_submodule_urls);
    if entries.len() + managed_entry_count > MAX_INLINE_GIT_CONFIG_PARAMETERS + 2 {
        bail!("inline Git configuration leaves no room for checkout credentials");
    }

    environment.remove("GIT_CONFIG_COUNT");
    for index in 0..count.max(2) {
        environment.remove(&format!("GIT_CONFIG_KEY_{index}"));
        environment.remove(&format!("GIT_CONFIG_VALUE_{index}"));
    }

    if !token.is_empty() {
        let credential = STANDARD.encode(format!("x-access-token:{token}"));
        entries.push((
            CHECKOUT_HTTP_EXTRAHEADER.to_owned(),
            format!("AUTHORIZATION: basic {credential}"),
        ));
    }
    if rewrite_ssh_submodule_urls {
        entries.push((
            CHECKOUT_SSH_INSTEAD_OF.to_owned(),
            "git@github.com:".to_owned(),
        ));
    }
    for (index, (key, value)) in entries.iter().enumerate() {
        environment.insert(format!("GIT_CONFIG_KEY_{index}"), key.clone());
        environment.insert(format!("GIT_CONFIG_VALUE_{index}"), value.clone());
    }
    if !entries.is_empty() {
        environment.insert("GIT_CONFIG_COUNT".to_owned(), entries.len().to_string());
    }
    Ok(())
}

fn configure_checkout_ssh(environment: &mut BTreeMap<String, String>, command: Option<&str>) {
    environment.remove("GIT_SSH_COMMAND");
    if let Some(command) = command {
        environment.insert("GIT_SSH_COMMAND".to_owned(), command.to_owned());
    }
}

async fn configure_checkout_safe_directory(
    environment: &mut BTreeMap<String, String>,
    directory: &Path,
    run_dir: &Path,
    enabled: bool,
) -> Result<Option<SensitiveDirectoryGuard>> {
    if !enabled {
        return Ok(None);
    }
    let runner_temp = environment
        .get("RUNNER_TEMP")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| run_dir.join("_temp"));
    tokio::fs::create_dir_all(&runner_temp)
        .await
        .context("create actions/checkout temporary directory")?;
    let temporary_home = runner_temp.join(format!("checkout-global-{}", Uuid::new_v4()));
    tokio::fs::create_dir(&temporary_home)
        .await
        .context("create actions/checkout temporary Git home")?;
    let guard = SensitiveDirectoryGuard(temporary_home.clone());
    let temporary_config = temporary_home.join(".gitconfig");
    let original_config = environment
        .get("GIT_CONFIG_GLOBAL")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            environment
                .get("HOME")
                .filter(|path| !path.is_empty())
                .map(|home| Path::new(home).join(".gitconfig"))
        })
        .or_else(|| {
            std::env::var_os("HOME")
                .filter(|home| !home.is_empty())
                .map(|home| PathBuf::from(home).join(".gitconfig"))
        });
    let copied = if let Some(original_config) = original_config {
        match tokio::fs::copy(&original_config, &temporary_config).await {
            Ok(_) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "copy global Git configuration {} for actions/checkout",
                        original_config.display()
                    )
                });
            }
        }
    } else {
        false
    };
    if !copied {
        write_private_checkout_file(&temporary_config, b"")
            .context("create actions/checkout temporary global Git configuration")?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&temporary_config, std::fs::Permissions::from_mode(0o600))
            .context("restrict actions/checkout temporary global Git configuration")?;
    }

    environment.insert("HOME".to_owned(), temporary_home.display().to_string());
    environment.insert(
        "GIT_CONFIG_GLOBAL".to_owned(),
        temporary_config.display().to_string(),
    );
    let output = Command::new("git")
        .args(["config", "--global", "--add", CHECKOUT_SAFE_DIRECTORY])
        .arg(directory)
        .envs(environment.iter())
        .output()
        .await
        .context("run Git while configuring actions/checkout safe.directory")?;
    if !output.status.success() {
        bail!(
            "configure actions/checkout safe.directory: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(Some(guard))
}

fn checkout_directory(workspace: &Path, path: &str) -> Result<PathBuf> {
    if path.is_empty() || path == "." {
        return Ok(workspace.to_owned());
    }
    let relative = Path::new(path);
    if relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
    {
        bail!("actions/checkout path must stay within the job workspace");
    }
    Ok(workspace.join(relative))
}

async fn validate_checkout_directory(workspace: &Path, directory: &Path) -> Result<PathBuf> {
    let relative = directory
        .strip_prefix(workspace)
        .context("actions/checkout path escapes the job workspace")?;
    let relative = relative.components().collect::<PathBuf>();
    let workspace = tokio::fs::canonicalize(workspace)
        .await
        .context("canonicalize job workspace")?;
    let mut current = workspace.clone();
    for component in relative.components() {
        current.push(component);
        match tokio::fs::symlink_metadata(&current).await {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                bail!("actions/checkout path traverses a symbolic link");
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(error).context("inspect actions/checkout path"),
        }
    }
    if relative.as_os_str().is_empty() {
        Ok(workspace)
    } else {
        Ok(workspace.join(&relative))
    }
}

async fn git_repository_exists(
    directory: &Path,
    environment: &BTreeMap<String, String>,
) -> Result<bool> {
    if !tokio::fs::try_exists(directory).await? {
        return Ok(false);
    }
    let git_directory = directory.join(".git");
    let metadata = match tokio::fs::symlink_metadata(&git_directory).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error).context("inspect checkout Git directory"),
    };
    // Git normally walks into parent directories. A job workspace nested under
    // any unrelated repository must never be mistaken for that repository.
    // GitZero initializes ordinary repositories itself, so a direct, real
    // directory is also sufficient to reject malicious gitdir indirection.
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Ok(false);
    }
    let expected = tokio::fs::canonicalize(&git_directory)
        .await
        .context("canonicalize checkout Git directory")?;
    let output = Command::new("git")
        .args(["rev-parse", "--absolute-git-dir"])
        .current_dir(directory)
        .envs(environment.iter())
        .output()
        .await;
    let Ok(output) = output else {
        return Ok(false);
    };
    if !output.status.success() {
        return Ok(false);
    }
    let actual = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
    Ok(tokio::fs::canonicalize(actual)
        .await
        .is_ok_and(|actual| actual == expected))
}

async fn git_repository_object_format(
    directory: &Path,
    environment: &BTreeMap<String, String>,
) -> Result<GitObjectFormat> {
    let output = Command::new("git")
        .args(["config", "--local", "--get", "extensions.objectFormat"])
        .current_dir(directory)
        .envs(environment.iter())
        .output()
        .await
        .context("inspect checkout repository object format")?;
    if output.status.success() {
        return match String::from_utf8_lossy(&output.stdout).trim() {
            "sha1" => Ok(GitObjectFormat::Sha1),
            "sha256" => Ok(GitObjectFormat::Sha256),
            _ => bail!("checkout repository uses an unsupported Git object format"),
        };
    }
    if output.status.code() == Some(1) && output.stderr.is_empty() {
        return Ok(GitObjectFormat::Sha1);
    }
    bail!(
        "inspect checkout repository object format: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    )
}

async fn clear_checkout_directory(directory: &Path) -> Result<()> {
    if !tokio::fs::try_exists(directory).await? {
        return Ok(());
    }
    let metadata = tokio::fs::symlink_metadata(directory).await?;
    if !metadata.is_dir() {
        tokio::fs::remove_file(directory).await?;
        return Ok(());
    }
    let mut entries = tokio::fs::read_dir(directory)
        .await
        .with_context(|| format!("read checkout directory {}", directory.display()))?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let metadata = tokio::fs::symlink_metadata(&path).await?;
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            tokio::fs::remove_dir_all(&path).await?;
        } else {
            tokio::fs::remove_file(&path).await?;
        }
    }
    Ok(())
}

fn checkout_boolean(inputs: &BTreeMap<String, String>, name: &str, default: bool) -> Result<bool> {
    match inputs
        .get(name)
        .map(|value| value.trim().to_ascii_lowercase())
    {
        None => Ok(default),
        Some(value) if value == "true" => Ok(true),
        Some(value) if value == "false" => Ok(false),
        Some(_) => bail!("actions/checkout input '{name}' must be true or false"),
    }
}

fn parse_timeout(
    timeout_minutes: Option<&str>,
    context: &EvaluationContext,
) -> Result<Option<Duration>> {
    let Some(timeout_minutes) = timeout_minutes else {
        return Ok(None);
    };
    let rendered = context.render(timeout_minutes)?;
    let minutes = rendered
        .parse::<u64>()
        .with_context(|| format!("timeout-minutes '{rendered}' is not a positive integer"))?;
    if !(1..=360).contains(&minutes) {
        bail!("timeout-minutes must be a positive integer no greater than 360");
    }
    Ok(Some(Duration::from_secs(minutes * 60)))
}

#[allow(clippy::too_many_arguments)]
async fn execute_action_step(
    job_id: Uuid,
    step_id: &str,
    step_name: &str,
    action: &str,
    supplied_inputs: &BTreeMap<String, String>,
    run: &RunSpec,
    workspace: &Path,
    run_dir: &Path,
    environment: &mut BTreeMap<String, String>,
    visible_environment: &BTreeMap<String, String>,
    context: &EvaluationContext,
    continue_on_error: bool,
    timeout: Option<Duration>,
    posts: &mut Vec<ActionPost>,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
    job_timed_out: &AtomicBool,
    job_summary: &mut JobSummaryBuilder,
    workflow_commands: &Arc<StdMutex<WorkflowCommandProcessor>>,
    repository_access: &RunRepositoryAccess,
    checkout_secret_values: &BTreeSet<String>,
) -> Result<StepExecution> {
    let step_timed_out = Arc::new(AtomicBool::new(false));
    let (effective_cancel, timeout_task) =
        deadline_cancellation(cancel, timeout, step_timed_out.clone());
    let _timeout_task = AbortTaskOnDrop(timeout_task);
    let cancel = &effective_cancel;
    let reference = ActionReference::parse(action)?;
    let action_directory = materialize_action(
        job_id,
        step_id,
        &reference,
        run,
        workspace,
        run_dir,
        cancel,
        outbound,
        sequence,
        repository_access,
    )
    .await;
    let action_directory = match action_directory {
        Ok(directory) => directory,
        Err(_error)
            if step_timed_out.load(Ordering::Acquire)
                || job_timed_out.load(Ordering::Acquire)
                || *cancel.borrow() =>
        {
            let outcome = if job_timed_out.load(Ordering::Acquire) {
                JobConclusion::TimedOut
            } else if step_timed_out.load(Ordering::Acquire) {
                JobConclusion::Failure
            } else {
                JobConclusion::Cancelled
            };
            let conclusion = if continue_on_error && outcome == JobConclusion::Failure {
                JobConclusion::Success
            } else {
                outcome
            };
            send(
                outbound,
                AgentMessage::StepFinished {
                    message_id: Uuid::new_v4(),
                    job_id,
                    step_id: step_id.to_owned(),
                    conclusion: protocol_conclusion(conclusion),
                    exit_code: None,
                },
            )
            .await?;
            return Ok(StepExecution {
                conclusion,
                outcome,
                outputs: BTreeMap::new(),
                ran: true,
            });
        }
        Err(error) => return Err(error),
    };
    let definition = load_definition(&action_directory).await?;
    let mut action_context = context.clone();
    action_context.extend_json_object(
        "github",
        serde_json::Map::from_iter([
            (
                "action_path".to_owned(),
                JsonValue::String(action_directory.display().to_string()),
            ),
            (
                "action_ref".to_owned(),
                reference
                    .git_ref()
                    .map_or(JsonValue::Null, |value| JsonValue::String(value.to_owned())),
            ),
            (
                "action_repository".to_owned(),
                reference
                    .repository_name()
                    .map_or(JsonValue::Null, JsonValue::String),
            ),
        ]),
    )?;
    let rendered_supplied = render_environment(supplied_inputs, &action_context)?;
    let inputs = resolve_action_inputs(&definition, &rendered_supplied, &action_context)?;
    action_context.insert_json("inputs", serde_json::to_value(&inputs)?)?;

    let mut action_environment = visible_environment.clone();
    for (name, value) in &inputs {
        action_environment.insert(action_input_environment_name(name), value.clone());
    }
    action_environment.insert(
        "GITHUB_ACTION_PATH".to_owned(),
        action_directory.display().to_string(),
    );
    if let Some(repository) = reference.repository_name() {
        action_environment.insert("GITHUB_ACTION_REPOSITORY".to_owned(), repository);
    }
    if let Some(git_ref) = reference.git_ref() {
        action_environment.insert("GITHUB_ACTION_REF".to_owned(), git_ref.to_owned());
    }

    if definition.runs.using == "composite" {
        let mut execution = execute_composite_action(
            job_id,
            step_id,
            &definition,
            run,
            workspace,
            run_dir,
            environment,
            &action_environment,
            &action_context,
            posts,
            cancel,
            outbound,
            sequence,
            job_timed_out,
            job_summary,
            workflow_commands,
            repository_access,
            checkout_secret_values,
        )
        .await?;
        if job_timed_out.load(Ordering::Acquire) {
            execution.conclusion = JobConclusion::TimedOut;
            execution.outputs.clear();
            execution.exit_code = None;
        } else if step_timed_out.load(Ordering::Acquire) {
            execution.conclusion = JobConclusion::Failure;
            execution.outputs.clear();
            execution.exit_code = None;
        }
        let outcome = execution.conclusion;
        let conclusion = if continue_on_error && outcome == JobConclusion::Failure {
            JobConclusion::Success
        } else {
            outcome
        };
        send(
            outbound,
            AgentMessage::StepFinished {
                message_id: Uuid::new_v4(),
                job_id,
                step_id: step_id.to_owned(),
                conclusion: protocol_conclusion(conclusion),
                exit_code: execution.exit_code,
            },
        )
        .await?;
        return Ok(StepExecution {
            conclusion,
            outcome,
            outputs: execution.outputs,
            ran: true,
        });
    }
    if !definition.runs.using.starts_with("node") {
        bail!(
            "action '{}' uses unsupported runtime '{}'",
            definition.name,
            definition.runs.using
        );
    }

    let mut state = BTreeMap::new();
    let mut execution = ActionPhaseExecution {
        conclusion: JobConclusion::Success,
        outputs: BTreeMap::new(),
        state: BTreeMap::new(),
        exit_code: Some(0),
    };
    if let Some(pre) = &definition.runs.pre {
        let condition = definition
            .runs
            .pre_if
            .as_ref()
            .map_or("always()", |condition| condition.as_str());
        if action_context.evaluate_condition(condition)? {
            let summary_name = format!("{step_name} (pre)");
            execution = execute_node_action_phase(
                job_id,
                step_id,
                "pre",
                &action_directory,
                pre,
                None,
                workspace,
                run_dir,
                environment,
                &action_environment,
                &state,
                cancel,
                outbound,
                sequence,
                job_timed_out,
                job_summary,
                &summary_name,
                workflow_commands,
            )
            .await?;
            state.extend(execution.state.clone());
        }
    }
    if execution.conclusion == JobConclusion::Success {
        let main = definition
            .runs
            .main
            .as_deref()
            .context("JavaScript action metadata is missing runs.main")?;
        let summary_name = format!("{step_name} (main)");
        execution = execute_node_action_phase(
            job_id,
            step_id,
            "main",
            &action_directory,
            main,
            None,
            workspace,
            run_dir,
            environment,
            &action_environment,
            &state,
            cancel,
            outbound,
            sequence,
            job_timed_out,
            job_summary,
            &summary_name,
            workflow_commands,
        )
        .await?;
        state.extend(execution.state.clone());
    }

    if let Some(post) = &definition.runs.post {
        posts.push(ActionPost {
            name: step_name.to_owned(),
            step_id: step_id.to_owned(),
            action_directory,
            script: post.clone(),
            environment: action_environment,
            state,
            condition: definition.runs.post_if.as_ref().map_or_else(
                || "always()".to_owned(),
                |condition| condition.as_str().to_owned(),
            ),
            context: action_context,
        });
    }

    if job_timed_out.load(Ordering::Acquire) {
        execution.conclusion = JobConclusion::TimedOut;
        execution.outputs.clear();
        execution.exit_code = None;
    } else if step_timed_out.load(Ordering::Acquire) {
        execution.conclusion = JobConclusion::Failure;
        execution.outputs.clear();
        execution.exit_code = None;
    }
    let outcome = execution.conclusion;
    let conclusion = if continue_on_error && outcome == JobConclusion::Failure {
        JobConclusion::Success
    } else {
        outcome
    };
    send(
        outbound,
        AgentMessage::StepFinished {
            message_id: Uuid::new_v4(),
            job_id,
            step_id: step_id.to_owned(),
            conclusion: protocol_conclusion(conclusion),
            exit_code: execution.exit_code,
        },
    )
    .await?;
    Ok(StepExecution {
        conclusion,
        outcome,
        outputs: execution.outputs,
        ran: true,
    })
}

#[allow(clippy::too_many_arguments)]
async fn execute_composite_action(
    job_id: Uuid,
    parent_step_id: &str,
    definition: &ActionDefinition,
    run: &RunSpec,
    workspace: &Path,
    run_dir: &Path,
    environment: &mut BTreeMap<String, String>,
    visible_environment: &BTreeMap<String, String>,
    action_context: &EvaluationContext,
    posts: &mut Vec<ActionPost>,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
    job_timed_out: &AtomicBool,
    job_summary: &mut JobSummaryBuilder,
    workflow_commands: &Arc<StdMutex<WorkflowCommandProcessor>>,
    repository_access: &RunRepositoryAccess,
    checkout_secret_values: &BTreeSet<String>,
) -> Result<ActionPhaseExecution> {
    if definition.runs.main.is_some()
        || definition.runs.pre.is_some()
        || definition.runs.post.is_some()
    {
        bail!(
            "composite action '{}' mixes JavaScript entrypoints with steps",
            definition.name
        );
    }
    let initial_environment = visible_environment.clone();
    let mut composite_environment = initial_environment.clone();
    let mut steps = serde_json::Map::new();
    let mut status = ExecutionStatus::Success;

    for (index, source_step) in definition.runs.steps.iter().enumerate() {
        let planned = plan_composite_step(source_step, index)?;
        let mut context = action_context.clone();
        context.set_status(status);
        context.extend_json_object(
            "github",
            serde_json::Map::from_iter([(
                "action_status".to_owned(),
                JsonValue::String(execution_status_result(status).to_owned()),
            )]),
        )?;
        context.insert_json("env", serde_json::to_value(&composite_environment)?)?;
        context.insert_json("steps", JsonValue::Object(steps.clone()))?;
        let execution = Box::pin(execute_step(
            job_id,
            &definition.name,
            parent_step_id,
            &planned,
            run,
            workspace,
            run_dir,
            &mut composite_environment,
            &context,
            status,
            posts,
            cancel,
            outbound,
            sequence,
            job_timed_out,
            job_summary,
            workflow_commands,
            repository_access,
            checkout_secret_values,
        ))
        .await?;
        steps.insert(
            planned.id.clone(),
            json!({
                "outputs": execution.outputs,
                "outcome": execution.outcome.as_github_result(),
                "conclusion": execution.conclusion.as_github_result(),
            }),
        );
        match execution.conclusion {
            JobConclusion::Failure | JobConclusion::TimedOut => status = ExecutionStatus::Failure,
            JobConclusion::Cancelled => status = ExecutionStatus::Cancelled,
            JobConclusion::Success | JobConclusion::Skipped => {}
        }
        if status == ExecutionStatus::Cancelled {
            break;
        }
    }

    for (name, value) in &composite_environment {
        if initial_environment.get(name) != Some(value) {
            environment.insert(name.clone(), value.clone());
        }
    }
    let mut output_context = action_context.clone();
    output_context.set_status(status);
    output_context.extend_json_object(
        "github",
        serde_json::Map::from_iter([(
            "action_status".to_owned(),
            JsonValue::String(execution_status_result(status).to_owned()),
        )]),
    )?;
    output_context.insert_json("env", serde_json::to_value(&composite_environment)?)?;
    output_context.insert_json("steps", JsonValue::Object(steps))?;
    let outputs = definition
        .outputs
        .iter()
        .filter_map(|(name, output)| output.value.as_ref().map(|value| (name, value)))
        .map(|(name, value)| {
            output_context
                .render(value.as_str())
                .map(|value| (name.clone(), value))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let conclusion = match status {
        ExecutionStatus::Success => JobConclusion::Success,
        ExecutionStatus::Failure => JobConclusion::Failure,
        ExecutionStatus::Cancelled => JobConclusion::Cancelled,
        ExecutionStatus::Skipped => JobConclusion::Skipped,
    };
    Ok(ActionPhaseExecution {
        conclusion,
        outputs,
        state: BTreeMap::new(),
        exit_code: match conclusion {
            JobConclusion::Success => Some(0),
            JobConclusion::Failure => Some(1),
            JobConclusion::TimedOut | JobConclusion::Cancelled | JobConclusion::Skipped => None,
        },
    })
}

fn plan_composite_step(step: &ActionStep, index: usize) -> Result<PlannedStep> {
    if let Some(feature) = step.extra.keys().next() {
        bail!("composite step uses unsupported key '{feature}'");
    }
    let id = step
        .id
        .clone()
        .unwrap_or_else(|| format!("composite-step-{}", index + 1));
    let name = step.name.clone().unwrap_or_else(|| {
        step.uses
            .clone()
            .or_else(|| step.run.as_ref().map(|_| "Run".to_owned()))
            .unwrap_or_else(|| id.clone())
    });
    let inputs = step
        .with
        .iter()
        .map(|(name, value)| (name.clone(), value.as_str().to_owned()))
        .collect::<BTreeMap<_, _>>();
    let kind = match (&step.run, &step.uses) {
        (Some(script), None) if inputs.is_empty() => {
            let shell = step
                .shell
                .clone()
                .context("composite run steps must declare a shell")?;
            StepKind::Run {
                shell,
                script: script.clone(),
            }
        }
        (Some(_), None) => bail!("composite run steps cannot declare with inputs"),
        (None, Some(action)) if action.starts_with("actions/checkout@") => {
            StepKind::Checkout { inputs }
        }
        (None, Some(action)) => StepKind::Uses {
            action: action.clone(),
            inputs,
        },
        _ => bail!("composite steps must define exactly one of run or uses"),
    };
    let github_action = step.id.clone().unwrap_or_else(|| match &kind {
        StepKind::Run { .. } => {
            if index == 0 {
                "__run".to_owned()
            } else {
                format!("__run_{}", index + 1)
            }
        }
        StepKind::Checkout { .. } => "actionscheckout".to_owned(),
        StepKind::Uses { action, .. } => action
            .split('@')
            .next()
            .unwrap_or(action)
            .chars()
            .filter(char::is_ascii_alphanumeric)
            .collect(),
    });
    Ok(PlannedStep {
        id,
        github_action,
        name,
        environment: step
            .env
            .iter()
            .map(|(name, value)| (name.clone(), value.as_str().to_owned()))
            .collect(),
        working_directory: step.working_directory.clone(),
        condition: step
            .condition
            .as_ref()
            .map(|condition| condition.as_str().to_owned()),
        continue_on_error: step
            .continue_on_error
            .as_ref()
            .map(|condition| condition.as_str().to_owned()),
        timeout_minutes: None,
        kind,
    })
}

fn resolve_action_inputs(
    definition: &ActionDefinition,
    supplied: &BTreeMap<String, String>,
    context: &EvaluationContext,
) -> Result<BTreeMap<String, String>> {
    let mut inputs = supplied
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    for (name, definition) in &definition.inputs {
        let name = name.to_ascii_lowercase();
        if !inputs.contains_key(&name)
            && let Some(default) = &definition.default
        {
            let mut default_context = context.clone();
            default_context.insert_json("inputs", serde_json::to_value(&inputs)?)?;
            inputs.insert(name.clone(), default_context.render(default.as_str())?);
        }
        if definition.required && !inputs.contains_key(&name) {
            bail!("required action input '{name}' was not supplied");
        }
    }
    Ok(inputs)
}

fn action_input_environment_name(name: &str) -> String {
    format!("INPUT_{}", name.replace(' ', "_").to_ascii_uppercase())
}

#[allow(clippy::too_many_arguments)]
async fn materialize_action(
    job_id: Uuid,
    step_id: &str,
    reference: &ActionReference,
    run: &RunSpec,
    workspace: &Path,
    run_dir: &Path,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
    repository_access: &RunRepositoryAccess,
) -> Result<PathBuf> {
    match reference {
        ActionReference::Local { path } => {
            let directory = workspace.join(path);
            ensure_within(workspace, &directory).await?;
            Ok(directory)
        }
        ActionReference::Remote {
            owner,
            repository,
            path,
            git_ref,
        } => {
            let remote = format!("https://github.com/{owner}/{repository}.git");
            let checkout = materialize_remote_repository_with_shared_access(
                job_id,
                step_id,
                owner,
                repository,
                git_ref,
                &remote,
                run,
                repository_access,
                run_dir,
                cancel,
                outbound,
                sequence,
            )
            .await?;
            let directory = if path.is_empty() {
                checkout.clone()
            } else {
                checkout.join(path)
            };
            ensure_within(&checkout, &directory).await?;
            Ok(directory)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn materialize_remote_repository_with_shared_access(
    job_id: Uuid,
    step_id: &str,
    owner: &str,
    repository: &str,
    git_ref: &str,
    remote: &str,
    run: &RunSpec,
    repository_access: &RunRepositoryAccess,
    run_dir: &Path,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
) -> Result<PathBuf> {
    let same_repository = owner.eq_ignore_ascii_case(&run.repository.owner)
        && repository.eq_ignore_ascii_case(&run.repository.name);
    let cached_token = if same_repository {
        repository_access.source_token(run, false, cancel).await?
    } else {
        repository_access
            .cached_token(RepositoryTokenPurpose::SharedSource, owner, repository)
            .await
    };
    let first = materialize_remote_repository(
        job_id,
        step_id,
        owner,
        repository,
        git_ref,
        remote,
        RemoteRepositoryMaterializationScope::SharedSource,
        cached_token.as_deref(),
        run_dir,
        cancel,
        outbound,
        sequence,
    )
    .await;
    let first_error = match first {
        Ok(checkout) => return Ok(checkout),
        Err(error) => error,
    };
    let token = if same_repository {
        repository_access
            .source_token(run, true, cancel)
            .await?
            .with_context(|| {
                format!(
                    "fetch {owner}/{repository}@{git_ref} without usable source-repository access: {first_error:#}"
                )
            })?
    } else {
        repository_access
            .request_token(
                run.id,
                RepositoryTokenPurpose::SharedSource,
                owner,
                repository,
                cached_token.is_some(),
                cancel,
            )
            .await
            .with_context(|| {
                format!(
                    "fetch {owner}/{repository}@{git_ref} without usable shared-repository access: {first_error:#}"
                )
            })?
    };
    materialize_remote_repository(
        job_id,
        step_id,
        owner,
        repository,
        git_ref,
        remote,
        RemoteRepositoryMaterializationScope::SharedSource,
        Some(&token),
        run_dir,
        cancel,
        outbound,
        sequence,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn materialize_remote_repository(
    job_id: Uuid,
    step_id: &str,
    owner: &str,
    repository: &str,
    git_ref: &str,
    remote: &str,
    scope: RemoteRepositoryMaterializationScope,
    checkout_token: Option<&str>,
    run_dir: &Path,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
) -> Result<PathBuf> {
    materialize_remote_repository_with_fetch(
        job_id,
        step_id,
        owner,
        repository,
        git_ref,
        remote,
        "origin",
        scope,
        checkout_token,
        None,
        run_dir,
        cancel,
        outbound,
        sequence,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn materialize_remote_repository_with_fetch(
    job_id: Uuid,
    step_id: &str,
    owner: &str,
    repository: &str,
    git_ref: &str,
    cache_remote: &str,
    fetch_remote: &str,
    scope: RemoteRepositoryMaterializationScope,
    checkout_token: Option<&str>,
    checkout_environment: Option<&BTreeMap<String, String>>,
    run_dir: &Path,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
) -> Result<PathBuf> {
    let encoded_ref = URL_SAFE_NO_PAD.encode(git_ref);
    let checkout = run_dir
        .join("_actions")
        .join(sanitize_id(
            step_id.rsplit_once('/').map_or("job", |(job, _)| job),
        ))
        .join(scope.directory_name())
        .join(owner)
        .join(repository)
        .join(&encoded_ref);
    if tokio::fs::try_exists(&checkout).await? {
        return Ok(checkout);
    }

    let cache = remote_repository_cache_path(run_dir, owner, repository)?;
    let lock = remote_repository_lock(&cache);
    let _guard = lock.lock().await;
    if tokio::fs::try_exists(&checkout).await? {
        return Ok(checkout);
    }

    ensure_remote_repository_cache(
        job_id,
        step_id,
        &cache,
        cache_remote,
        git_ref,
        if fetch_remote == "origin" {
            cache_remote
        } else {
            fetch_remote
        },
        checkout_token,
        checkout_environment,
        cancel,
        outbound,
        sequence,
    )
    .await?;
    let resolution = remote_repository_resolution_path(run_dir, owner, repository, git_ref);
    let mut commit = read_cached_remote_resolution(&cache, &resolution).await?;
    let resolution_hit = commit.is_some();
    if resolution_hit && (checkout_token.is_none() || checkout_environment.is_some()) {
        let mut probe = Command::new("git");
        probe
            .arg("--git-dir")
            .arg(&cache)
            .args(["ls-remote", "--quiet", fetch_remote, "HEAD"]);
        configure_remote_repository_access(&mut probe, checkout_token, checkout_environment);
        run_process(
            job_id,
            step_id,
            &mut probe,
            None,
            cancel.clone(),
            outbound.clone(),
            sequence.clone(),
        )
        .await
        .map_err(anyhow::Error::from)
        .with_context(|| format!("verify selected access to {owner}/{repository}"))?;
    }
    if commit.is_none() {
        let mut fetch = Command::new("git");
        fetch.arg("--git-dir").arg(&cache).args([
            "fetch",
            "--quiet",
            "--no-tags",
            "--depth=1",
            "--force",
            fetch_remote,
            git_ref,
        ]);
        configure_remote_repository_access(&mut fetch, checkout_token, checkout_environment);
        run_process(
            job_id,
            step_id,
            &mut fetch,
            None,
            cancel.clone(),
            outbound.clone(),
            sequence.clone(),
        )
        .await
        .map_err(anyhow::Error::from)
        .with_context(|| format!("fetch remote repository {owner}/{repository}@{git_ref}"))?;
        let resolved = git_output(
            Command::new("git").arg("--git-dir").arg(&cache).args([
                "rev-parse",
                "--verify",
                "FETCH_HEAD^{commit}",
            ]),
            "resolve fetched remote commit",
        )
        .await?;
        validate_remote_commit(&resolved)?;
        if let Some(parent) = resolution.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&resolution, format!("{resolved}\n"))
            .await
            .with_context(|| format!("write remote resolution {}", resolution.display()))?;
        commit = Some(resolved);
    }
    let commit = commit.expect("remote commit was fetched or cached");
    run_process(
        job_id,
        step_id,
        Command::new("git").arg("--git-dir").arg(&cache).args([
            "update-ref",
            &format!("refs/heads/gitzero-cache/{commit}"),
            &commit,
        ]),
        None,
        cancel.clone(),
        outbound.clone(),
        sequence.clone(),
    )
    .await
    .map_err(anyhow::Error::from)
    .context("retain resolved remote commit")?;

    let checkout_parent = checkout.parent().context("action checkout has parent")?;
    tokio::fs::create_dir_all(checkout_parent).await?;
    let temporary = checkout_parent.join(format!(".checkout-{}", Uuid::new_v4()));
    let mut clone = Command::new("git");
    clone
        .args(["clone", "--quiet", "--shared", "--no-checkout"])
        .arg(&cache)
        .arg(&temporary)
        .env("GIT_TERMINAL_PROMPT", "0");
    run_process(
        job_id,
        step_id,
        &mut clone,
        None,
        cancel.clone(),
        outbound.clone(),
        sequence.clone(),
    )
    .await
    .map_err(anyhow::Error::from)
    .context("materialize cached remote repository")?;
    run_process(
        job_id,
        step_id,
        Command::new("git")
            .args(["checkout", "--quiet", "--detach", &commit])
            .current_dir(&temporary),
        None,
        cancel.clone(),
        outbound.clone(),
        sequence.clone(),
    )
    .await
    .map_err(anyhow::Error::from)
    .context("checkout cached remote commit")?;
    tokio::fs::rename(&temporary, &checkout)
        .await
        .with_context(|| format!("publish action checkout {}", checkout.display()))?;
    info!(
        source = %format_args!("{owner}/{repository}@{git_ref}"),
        %commit,
        resolution_hit,
        "materialized remote repository"
    );
    Ok(checkout)
}

fn remote_repository_resolution_path(
    run_dir: &Path,
    owner: &str,
    repository: &str,
    git_ref: &str,
) -> PathBuf {
    run_dir
        .join("_action-resolutions")
        .join(owner)
        .join(repository)
        .join(URL_SAFE_NO_PAD.encode(git_ref))
}

fn remote_repository_cache_path(run_dir: &Path, owner: &str, repository: &str) -> Result<PathBuf> {
    Ok(run_dir
        .parent()
        .context("run directory must have a work-root parent")?
        .join("_action-cache")
        .join(owner)
        .join(format!("{repository}.git")))
}

fn remote_repository_lock(cache: &Path) -> Arc<Mutex<()>> {
    static LOCKS: OnceLock<StdMutex<BTreeMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();
    LOCKS
        .get_or_init(|| StdMutex::new(BTreeMap::new()))
        .lock()
        .expect("remote repository lock registry was poisoned")
        .entry(cache.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

#[allow(clippy::too_many_arguments)]
async fn ensure_remote_repository_cache(
    job_id: Uuid,
    step_id: &str,
    cache: &Path,
    remote: &str,
    git_ref: &str,
    object_format_remote: &str,
    checkout_token: Option<&str>,
    checkout_environment: Option<&BTreeMap<String, String>>,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
) -> Result<()> {
    if remote_repository_cache_is_valid(cache, remote).await? {
        return Ok(());
    }
    let object_format = remote_repository_object_format(
        git_ref,
        object_format_remote,
        checkout_token,
        checkout_environment,
        cancel,
    )
    .await?;
    prepare_directory(cache).await?;
    let mut initialize = Command::new("git");
    initialize.args(["init", "--bare", "--quiet"]);
    object_format.configure_git_init(&mut initialize);
    initialize.current_dir(cache);
    run_process(
        job_id,
        step_id,
        &mut initialize,
        None,
        cancel.clone(),
        outbound.clone(),
        sequence.clone(),
    )
    .await
    .map_err(anyhow::Error::from)
    .context("initialize shared remote repository cache")?;
    run_process(
        job_id,
        step_id,
        Command::new("git")
            .args(["remote", "add", "origin", remote])
            .current_dir(cache),
        None,
        cancel.clone(),
        outbound.clone(),
        sequence.clone(),
    )
    .await
    .map_err(anyhow::Error::from)
    .context("configure shared remote repository cache")?;
    Ok(())
}

async fn remote_repository_cache_is_valid(cache: &Path, remote: &str) -> Result<bool> {
    if !tokio::fs::try_exists(cache).await? {
        return Ok(false);
    }
    let bare = Command::new("git")
        .arg("--git-dir")
        .arg(cache)
        .args(["rev-parse", "--is-bare-repository"])
        .output()
        .await
        .context("inspect shared remote repository cache")?;
    if !bare.status.success() || String::from_utf8_lossy(&bare.stdout).trim() != "true" {
        return Ok(false);
    }
    let configured_remote = Command::new("git")
        .arg("--git-dir")
        .arg(cache)
        .args(["config", "--get", "remote.origin.url"])
        .output()
        .await
        .context("inspect shared remote repository cache remote")?;
    if !configured_remote.status.success()
        || String::from_utf8_lossy(&configured_remote.stdout).trim() != remote
    {
        return Ok(false);
    }
    let has_refs = Command::new("git")
        .arg("--git-dir")
        .arg(cache)
        .args(["show-ref", "--quiet"])
        .status()
        .await
        .context("inspect shared remote repository cache refs")?;
    Ok(has_refs.success())
}

async fn read_cached_remote_resolution(cache: &Path, resolution: &Path) -> Result<Option<String>> {
    let commit = match tokio::fs::read_to_string(resolution).await {
        Ok(commit) => commit.trim().to_owned(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("read cached remote resolution"),
    };
    if validate_remote_commit(&commit).is_err() {
        return Ok(None);
    }
    let exists = Command::new("git")
        .arg("--git-dir")
        .arg(cache)
        .args(["cat-file", "-e", &format!("{commit}^{{commit}}")])
        .status()
        .await
        .context("inspect cached remote commit")?;
    Ok(exists.success().then_some(commit))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GitObjectFormat {
    Sha1,
    Sha256,
}

impl GitObjectFormat {
    fn from_object_id(object_id: &str) -> Result<Self> {
        if !object_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            bail!("Git object ID must contain only hexadecimal characters");
        }
        match object_id.len() {
            40 => Ok(Self::Sha1),
            64 => Ok(Self::Sha256),
            _ => bail!("Git object ID must contain exactly 40 or 64 hexadecimal characters"),
        }
    }

    fn configure_git_init(self, command: &mut Command) {
        if self == Self::Sha256 {
            command.arg("--object-format=sha256");
        }
    }
}

fn configure_git_init_object_format(command: &mut Command, object_id: &str) -> Result<()> {
    GitObjectFormat::from_object_id(object_id)?.configure_git_init(command);
    Ok(())
}

fn configure_remote_repository_access(
    command: &mut Command,
    checkout_token: Option<&str>,
    checkout_environment: Option<&BTreeMap<String, String>>,
) {
    command.env("GIT_TERMINAL_PROMPT", "0");
    if let Some(checkout_environment) = checkout_environment {
        command.envs(checkout_environment.iter());
    }
    if let Some(checkout_token) = checkout_token {
        let credential = STANDARD.encode(format!("x-access-token:{checkout_token}"));
        command
            .env("GIT_CONFIG_COUNT", "1")
            .env("GIT_CONFIG_KEY_0", "http.https://github.com/.extraheader")
            .env(
                "GIT_CONFIG_VALUE_0",
                format!("AUTHORIZATION: basic {credential}"),
            );
    }
}

async fn remote_repository_object_format(
    git_ref: &str,
    remote: &str,
    checkout_token: Option<&str>,
    checkout_environment: Option<&BTreeMap<String, String>>,
    cancel: &watch::Receiver<bool>,
) -> Result<GitObjectFormat> {
    if let Ok(object_format) = GitObjectFormat::from_object_id(git_ref) {
        return Ok(object_format);
    }
    let mut command = Command::new("git");
    command.args(["ls-remote", "--quiet", remote, "HEAD"]);
    if git_ref.starts_with("refs/") {
        command.arg(git_ref);
    } else {
        command
            .arg(format!("refs/heads/{git_ref}"))
            .arg(format!("refs/tags/{git_ref}"));
    }
    configure_remote_repository_access(&mut command, checkout_token, checkout_environment);
    let output = run_process_capture_stdout(&mut command, cancel.clone())
        .await
        .with_context(|| format!("detect Git object format for remote ref {git_ref}"))?;
    let object_id = output
        .lines()
        .filter_map(|line| line.split_once('\t').map(|(object_id, _)| object_id))
        .find(|object_id| !object_id.starts_with("ref: "))
        .context("remote did not advertise HEAD or the selected ref")?;
    GitObjectFormat::from_object_id(object_id)
        .context("remote advertised an unsupported Git object format")
}

fn validate_remote_commit(commit: &str) -> Result<()> {
    GitObjectFormat::from_object_id(commit).map(|_| ())
}

async fn git_output(command: &mut Command, operation: &str) -> Result<String> {
    let output = command
        .output()
        .await
        .with_context(|| operation.to_owned())?;
    if !output.status.success() {
        bail!(
            "{operation} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    String::from_utf8(output.stdout)
        .context("Git output was not UTF-8")
        .map(|output| output.trim().to_owned())
}

#[allow(clippy::too_many_arguments)]
async fn execute_node_action_phase(
    job_id: Uuid,
    step_id: &str,
    phase: &str,
    action_directory: &Path,
    script: &str,
    timeout: Option<Duration>,
    workspace: &Path,
    run_dir: &Path,
    environment: &mut BTreeMap<String, String>,
    action_environment: &BTreeMap<String, String>,
    state: &BTreeMap<String, String>,
    cancel: &watch::Receiver<bool>,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
    job_timed_out: &AtomicBool,
    job_summary: &mut JobSummaryBuilder,
    summary_name: &str,
    workflow_commands: &Arc<StdMutex<WorkflowCommandProcessor>>,
) -> Result<ActionPhaseExecution> {
    let script = action_directory.join(script);
    ensure_within(action_directory, &script).await?;
    let temp_dir = environment
        .get("RUNNER_TEMP")
        .map(PathBuf::from)
        .unwrap_or_else(|| run_dir.join("_temp").join(sanitize_id(step_id)));
    tokio::fs::create_dir_all(&temp_dir).await?;
    let nonce = Uuid::new_v4();
    let env_file = temp_dir.join(format!("action-{phase}-{nonce}-env.txt"));
    let output_file = temp_dir.join(format!("action-{phase}-{nonce}-output.txt"));
    let path_file = temp_dir.join(format!("action-{phase}-{nonce}-path.txt"));
    let state_file = temp_dir.join(format!("action-{phase}-{nonce}-state.txt"));
    let summary_file = temp_dir.join(format!("action-{phase}-{nonce}-summary.md"));
    for path in [
        &env_file,
        &output_file,
        &path_file,
        &state_file,
        &summary_file,
    ] {
        tokio::fs::write(path, b"").await?;
    }
    send(
        outbound,
        AgentMessage::LogChunk {
            message_id: Uuid::new_v4(),
            job_id,
            step_id: step_id.to_owned(),
            sequence: sequence.fetch_add(1, Ordering::Relaxed),
            stream: LogStream::System,
            data: format!("Running JavaScript action {phase} entrypoint.\n"),
        },
    )
    .await?;
    let mut phase_environment = action_environment.clone();
    for (name, value) in state {
        phase_environment.insert(format!("STATE_{name}"), value.clone());
    }
    let mut command = Command::new("node");
    command
        .arg(&script)
        .current_dir(workspace)
        .envs(environment.iter())
        .envs(phase_environment.iter())
        .env("GITHUB_ENV", &env_file)
        .env("GITHUB_OUTPUT", &output_file)
        .env("GITHUB_PATH", &path_file)
        .env("GITHUB_STATE", &state_file)
        .env("GITHUB_STEP_SUMMARY", &summary_file);
    let result = run_workflow_process(
        job_id,
        step_id,
        &mut command,
        timeout,
        cancel.clone(),
        outbound.clone(),
        sequence.clone(),
        workflow_commands.clone(),
    )
    .await;
    capture_step_summary(
        job_summary,
        &summary_file,
        summary_name,
        job_id,
        step_id,
        outbound,
        sequence,
    )
    .await?;
    apply_environment_file(environment, &env_file).await?;
    apply_path_file(environment, &path_file).await?;
    let mut outputs = workflow_commands
        .lock()
        .expect("workflow command processor was poisoned")
        .take_legacy_outputs(step_id)?;
    outputs.extend(parse_command_file(&output_file).await?);
    let outputs = filter_masked_outputs(
        outputs,
        workflow_commands,
        job_id,
        step_id,
        outbound,
        sequence,
    )
    .await?;
    let mut state = workflow_commands
        .lock()
        .expect("workflow command processor was poisoned")
        .take_legacy_state(step_id)?;
    state.extend(parse_command_file(&state_file).await?);
    let timed_out = job_timed_out.load(Ordering::Acquire);
    let (_, exit_code) = process_conclusion(&result, timed_out);
    let conclusion = match result {
        Ok(_) => JobConclusion::Success,
        Err(ProcessError::Cancelled) if timed_out => JobConclusion::TimedOut,
        Err(ProcessError::Cancelled) => JobConclusion::Cancelled,
        Err(ProcessError::Exit(_) | ProcessError::Io(_) | ProcessError::TimedOut) => {
            JobConclusion::Failure
        }
    };
    Ok(ActionPhaseExecution {
        conclusion,
        outputs,
        state,
        exit_code,
    })
}

fn process_conclusion(
    result: &Result<i32, ProcessError>,
    job_timed_out: bool,
) -> (Conclusion, Option<i32>) {
    match result {
        Ok(code) => (Conclusion::Success, Some(*code)),
        Err(ProcessError::Cancelled) if job_timed_out => (Conclusion::TimedOut, None),
        Err(ProcessError::Cancelled) => (Conclusion::Cancelled, None),
        Err(ProcessError::TimedOut) => (Conclusion::TimedOut, None),
        Err(ProcessError::Exit(code)) => (Conclusion::Failure, Some(*code)),
        Err(ProcessError::Io(_)) => (Conclusion::Failure, None),
    }
}

fn protocol_conclusion(conclusion: JobConclusion) -> Conclusion {
    match conclusion {
        JobConclusion::Success => Conclusion::Success,
        JobConclusion::Failure => Conclusion::Failure,
        JobConclusion::TimedOut => Conclusion::TimedOut,
        JobConclusion::Cancelled => Conclusion::Cancelled,
        JobConclusion::Skipped => Conclusion::Neutral,
    }
}

fn aggregate_conclusions(conclusions: &[JobConclusion]) -> JobConclusion {
    if conclusions.contains(&JobConclusion::Failure) {
        JobConclusion::Failure
    } else if conclusions.contains(&JobConclusion::TimedOut) {
        JobConclusion::TimedOut
    } else if conclusions.contains(&JobConclusion::Cancelled) {
        JobConclusion::Cancelled
    } else if conclusions
        .iter()
        .all(|conclusion| *conclusion == JobConclusion::Skipped)
    {
        JobConclusion::Skipped
    } else {
        JobConclusion::Success
    }
}

fn dependency_status(
    needs: &[String],
    completed: &BTreeMap<String, JobConclusion>,
) -> ExecutionStatus {
    if needs
        .iter()
        .any(|dependency| completed[dependency] == JobConclusion::Cancelled)
    {
        ExecutionStatus::Cancelled
    } else if needs.iter().any(|dependency| {
        matches!(
            completed[dependency],
            JobConclusion::Failure | JobConclusion::TimedOut
        )
    }) {
        ExecutionStatus::Failure
    } else if needs
        .iter()
        .any(|dependency| completed[dependency] == JobConclusion::Skipped)
    {
        ExecutionStatus::Skipped
    } else {
        ExecutionStatus::Success
    }
}

fn execution_status_result(status: ExecutionStatus) -> &'static str {
    match status {
        ExecutionStatus::Success => "success",
        ExecutionStatus::Failure => "failure",
        ExecutionStatus::Cancelled => "cancelled",
        ExecutionStatus::Skipped => "skipped",
    }
}

fn condition_allows(
    condition: Option<&str>,
    status: ExecutionStatus,
    context: &EvaluationContext,
) -> Result<bool> {
    let Some(condition) = condition else {
        return Ok(status == ExecutionStatus::Success);
    };
    if status != ExecutionStatus::Success && !contains_status_function(condition) {
        return Ok(false);
    }
    context
        .evaluate_condition(condition)
        .with_context(|| format!("evaluate condition '{condition}'"))
}

fn resolve_concurrency(
    concurrency: &PlannedConcurrency,
    context: &EvaluationContext,
    unit: &str,
) -> Result<(String, bool, ConcurrencyQueue)> {
    let group = context
        .render(&concurrency.group)
        .with_context(|| format!("render concurrency group for '{unit}'"))?;
    let group = group.trim().to_owned();
    if group.is_empty() || group.len() > 256 || group.contains(['\0', '\n', '\r']) {
        bail!("concurrency group for '{unit}' resolved to an invalid value");
    }
    let cancel_in_progress = context
        .evaluate_condition(&concurrency.cancel_in_progress)
        .with_context(|| format!("evaluate concurrency cancel-in-progress for '{unit}'"))?;
    let queue = match concurrency.queue {
        PlannedConcurrencyQueue::Single => ConcurrencyQueue::Single,
        PlannedConcurrencyQueue::Max => ConcurrencyQueue::Max,
    };
    if queue == ConcurrencyQueue::Max && cancel_in_progress {
        bail!("concurrency for '{unit}' cannot resolve queue: max with cancel-in-progress: true");
    }
    Ok((group, cancel_in_progress, queue))
}

fn contains_status_function(condition: &str) -> bool {
    let compact = condition
        .chars()
        .filter(|character| !character.is_whitespace())
        .flat_map(char::to_lowercase)
        .collect::<String>();
    ["success(", "failure(", "cancelled(", "always("]
        .iter()
        .any(|function| compact.contains(function))
}

#[allow(clippy::too_many_arguments)]
fn expression_context(
    run: &RunSpec,
    job: &PlannedJob,
    completed_bases: &BTreeMap<String, JobConclusion>,
    completed_outputs: &BTreeMap<String, BTreeMap<String, String>>,
    environment: &BTreeMap<String, String>,
    steps: &JsonValue,
    inputs: &JsonValue,
    status: ExecutionStatus,
    workspace: &Path,
    run_dir: &Path,
    github_token: Option<&str>,
) -> Result<EvaluationContext> {
    expression_context_with_variables(
        run,
        &run.variables,
        job,
        completed_bases,
        completed_outputs,
        environment,
        steps,
        inputs,
        status,
        workspace,
        run_dir,
        github_token,
        &BTreeMap::new(),
    )
}

#[allow(clippy::too_many_arguments)]
fn expression_context_with_variables(
    run: &RunSpec,
    variables: &BTreeMap<String, String>,
    job: &PlannedJob,
    completed_bases: &BTreeMap<String, JobConclusion>,
    completed_outputs: &BTreeMap<String, BTreeMap<String, String>>,
    environment: &BTreeMap<String, String>,
    steps: &JsonValue,
    inputs: &JsonValue,
    status: ExecutionStatus,
    workspace: &Path,
    run_dir: &Path,
    github_token: Option<&str>,
    managed_secrets: &BTreeMap<String, String>,
) -> Result<EvaluationContext> {
    let repository = format!("{}/{}", run.repository.owner, run.repository.name);
    let github_ref = run.pull_request.execution_ref.clone();
    let github_ref_name = execution_ref_name(run);
    let event = github_event(run);
    let actor = event_scalar(&event, "/sender/login");
    let actor_id = event_scalar(&event, "/sender/id");
    let repository_id = event_scalar(&event, "/repository/id");
    let repository_owner_id = event_scalar(&event, "/repository/owner/id");
    let run_id = synthetic_run_id(run);
    let run_number = effective_run_number(run).to_string();
    let needs = job
        .need_aliases
        .iter()
        .map(|(alias, id)| {
            let conclusion = completed_bases[id];
            let outputs = &completed_outputs[id];
            (
                alias.clone(),
                json!({"result": conclusion.as_github_result(), "outputs": outputs}),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let mut context = EvaluationContext::new()
        .with_status(status)
        .with_workspace(workspace);
    context.insert_json(
        "github",
        json!({
            "action": environment.get("GITHUB_ACTION").cloned().unwrap_or_default(),
            "actor": actor.clone(),
            "actor_id": actor_id,
            "api_url": "https://api.github.com",
            "event": event,
            "event_name": "pull_request",
            "event_path": run_dir.join("event.json").display().to_string(),
            "graphql_url": "https://api.github.com/graphql",
            "sha": run.pull_request.merge_sha,
            "ref": github_ref,
            "ref_name": github_ref_name,
            "ref_protected": false,
            "ref_type": "branch",
            "head_ref": run.pull_request.head_ref,
            "base_ref": run.pull_request.base_ref,
            "repository": repository,
            "repository_id": repository_id,
            "repository_owner": run.repository.owner,
            "repository_owner_id": repository_owner_id,
            "repositoryUrl": run.repository.clone_url,
            "retention_days": "30",
            "run_id": run_id,
            "run_number": run_number,
            "run_attempt": "1",
            "server_url": "https://github.com",
            "triggering_actor": actor,
            "workflow": environment.get("GITHUB_WORKFLOW").cloned().unwrap_or_default(),
            "workflow_ref": environment.get("GITHUB_WORKFLOW_REF").cloned().unwrap_or_default(),
            "workflow_sha": environment.get("GITHUB_WORKFLOW_SHA").cloned().unwrap_or_else(|| run.pull_request.merge_sha.clone()),
            "workspace": workspace.display().to_string(),
            "job": job.base_id,
            "token": github_token.map_or(JsonValue::Null, |token| JsonValue::String(token.to_owned())),
            "secret_source": if github_token.is_some() || !managed_secrets.is_empty() { "Actions" } else { "None" },
        }),
    )?;
    let mut base_secrets = managed_secrets.clone();
    if let Some(github_token) = github_token {
        base_secrets.insert("GITHUB_TOKEN".to_owned(), github_token.to_owned());
    }
    if !base_secrets.is_empty() {
        let secrets = resolve_reusable_secrets(job, &base_secrets)?
            .into_iter()
            .map(|(name, value)| (name, JsonValue::String(value)))
            .collect::<serde_json::Map<_, _>>();
        context.insert_json("secrets", JsonValue::Object(secrets))?;
    }
    context.insert_json("env", serde_json::to_value(environment)?)?;
    context.insert_json("matrix", serde_json::to_value(&job.matrix)?)?;
    let strategy = match (job.strategy_job_index, job.strategy_job_total) {
        (Some(index), Some(total)) => json!({
            "fail-fast": job.matrix_fail_fast,
            "job-index": index,
            "job-total": total,
            "max-parallel": job.matrix_max_parallel.unwrap_or(total),
        }),
        _ => JsonValue::Object(Default::default()),
    };
    context.insert_json("strategy", strategy)?;
    context.insert_json("vars", serde_json::to_value(variables)?)?;
    context.insert_json("needs", JsonValue::Object(needs))?;
    context.insert_json("steps", steps.clone())?;
    context.insert_json("inputs", inputs.clone())?;
    context.insert_json("job", json!({"status": execution_status_result(status)}))?;
    context.insert_json(
        "runner",
        json!({
            "os": "macOS",
            "arch": runner_arch(),
            "name": environment.get("RUNNER_NAME").cloned().unwrap_or_default(),
            "temp": environment.get("RUNNER_TEMP").cloned().unwrap_or_else(|| run_dir.join("_temp").display().to_string()),
            "tool_cache": environment.get("RUNNER_TOOL_CACHE").cloned().unwrap_or_default(),
            "environment": environment.get("RUNNER_ENVIRONMENT").cloned().unwrap_or_default(),
        }),
    )?;
    Ok(context)
}

fn render_environment(
    source: &BTreeMap<String, String>,
    context: &EvaluationContext,
) -> Result<BTreeMap<String, String>> {
    source
        .iter()
        .map(|(key, value)| {
            context
                .render(value)
                .with_context(|| format!("render environment variable '{key}'"))
                .map(|value| (key.clone(), value))
        })
        .collect()
}

#[derive(Debug, PartialEq, Eq)]
struct RunnerSelector {
    labels: Vec<String>,
    group: Option<String>,
}

fn statically_resolvable_runner_requirements(
    plans: &[ExecutionPlan],
    run: &RunSpec,
    repository_dir: &Path,
    run_dir: &Path,
) -> Result<Vec<RunnerRequirement>> {
    fn collect(
        plan: &ExecutionPlan,
        run: &RunSpec,
        repository_dir: &Path,
        run_dir: &Path,
        reusable_inputs_available: bool,
        requirements: &mut BTreeSet<RunnerRequirement>,
    ) -> Result<()> {
        for job in &plan.jobs {
            let condition_analysis = match job.condition.as_deref() {
                Some(condition) => match analyze_expression(condition) {
                    Ok(analysis) => Some(analysis),
                    Err(_) => continue,
                },
                None => None,
            };
            let runner_analysis = if job.virtual_job.is_none() {
                match yaml_expression_analysis(&job.runs_on) {
                    Ok(analysis) => Some(analysis),
                    Err(_) => continue,
                }
            } else {
                None
            };
            let condition_contexts = if reusable_inputs_available {
                &["github", "inputs", "vars"][..]
            } else {
                &["github", "vars"][..]
            };
            if condition_analysis
                .as_ref()
                .is_some_and(|analysis| !analysis_is_preflight_safe(analysis, condition_contexts))
            {
                continue;
            }
            let mut runner_contexts = vec!["github", "vars"];
            if reusable_inputs_available {
                runner_contexts.push("inputs");
            }
            if job.dynamic_matrix.is_none() {
                runner_contexts.extend(["matrix", "strategy"]);
            }
            if runner_analysis
                .as_ref()
                .is_some_and(|analysis| !analysis_is_preflight_safe(analysis, &runner_contexts))
            {
                continue;
            }

            let needs_inputs = condition_analysis
                .as_ref()
                .into_iter()
                .chain(runner_analysis.as_ref())
                .any(|analysis| analysis.context_roots.contains("inputs"));
            let mut condition_job = job.clone();
            condition_job.matrix.clear();
            let empty_inputs = JsonValue::Object(Default::default());
            let base_context = preflight_expression_context(
                run,
                plan,
                &condition_job,
                repository_dir,
                run_dir,
                &empty_inputs,
            )?;
            let reusable_inputs = if needs_inputs {
                if !reusable_inputs_are_preflight_safe(job) {
                    continue;
                }
                let Ok(inputs) = resolve_reusable_inputs(&condition_job, &base_context) else {
                    continue;
                };
                inputs
            } else {
                empty_inputs
            };
            if let Some(condition) = job.condition.as_deref() {
                let condition_context = preflight_expression_context(
                    run,
                    plan,
                    &condition_job,
                    repository_dir,
                    run_dir,
                    &reusable_inputs,
                )?;
                let Ok(allows) = condition_context.evaluate_condition(condition) else {
                    continue;
                };
                if !allows {
                    continue;
                }
            }

            match &job.virtual_job {
                None => {
                    let context = preflight_expression_context(
                        run,
                        plan,
                        job,
                        repository_dir,
                        run_dir,
                        &reusable_inputs,
                    )?;
                    let Ok(selector) = render_runner_selector(&job.runs_on, &context) else {
                        continue;
                    };
                    insert_runner_requirement(selector, requirements)?;
                }
                Some(PlannedVirtualJob::ReusableDynamicCall(call)) => collect(
                    &call.called_plan,
                    run,
                    repository_dir,
                    run_dir,
                    false,
                    requirements,
                )?,
                _ => {}
            }
        }
        Ok(())
    }

    let mut requirements = BTreeSet::new();
    for plan in plans {
        collect(plan, run, repository_dir, run_dir, true, &mut requirements)?;
    }
    Ok(requirements.into_iter().collect())
}

fn yaml_expression_analysis(value: &YamlValue) -> Result<ExpressionAnalysis> {
    fn merge(target: &mut ExpressionAnalysis, source: ExpressionAnalysis) {
        target.context_roots.extend(source.context_roots);
        target.uses_status_function |= source.uses_status_function;
        target.uses_hash_files |= source.uses_hash_files;
    }

    let mut analysis = ExpressionAnalysis::default();
    match value {
        YamlValue::String(value) => merge(&mut analysis, analyze_template(value)?),
        YamlValue::Sequence(values) => {
            for value in values {
                merge(&mut analysis, yaml_expression_analysis(value)?);
            }
        }
        YamlValue::Mapping(values) => {
            for value in values.values() {
                merge(&mut analysis, yaml_expression_analysis(value)?);
            }
        }
        YamlValue::Tagged(value) => merge(&mut analysis, yaml_expression_analysis(&value.value)?),
        YamlValue::Null | YamlValue::Bool(_) | YamlValue::Number(_) => {}
    }
    Ok(analysis)
}

fn analysis_is_preflight_safe(analysis: &ExpressionAnalysis, allowed_roots: &[&str]) -> bool {
    !analysis.uses_status_function
        && !analysis.uses_hash_files
        && analysis
            .context_roots
            .iter()
            .all(|root| allowed_roots.contains(&root.as_str()))
}

fn reusable_inputs_are_preflight_safe(job: &PlannedJob) -> bool {
    const ALLOWED_ROOTS: &[&str] = &["github", "inputs", "matrix", "strategy", "vars"];
    job.reusable_input_scopes.iter().all(|scope| {
        scope.inputs.values().all(|input| {
            yaml_expression_analysis(&input.value)
                .is_ok_and(|analysis| analysis_is_preflight_safe(&analysis, ALLOWED_ROOTS))
        })
    })
}

fn preflight_expression_context(
    run: &RunSpec,
    plan: &ExecutionPlan,
    job: &PlannedJob,
    repository_dir: &Path,
    run_dir: &Path,
    inputs: &JsonValue,
) -> Result<EvaluationContext> {
    let mut completed_bases = BTreeMap::new();
    let mut completed_outputs = BTreeMap::new();
    for dependency in job.need_aliases.values() {
        completed_bases.insert(dependency.clone(), JobConclusion::Success);
        completed_outputs.insert(dependency.clone(), BTreeMap::new());
    }
    expression_context(
        run,
        job,
        &completed_bases,
        &completed_outputs,
        &github_workflow_environment(run, plan),
        &JsonValue::Object(Default::default()),
        inputs,
        ExecutionStatus::Success,
        repository_dir,
        run_dir,
        None,
    )
}

fn insert_runner_requirement(
    selector: RunnerSelector,
    requirements: &mut BTreeSet<RunnerRequirement>,
) -> Result<()> {
    if selector.group.is_none()
        && selector.labels.len() == 1
        && is_github_hosted_non_macos_label(&selector.labels[0])
    {
        return Ok(());
    }
    if selector.labels.len() > MAX_RUNNER_LABELS {
        bail!("runner selector declares more than {MAX_RUNNER_LABELS} labels");
    }
    for value in selector.labels.iter().chain(selector.group.iter()) {
        if value.len() > MAX_RUNNER_SELECTOR_BYTES || value.contains(['\0', '\n', '\r']) {
            bail!(
                "runner selectors must contain at most {MAX_RUNNER_SELECTOR_BYTES} bytes without line breaks"
            );
        }
    }
    let mut labels = selector
        .labels
        .into_iter()
        .map(|label| label.to_ascii_lowercase())
        .collect::<Vec<_>>();
    labels.sort();
    labels.dedup();
    requirements.insert(RunnerRequirement {
        labels,
        runner_group: selector.group.map(|group| group.to_ascii_lowercase()),
    });
    if requirements.len() > MAX_RUNNER_REQUIREMENTS {
        bail!("workflow resolves to more than {MAX_RUNNER_REQUIREMENTS} distinct runner selectors");
    }
    Ok(())
}

fn runner_satisfies_requirements(
    requirements: &[RunnerRequirement],
    runner: &RunnerTargeting,
) -> bool {
    requirements.iter().all(|requirement| {
        ensure_runner_eligible(
            &RunnerSelector {
                labels: requirement.labels.clone(),
                group: requirement.runner_group.clone(),
            },
            &runner.labels,
            runner.group.as_deref(),
        )
        .is_ok()
    })
}

fn render_runner_selector(
    value: &YamlValue,
    context: &EvaluationContext,
) -> Result<RunnerSelector> {
    fn collect_labels(
        value: &YamlValue,
        context: &EvaluationContext,
        labels: &mut Vec<String>,
    ) -> Result<()> {
        match value {
            YamlValue::String(value) if is_full_expression(value) => {
                let rendered = context.evaluate_json(value)?;
                match rendered {
                    JsonValue::String(value) => labels.push(value),
                    JsonValue::Array(values) => {
                        for value in values {
                            let JsonValue::String(value) = value else {
                                bail!("runs-on expression arrays must contain strings");
                            };
                            labels.push(value);
                        }
                    }
                    _ => bail!("runs-on expression must resolve to a string or string array"),
                }
            }
            YamlValue::String(value) => labels.push(context.render(value)?),
            YamlValue::Sequence(values) => {
                for value in values {
                    collect_labels(value, context, labels)?;
                }
            }
            YamlValue::Tagged(value) => collect_labels(&value.value, context, labels)?,
            _ => bail!("runs-on must resolve to a string or string array"),
        }
        Ok(())
    }

    fn render_group(value: &YamlValue, context: &EvaluationContext) -> Result<String> {
        match value {
            YamlValue::String(value) if is_full_expression(value) => {
                let JsonValue::String(value) = context.evaluate_json(value)? else {
                    bail!("runs-on group expression must resolve to a string");
                };
                Ok(value)
            }
            YamlValue::String(value) => Ok(context.render(value)?),
            YamlValue::Tagged(value) => render_group(&value.value, context),
            _ => bail!("runs-on group must resolve to a string"),
        }
    }

    let mut labels = Vec::new();
    let mut group = None;
    if let YamlValue::Mapping(selector) = value {
        for (key, value) in selector {
            let Some(key) = key.as_str() else {
                bail!("runs-on selector keys must be strings");
            };
            match key {
                "labels" => collect_labels(value, context, &mut labels)?,
                "group" => group = Some(render_group(value, context)?),
                _ => bail!("runs-on selector key '{key}' is not supported"),
            }
        }
    } else {
        collect_labels(value, context, &mut labels)?;
    }

    for label in &mut labels {
        *label = label.trim().to_owned();
        if label.is_empty() {
            bail!("runs-on labels must not be empty");
        }
    }
    group = group
        .map(|group| group.trim().to_owned())
        .filter(|group| !group.is_empty());
    if labels.is_empty() && group.is_none() {
        bail!("runs-on must select at least one label or runner group");
    }
    Ok(RunnerSelector { labels, group })
}

fn ensure_runner_eligible(
    selector: &RunnerSelector,
    runner_labels: &BTreeSet<String>,
    runner_group: Option<&str>,
) -> Result<()> {
    if let Some(required_group) = &selector.group
        && !runner_group.is_some_and(|group| group.eq_ignore_ascii_case(required_group))
    {
        bail!("requires runner group '{required_group}'");
    }

    if selector.group.is_none()
        && selector.labels.len() == 1
        && is_github_hosted_macos_label(&selector.labels[0])
    {
        return Ok(());
    }

    let missing = selector
        .labels
        .iter()
        .filter(|label| !runner_labels.contains(&label.to_ascii_lowercase()))
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!("missing runner labels: {}", missing.join(", "));
    }
    Ok(())
}

fn is_github_hosted_macos_label(label: &str) -> bool {
    let label = label.to_ascii_lowercase();
    label == "macos-latest" || label.starts_with("macos-")
}

fn is_github_hosted_non_macos_label(label: &str) -> bool {
    let label = label.to_ascii_lowercase();
    label == "ubuntu-latest"
        || label.starts_with("ubuntu-")
        || label == "windows-latest"
        || label.starts_with("windows-")
}

fn is_full_expression(value: &str) -> bool {
    let value = value.trim();
    value.starts_with("${{") && value.ends_with("}}")
}

async fn report_skipped(
    job_id: Uuid,
    step_id: &str,
    name: &str,
    reason: &str,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
) -> Result<()> {
    send(
        outbound,
        AgentMessage::StepStarted {
            message_id: Uuid::new_v4(),
            job_id,
            step_id: step_id.to_owned(),
            name: name.to_owned(),
        },
    )
    .await?;
    send(
        outbound,
        AgentMessage::LogChunk {
            message_id: Uuid::new_v4(),
            job_id,
            step_id: step_id.to_owned(),
            sequence: sequence.fetch_add(1, Ordering::Relaxed),
            stream: LogStream::System,
            data: format!("{reason}\n"),
        },
    )
    .await?;
    send(
        outbound,
        AgentMessage::StepFinished {
            message_id: Uuid::new_v4(),
            job_id,
            step_id: step_id.to_owned(),
            conclusion: Conclusion::Neutral,
            exit_code: None,
        },
    )
    .await
}

#[derive(Debug, thiserror::Error)]
enum ProcessError {
    #[error("process cancelled")]
    Cancelled,
    #[error("process timed out")]
    TimedOut,
    #[error("process exited with code {0}")]
    Exit(i32),
    #[error("process I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

async fn run_process(
    job_id: Uuid,
    step_id: &str,
    command: &mut Command,
    timeout: Option<Duration>,
    cancel: watch::Receiver<bool>,
    outbound: mpsc::Sender<AgentMessage>,
    sequence: Arc<AtomicU64>,
) -> Result<i32, ProcessError> {
    run_process_inner(
        job_id, step_id, command, timeout, cancel, outbound, sequence, None,
    )
    .await
}

async fn run_process_capture_stdout(
    command: &mut Command,
    mut cancel: watch::Receiver<bool>,
) -> Result<String> {
    if *cancel.borrow() {
        return Err(ProcessError::Cancelled.into());
    }
    #[cfg(unix)]
    command.process_group(0);
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn()?;
    let stdout = child.stdout.take().expect("stdout configured");
    let stderr = child.stderr.take().expect("stderr configured");
    let stdout_task = tokio::spawn(read_bounded_process_output(stdout));
    let stderr_task = tokio::spawn(read_bounded_process_output(stderr));
    let status = tokio::select! {
        status = child.wait() => status?,
        changed = cancel.changed() => {
            if changed.is_ok() && *cancel.borrow() {
                terminate_process_tree(&mut child).await;
                let _ = stdout_task.await;
                let _ = stderr_task.await;
                return Err(ProcessError::Cancelled.into());
            }
            child.wait().await?
        },
    };
    let (stdout, stdout_truncated) = stdout_task.await.context("join captured Git stdout")??;
    let (_, stderr_truncated) = stderr_task.await.context("join captured Git stderr")??;
    if !status.success() {
        return Err(ProcessError::Exit(status.code().unwrap_or(128)).into());
    }
    if stdout_truncated || stderr_truncated {
        bail!("captured Git output exceeded its safe bound");
    }
    String::from_utf8(stdout).context("captured Git output was not UTF-8")
}

async fn read_bounded_process_output<R>(mut reader: R) -> std::io::Result<(Vec<u8>, bool)>
where
    R: AsyncRead + Unpin,
{
    let mut captured = Vec::new();
    let mut truncated = false;
    let mut buffer = [0_u8; 8 * 1_024];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            return Ok((captured, truncated));
        }
        let remaining = MAX_GIT_CAPTURE_BYTES.saturating_sub(captured.len());
        let take = read.min(remaining);
        captured.extend_from_slice(&buffer[..take]);
        truncated |= take < read;
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_workflow_process(
    job_id: Uuid,
    step_id: &str,
    command: &mut Command,
    timeout: Option<Duration>,
    cancel: watch::Receiver<bool>,
    outbound: mpsc::Sender<AgentMessage>,
    sequence: Arc<AtomicU64>,
    workflow_commands: Arc<StdMutex<WorkflowCommandProcessor>>,
) -> Result<i32, ProcessError> {
    run_process_inner(
        job_id,
        step_id,
        command,
        timeout,
        cancel,
        outbound,
        sequence,
        Some(workflow_commands),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_process_inner(
    job_id: Uuid,
    step_id: &str,
    command: &mut Command,
    timeout: Option<Duration>,
    mut cancel: watch::Receiver<bool>,
    outbound: mpsc::Sender<AgentMessage>,
    sequence: Arc<AtomicU64>,
    workflow_commands: Option<Arc<StdMutex<WorkflowCommandProcessor>>>,
) -> Result<i32, ProcessError> {
    if *cancel.borrow() {
        return Err(ProcessError::Cancelled);
    }
    #[cfg(unix)]
    command.process_group(0);
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn()?;
    let stdout = child.stdout.take().expect("stdout configured");
    let stderr = child.stderr.take().expect("stderr configured");
    let stdout_task = tokio::spawn(forward_output(
        BufReader::new(stdout),
        job_id,
        step_id.to_owned(),
        LogStream::Stdout,
        outbound.clone(),
        sequence.clone(),
        workflow_commands.clone(),
    ));
    let stderr_task = tokio::spawn(forward_output(
        BufReader::new(stderr),
        job_id,
        step_id.to_owned(),
        LogStream::Stderr,
        outbound,
        sequence,
        workflow_commands,
    ));

    let timeout_enabled = timeout.is_some();
    let timeout = tokio::time::sleep(timeout.unwrap_or_default());
    tokio::pin!(timeout);
    let status = tokio::select! {
        status = child.wait() => status?,
        changed = cancel.changed() => {
            if changed.is_ok() && *cancel.borrow() {
                terminate_process_tree(&mut child).await;
                let _ = stdout_task.await;
                let _ = stderr_task.await;
                return Err(ProcessError::Cancelled);
            }
            child.wait().await?
        },
        _ = &mut timeout, if timeout_enabled => {
            terminate_process_tree(&mut child).await;
            let _ = stdout_task.await;
            let _ = stderr_task.await;
            return Err(ProcessError::TimedOut);
        }
    };
    stdout_task.await.ok();
    stderr_task.await.ok();
    match status.code() {
        Some(0) => Ok(0),
        Some(code) => Err(ProcessError::Exit(code)),
        None => Err(ProcessError::Exit(128)),
    }
}

async fn terminate_process_tree(child: &mut tokio::process::Child) {
    #[cfg(unix)]
    if let Some(process_id) = child.id() {
        let _ = signal_process_group(process_id, libc::SIGTERM);
        tokio::time::sleep(Duration::from_millis(250)).await;
        let _ = signal_process_group(process_id, libc::SIGKILL);
        let _ = child.wait().await;
        return;
    }

    let _ = child.kill().await;
    let _ = child.wait().await;
}

#[cfg(unix)]
fn signal_process_group(process_id: u32, signal: libc::c_int) -> std::io::Result<()> {
    let process_id = i32::try_from(process_id)
        .map_err(|_| std::io::Error::other("child process id exceeds i32"))?;
    // The child is placed into a process group whose ID matches its PID above.
    let result = unsafe { libc::kill(-process_id, signal) };
    if result == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(error)
    }
}

async fn forward_output<R>(
    mut reader: BufReader<R>,
    job_id: Uuid,
    step_id: String,
    stream: LogStream,
    outbound: mpsc::Sender<AgentMessage>,
    sequence: Arc<AtomicU64>,
    workflow_commands: Option<Arc<StdMutex<WorkflowCommandProcessor>>>,
) where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut pending = Vec::with_capacity(MAX_LOG_CHUNK_BYTES);
    loop {
        let available = match reader.fill_buf().await {
            Ok(available) => available,
            Err(_) => return,
        };
        if available.is_empty() {
            if !pending.is_empty() {
                let mut data = String::from_utf8_lossy(&pending).into_owned();
                if !prepare_workflow_log(workflow_commands.as_ref(), &step_id, stream, &mut data) {
                    return;
                }
                if !data.is_empty() {
                    let message_id = Uuid::new_v4();
                    mark_preprocessed_workflow_log(workflow_commands.as_ref(), message_id);
                    if outbound
                        .send(AgentMessage::LogChunk {
                            message_id,
                            job_id,
                            step_id: step_id.clone(),
                            sequence: sequence.fetch_add(1, Ordering::Relaxed),
                            stream,
                            data,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            if let Some(commands) = &workflow_commands {
                let mut commands = commands
                    .lock()
                    .expect("workflow command processor was poisoned");
                if let Err(error) = commands.finish_stream(&step_id, stream) {
                    commands.fail(error.to_string());
                }
            }
            return;
        }

        let mut consumed = 0;
        let mut ready = Vec::new();
        while consumed < available.len() {
            let remaining = &available[consumed..];
            let newline = remaining.iter().position(|byte| *byte == b'\n');
            let through_newline = newline.map_or(remaining.len(), |position| position + 1);
            let take = through_newline.min(MAX_LOG_CHUNK_BYTES - pending.len());
            pending.extend_from_slice(&remaining[..take]);
            consumed += take;
            if pending.len() == MAX_LOG_CHUNK_BYTES
                || (newline.is_some() && take == through_newline)
            {
                ready.push(std::mem::take(&mut pending));
                pending.reserve(MAX_LOG_CHUNK_BYTES);
            }
        }
        reader.consume(consumed);

        for data in ready {
            let mut data = String::from_utf8_lossy(&data).into_owned();
            if !prepare_workflow_log(workflow_commands.as_ref(), &step_id, stream, &mut data) {
                return;
            }
            if data.is_empty() {
                continue;
            }
            let message_id = Uuid::new_v4();
            mark_preprocessed_workflow_log(workflow_commands.as_ref(), message_id);
            if outbound
                .send(AgentMessage::LogChunk {
                    message_id,
                    job_id,
                    step_id: step_id.clone(),
                    sequence: sequence.fetch_add(1, Ordering::Relaxed),
                    stream,
                    data,
                })
                .await
                .is_err()
            {
                return;
            }
        }
    }
}

fn prepare_workflow_log(
    commands: Option<&Arc<StdMutex<WorkflowCommandProcessor>>>,
    step_id: &str,
    stream: LogStream,
    data: &mut String,
) -> bool {
    let Some(commands) = commands else {
        return true;
    };
    let mut commands = commands
        .lock()
        .expect("workflow command processor was poisoned");
    if let Err(error) = commands.process_log(step_id, stream, data) {
        commands.fail(error.to_string());
        data.clear();
        return false;
    }
    commands.mask_for_step(step_id, data);
    true
}

fn mark_preprocessed_workflow_log(
    commands: Option<&Arc<StdMutex<WorkflowCommandProcessor>>>,
    message_id: Uuid,
) {
    if let Some(commands) = commands {
        commands
            .lock()
            .expect("workflow command processor was poisoned")
            .mark_preprocessed_log(message_id);
    }
}

async fn github_environment(
    run: &RunSpec,
    repository_dir: &Path,
    run_dir: &Path,
    tool_cache: &Path,
    runner_name: &str,
) -> Result<BTreeMap<String, String>> {
    let event_path = run_dir.join("event.json");
    let event = github_event(run);
    let actor = event_scalar(&event, "/sender/login");
    let actor_id = event_scalar(&event, "/sender/id");
    let repository_id = event_scalar(&event, "/repository/id");
    let repository_owner_id = event_scalar(&event, "/repository/owner/id");
    tokio::fs::write(&event_path, serde_json::to_vec(&event)?).await?;

    Ok(BTreeMap::from([
        ("CI".to_owned(), "true".to_owned()),
        ("GITHUB_ACTIONS".to_owned(), "true".to_owned()),
        ("GITHUB_ACTOR".to_owned(), actor.clone()),
        ("GITHUB_ACTOR_ID".to_owned(), actor_id),
        (
            "GITHUB_API_URL".to_owned(),
            "https://api.github.com".to_owned(),
        ),
        ("GITHUB_EVENT_NAME".to_owned(), "pull_request".to_owned()),
        (
            "GITHUB_EVENT_PATH".to_owned(),
            event_path.display().to_string(),
        ),
        ("GITHUB_SHA".to_owned(), run.pull_request.merge_sha.clone()),
        (
            "GITHUB_REF".to_owned(),
            run.pull_request.execution_ref.clone(),
        ),
        ("GITHUB_REF_NAME".to_owned(), execution_ref_name(run)),
        ("GITHUB_REF_PROTECTED".to_owned(), "false".to_owned()),
        ("GITHUB_REF_TYPE".to_owned(), "branch".to_owned()),
        (
            "GITHUB_HEAD_REF".to_owned(),
            run.pull_request.head_ref.clone(),
        ),
        (
            "GITHUB_BASE_REF".to_owned(),
            run.pull_request.base_ref.clone(),
        ),
        (
            "GITHUB_REPOSITORY".to_owned(),
            format!("{}/{}", run.repository.owner, run.repository.name),
        ),
        ("GITHUB_REPOSITORY_ID".to_owned(), repository_id),
        (
            "GITHUB_REPOSITORY_OWNER".to_owned(),
            run.repository.owner.clone(),
        ),
        ("GITHUB_REPOSITORY_OWNER_ID".to_owned(), repository_owner_id),
        ("GITHUB_RETENTION_DAYS".to_owned(), "30".to_owned()),
        ("GITHUB_RUN_ATTEMPT".to_owned(), "1".to_owned()),
        ("GITHUB_RUN_ID".to_owned(), synthetic_run_id(run)),
        (
            "GITHUB_RUN_NUMBER".to_owned(),
            effective_run_number(run).to_string(),
        ),
        (
            "GITHUB_SERVER_URL".to_owned(),
            "https://github.com".to_owned(),
        ),
        (
            "GITHUB_GRAPHQL_URL".to_owned(),
            "https://api.github.com/graphql".to_owned(),
        ),
        ("GITHUB_TRIGGERING_ACTOR".to_owned(), actor),
        (
            "GITHUB_WORKSPACE".to_owned(),
            repository_dir.display().to_string(),
        ),
        ("RUNNER_OS".to_owned(), "macOS".to_owned()),
        ("RUNNER_ARCH".to_owned(), runner_arch().to_owned()),
        ("RUNNER_NAME".to_owned(), runner_name.to_owned()),
        ("RUNNER_ENVIRONMENT".to_owned(), "self-hosted".to_owned()),
        (
            "RUNNER_TEMP".to_owned(),
            run_dir.join("_temp").display().to_string(),
        ),
        (
            "RUNNER_TOOL_CACHE".to_owned(),
            tool_cache.display().to_string(),
        ),
        (
            "AGENT_TOOLSDIRECTORY".to_owned(),
            tool_cache.display().to_string(),
        ),
    ]))
}

fn github_workflow_environment(run: &RunSpec, plan: &ExecutionPlan) -> BTreeMap<String, String> {
    let repository = format!("{}/{}", run.repository.owner, run.repository.name);
    BTreeMap::from([
        ("GITHUB_WORKFLOW".to_owned(), plan.workflow_name.clone()),
        (
            "GITHUB_WORKFLOW_REF".to_owned(),
            format!(
                "{repository}/{}@{}",
                plan.workflow_path, run.pull_request.execution_ref
            ),
        ),
        (
            "GITHUB_WORKFLOW_SHA".to_owned(),
            run.pull_request.merge_sha.clone(),
        ),
    ])
}

fn github_event(run: &RunSpec) -> JsonValue {
    if run.event.as_object().is_some_and(|event| !event.is_empty()) {
        return run.event.clone();
    }
    json!({
        "action": run.pull_request.action,
        "number": run.pull_request.number,
        "installation": {"id": run.installation_id},
        "pull_request": {
            "number": run.pull_request.number,
            "merged": run.pull_request.action == "closed"
                && run.pull_request.execution_ref.starts_with("refs/heads/"),
            "merge_commit_sha": run.pull_request.merge_sha,
            "head": {"sha": run.pull_request.head_sha, "ref": run.pull_request.head_ref},
            "base": {"sha": run.pull_request.base_sha, "ref": run.pull_request.base_ref}
        },
        "repository": {
            "name": run.repository.name,
            "full_name": format!("{}/{}", run.repository.owner, run.repository.name),
            "clone_url": run.repository.clone_url,
            "owner": {"login": run.repository.owner}
        }
    })
}

fn event_scalar(event: &JsonValue, pointer: &str) -> String {
    match event.pointer(pointer) {
        Some(JsonValue::String(value)) => value.clone(),
        Some(JsonValue::Number(value)) => value.to_string(),
        Some(JsonValue::Bool(value)) => value.to_string(),
        _ => String::new(),
    }
}

fn synthetic_run_id(run: &RunSpec) -> String {
    run.id.as_u128().to_string()
}

fn effective_run_number(run: &RunSpec) -> u64 {
    if run.run_number == 0 {
        run.pull_request.number
    } else {
        run.run_number
    }
}

#[allow(clippy::too_many_arguments)]
async fn capture_step_summary(
    summary: &mut JobSummaryBuilder,
    path: &Path,
    name: &str,
    job_id: Uuid,
    step_id: &str,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
) -> Result<()> {
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            report_summary_failure(
                job_id,
                step_id,
                &format!("Step summary could not be read and was ignored: {error}"),
                outbound,
                sequence,
            )
            .await?;
            return Ok(());
        }
    };
    if bytes.is_empty() {
        return Ok(());
    }
    if bytes.len() > MAX_STEP_SUMMARY_BYTES {
        report_summary_failure(
            job_id,
            step_id,
            "Step summary exceeded GitHub's 1 MiB per-step limit and was ignored.",
            outbound,
            sequence,
        )
        .await?;
        return Ok(());
    }
    let markdown = match String::from_utf8(bytes) {
        Ok(markdown) => markdown,
        Err(_) => {
            report_summary_failure(
                job_id,
                step_id,
                "Step summary was not valid UTF-8 and was ignored.",
                outbound,
                sequence,
            )
            .await?;
            return Ok(());
        }
    };
    if !markdown.trim().is_empty() {
        summary.push(name.to_owned(), markdown);
    }
    Ok(())
}

async fn report_summary_failure(
    job_id: Uuid,
    step_id: &str,
    message: &str,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
) -> Result<()> {
    send(
        outbound,
        AgentMessage::LogChunk {
            message_id: Uuid::new_v4(),
            job_id,
            step_id: step_id.to_owned(),
            sequence: sequence.fetch_add(1, Ordering::Relaxed),
            stream: LogStream::System,
            data: format!("{message}\n"),
        },
    )
    .await
}

async fn apply_environment_file(
    environment: &mut BTreeMap<String, String>,
    path: &Path,
) -> Result<()> {
    for (name, value) in parse_command_file(path).await? {
        if !is_default_environment_name(&name) && !name.eq_ignore_ascii_case("NODE_OPTIONS") {
            environment.insert(name, value);
        }
    }
    Ok(())
}

fn restore_default_environment(
    environment: &mut BTreeMap<String, String>,
    defaults: &BTreeMap<String, String>,
) {
    environment.extend(
        defaults
            .iter()
            .filter(|(name, _)| is_default_environment_name(name))
            .map(|(name, value)| (name.clone(), value.clone())),
    );
}

fn is_default_environment_name(name: &str) -> bool {
    name.starts_with("GITHUB_") || name.starts_with("RUNNER_")
}

async fn parse_command_file(path: &Path) -> Result<BTreeMap<String, String>> {
    let source = tokio::fs::read_to_string(path).await?;
    parse_command_source(&source)
}

async fn filter_masked_outputs(
    mut outputs: BTreeMap<String, String>,
    workflow_commands: &Arc<StdMutex<WorkflowCommandProcessor>>,
    job_id: Uuid,
    step_id: &str,
    outbound: &mpsc::Sender<AgentMessage>,
    sequence: &Arc<AtomicU64>,
) -> Result<BTreeMap<String, String>> {
    let masked = {
        let commands = workflow_commands
            .lock()
            .expect("workflow command processor was poisoned");
        commands.ensure_healthy()?;
        outputs
            .iter()
            .filter(|(_, value)| commands.value_is_masked_for_step(step_id, value))
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>()
    };
    for name in masked {
        outputs.insert(name.clone(), String::new());
        send(
            outbound,
            AgentMessage::LogChunk {
                message_id: Uuid::new_v4(),
                job_id,
                step_id: step_id.to_owned(),
                sequence: sequence.fetch_add(1, Ordering::Relaxed),
                stream: LogStream::System,
                data: format!("Skip output '{name}' because it may contain a masked value.\n"),
            },
        )
        .await?;
    }
    Ok(outputs)
}

fn parse_command_source(source: &str) -> Result<BTreeMap<String, String>> {
    let lines = source.lines().collect::<Vec<_>>();
    let mut values = BTreeMap::new();
    let mut index = 0;
    while index < lines.len() {
        let line = lines[index].trim_end_matches('\r');
        index += 1;
        if line.is_empty() {
            continue;
        }

        let equals_index = line.find('=');
        let heredoc_index = line.find("<<");
        let (key, value) = if heredoc_index.is_some()
            && equals_index
                .is_none_or(|equals| heredoc_index.is_some_and(|heredoc| heredoc < equals))
        {
            let (key, delimiter) = line
                .split_once("<<")
                .expect("heredoc marker was located in command-file line");
            if delimiter.is_empty() {
                bail!("invalid command-file entry: multiline delimiter cannot be empty");
            }
            let start = index;
            while index < lines.len() && lines[index].trim_end_matches('\r') != delimiter {
                index += 1;
            }
            if index == lines.len() {
                bail!("invalid command-file entry: missing multiline delimiter '{delimiter}'");
            }
            let value = lines[start..index]
                .iter()
                .map(|line| line.trim_end_matches('\r'))
                .collect::<Vec<_>>()
                .join("\n");
            index += 1;
            (key, value)
        } else if equals_index.is_some() {
            let Some((key, value)) = line.split_once('=') else {
                unreachable!("equals marker was located in command-file line");
            };
            (key, value.to_owned())
        } else {
            bail!("invalid command-file entry: expected NAME=VALUE or NAME<<DELIMITER");
        };
        if key.is_empty() || key.contains(['\0', '\n', '\r', '=']) {
            bail!("invalid command-file variable name");
        }
        values.insert(key.to_owned(), value);
    }
    Ok(values)
}

async fn apply_path_file(environment: &mut BTreeMap<String, String>, path: &Path) -> Result<()> {
    let source = tokio::fs::read_to_string(path).await?;
    let existing = environment
        .get("PATH")
        .cloned()
        .or_else(|| std::env::var("PATH").ok())
        .unwrap_or_default();
    let mut entries = if existing.is_empty() {
        Vec::new()
    } else {
        existing.split(':').map(str::to_owned).collect()
    };
    let mut changed = false;
    for addition in source.lines().filter(|line| !line.is_empty()) {
        entries.retain(|entry| entry != addition);
        entries.insert(0, addition.to_owned());
        changed = true;
    }
    if changed {
        environment.insert("PATH".to_owned(), entries.join(":"));
    }
    Ok(())
}

fn shell_program(shell: &str) -> Result<String> {
    let parts = shell_words::split(shell).context("parse shell command")?;
    match parts.first().map(String::as_str) {
        Some("bash") => Ok("/bin/bash".to_owned()),
        Some("sh") => Ok("/bin/sh".to_owned()),
        Some("zsh") => Ok("/bin/zsh".to_owned()),
        Some(other) if !other.contains(['\0', '\n', '\r']) => Ok(other.to_owned()),
        Some(_) => bail!("shell program contains invalid characters"),
        None => bail!("shell cannot be empty"),
    }
}

fn shell_arguments_for(shell: &str, script_path: &Path) -> Result<Vec<String>> {
    let mut parts = shell_words::split(shell).context("parse shell command")?;
    let program = parts.first().cloned().context("shell cannot be empty")?;
    let script_path = script_path.display().to_string();
    if parts.len() == 1 {
        return match program.as_str() {
            "bash" => Ok(vec![
                "--noprofile".to_owned(),
                "--norc".to_owned(),
                "-eo".to_owned(),
                "pipefail".to_owned(),
                script_path,
            ]),
            "sh" => Ok(vec!["-e".to_owned(), script_path]),
            "python" => Ok(vec![script_path]),
            "pwsh" | "powershell" => Ok(vec![
                "-command".to_owned(),
                format!(". '{}'", script_path.replace('\'', "''")),
            ]),
            "cmd" => Ok(vec![
                "/D".to_owned(),
                "/E:ON".to_owned(),
                "/V:OFF".to_owned(),
                "/S".to_owned(),
                "/C".to_owned(),
                format!("CALL \"{script_path}\""),
            ]),
            _ => bail!("custom shell templates must include a {{0}} placeholder"),
        };
    }
    let mut arguments = parts.drain(1..).collect::<Vec<_>>();
    if !arguments.iter().any(|argument| argument.contains("{0}")) {
        bail!("custom shell templates must include a {{0}} placeholder");
    }
    for argument in &mut arguments {
        *argument = argument.replace("{0}", &script_path);
    }
    Ok(arguments)
}

fn shell_script_extension(shell: &str) -> Result<&'static str> {
    let parts = shell_words::split(shell).context("parse shell command")?;
    let program = parts.first().context("shell cannot be empty")?;
    let program = Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
        .context("shell program must be valid UTF-8")?;
    Ok(match program {
        "pwsh" | "powershell" => "ps1",
        "python" | "python3" => "py",
        "cmd" => "cmd",
        _ => "sh",
    })
}

async fn prepare_directory(path: &Path) -> Result<()> {
    if tokio::fs::try_exists(path).await? {
        tokio::fs::remove_dir_all(path)
            .await
            .with_context(|| format!("clear run directory {}", path.display()))?;
    }
    tokio::fs::create_dir_all(path)
        .await
        .with_context(|| format!("create run directory {}", path.display()))
}

async fn ensure_within(root: &Path, candidate: &Path) -> Result<()> {
    let root = tokio::fs::canonicalize(root).await?;
    let candidate = tokio::fs::canonicalize(candidate)
        .await
        .with_context(|| format!("working directory {} does not exist", candidate.display()))?;
    if !candidate.starts_with(&root) {
        bail!("working-directory escapes the repository workspace");
    }
    Ok(())
}

fn sanitize_id(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '-'
            }
        })
        .collect()
}

fn actions_runtime_token(
    workflow_run_backend_id: &str,
    workflow_job_run_backend_id: &str,
) -> Result<String> {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none","typ":"JWT"}"#);
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&json!({
        "scp": format!(
            "Actions.Results:{workflow_run_backend_id}:{workflow_job_run_backend_id}"
        ),
    }))?);
    let signature = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    Ok(format!("{header}.{payload}.{signature}"))
}

fn validate_sha(value: &str) -> Result<GitObjectFormat> {
    GitObjectFormat::from_object_id(value).context("invalid pull request snapshot object ID")
}

fn validate_checkout_token_metadata(run: &RunSpec) -> Result<()> {
    if run.checkout_token.is_empty() != run.checkout_token_expires_at_epoch_seconds.is_none() {
        bail!("checkout token and expiry must either both be present or absent");
    }
    Ok(())
}

fn validate_execution_ref(run: &RunSpec) -> Result<()> {
    let merge_ref = format!("refs/pull/{}/merge", run.pull_request.number);
    let base_ref = format!("refs/heads/{}", run.pull_request.base_ref);
    let valid = run.pull_request.execution_ref == merge_ref
        || (run.pull_request.action == "closed" && run.pull_request.execution_ref == base_ref);
    if !valid || !valid_git_ref(&run.pull_request.execution_ref) {
        bail!(
            "pull request execution ref must identify its merge ref or the base ref for a closed event"
        );
    }
    Ok(())
}

fn valid_git_ref(value: &str) -> bool {
    value.starts_with("refs/")
        && value.len() <= 4_096
        && !value.chars().any(|character| {
            character <= ' ' || character == '\u{7f}' || "~^:?*[\\".contains(character)
        })
        && !value.contains("..")
        && !value.contains("@{")
        && !value.contains("//")
        && !value.ends_with('.')
        && !value.ends_with('/')
        && value.split('/').all(|component| {
            !component.is_empty() && !component.starts_with('.') && !component.ends_with(".lock")
        })
}

fn execution_ref_name(run: &RunSpec) -> String {
    run.pull_request
        .execution_ref
        .strip_prefix("refs/heads/")
        .or_else(|| run.pull_request.execution_ref.strip_prefix("refs/pull/"))
        .unwrap_or(&run.pull_request.execution_ref)
        .to_owned()
}

fn runner_arch() -> &'static str {
    match std::env::consts::ARCH {
        "aarch64" => "ARM64",
        "x86_64" => "X64",
        _ => std::env::consts::ARCH,
    }
}

pub(crate) fn default_runner_labels() -> Vec<String> {
    vec![
        "self-hosted".to_owned(),
        "macOS".to_owned(),
        runner_arch().to_owned(),
    ]
}

fn default_runner_targeting() -> RunnerTargeting {
    RunnerTargeting {
        labels: default_runner_labels()
            .into_iter()
            .map(|label| label.to_ascii_lowercase())
            .collect(),
        group: None,
    }
}

async fn send(outbound: &mpsc::Sender<AgentMessage>, message: AgentMessage) -> Result<()> {
    outbound
        .send(message)
        .await
        .context("control plane event channel closed")
}

fn secret_values(run: &RunSpec) -> Vec<String> {
    let mut values = Vec::new();
    if !run.checkout_token.is_empty() {
        values.push(run.checkout_token.clone());
        let credential = STANDARD.encode(format!("x-access-token:{}", run.checkout_token));
        values.push(credential.clone());
        values.push(format!("AUTHORIZATION: basic {credential}"));
    }
    for (name, value) in &run.environment {
        let name = name.to_ascii_uppercase();
        if value.len() >= 8
            && ["TOKEN", "SECRET", "PASSWORD", "PRIVATE_KEY", "API_KEY"]
                .iter()
                .any(|marker| name.contains(marker))
        {
            values.push(value.clone());
        }
    }
    values.sort_by(|left, right| right.len().cmp(&left.len()).then_with(|| left.cmp(right)));
    values.dedup();
    values
}

fn register_repository_token_masks(
    commands: &Arc<StdMutex<WorkflowCommandProcessor>>,
    token: &str,
) {
    let credential = STANDARD.encode(format!("x-access-token:{token}"));
    let mut commands = commands
        .lock()
        .expect("workflow command processor was poisoned");
    commands.register_global_mask(format!("AUTHORIZATION: basic {credential}"));
    commands.register_global_mask(credential);
    commands.register_global_mask(token.to_owned());
}

fn register_managed_secret_masks(
    commands: &Arc<StdMutex<WorkflowCommandProcessor>>,
    secrets: &BTreeMap<String, String>,
) {
    let mut commands = commands
        .lock()
        .expect("workflow command processor was poisoned");
    for value in secrets.values().filter(|value| !value.is_empty()) {
        commands.register_global_mask(value.clone());
    }
}

fn valid_managed_secret_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_uppercase() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        && !name.starts_with("GITHUB_")
}

async fn relay_masked_events(
    mut incoming: mpsc::Receiver<AgentMessage>,
    outbound: mpsc::Sender<AgentMessage>,
    secrets: Vec<String>,
    commands: Arc<StdMutex<WorkflowCommandProcessor>>,
) -> Result<()> {
    while let Some(mut message) = incoming.recv().await {
        {
            let commands = commands
                .lock()
                .expect("workflow command processor was poisoned");
            commands.ensure_healthy()?;
        }
        match &mut message {
            AgentMessage::StepStarted { step_id, name, .. } => {
                mask_text(name, &secrets);
                commands
                    .lock()
                    .expect("workflow command processor was poisoned")
                    .mask_for_step(step_id, name);
            }
            AgentMessage::LogChunk {
                message_id,
                step_id,
                stream,
                data,
                ..
            } => {
                let preprocessed = commands
                    .lock()
                    .expect("workflow command processor was poisoned")
                    .take_preprocessed_log(*message_id);
                if !preprocessed && matches!(stream, LogStream::Stdout | LogStream::Stderr) {
                    commands
                        .lock()
                        .expect("workflow command processor was poisoned")
                        .process_log(step_id, *stream, data)?;
                }
                if data.is_empty() {
                    continue;
                }
                mask_text(data, &secrets);
                commands
                    .lock()
                    .expect("workflow command processor was poisoned")
                    .mask_for_step(step_id, data);
            }
            AgentMessage::StepFinished { step_id, .. } => {
                commands
                    .lock()
                    .expect("workflow command processor was poisoned")
                    .finish_step(step_id)?;
            }
            AgentMessage::JobFinished {
                summary,
                annotations,
                ..
            } => {
                mask_text(summary, &secrets);
                let mut commands = commands
                    .lock()
                    .expect("workflow command processor was poisoned");
                commands.mask_all(summary);
                *annotations = commands.take_annotations();
                for annotation in annotations {
                    mask_text(&mut annotation.path, &secrets);
                    mask_text(&mut annotation.message, &secrets);
                    commands.mask_all(&mut annotation.path);
                    commands.mask_all(&mut annotation.message);
                    if let Some(title) = &mut annotation.title {
                        mask_text(title, &secrets);
                        commands.mask_all(title);
                    }
                    bound_masked_annotation(annotation);
                }
            }
            AgentMessage::JobRejected { reason, .. } => {
                mask_text(reason, &secrets);
                commands
                    .lock()
                    .expect("workflow command processor was poisoned")
                    .mask_all(reason);
            }
            AgentMessage::Hello { .. }
            | AgentMessage::Heartbeat { .. }
            | AgentMessage::ConcurrencyAcquire { .. }
            | AgentMessage::ConcurrencyRelease { .. }
            | AgentMessage::RepositoryTokenRequest { .. }
            | AgentMessage::WorkflowTokenRequest { .. }
            | AgentMessage::SecretRequest { .. }
            | AgentMessage::DeploymentStarted { .. }
            | AgentMessage::DeploymentFinished { .. }
            | AgentMessage::JobStarted { .. } => {}
        }
        outbound
            .send(message)
            .await
            .context("control plane event channel closed")?;
    }
    Ok(())
}

#[derive(Default)]
struct WorkflowCommandProcessor {
    global_masks: Vec<String>,
    scopes: BTreeMap<String, WorkflowCommandScope>,
    legacy_values: BTreeMap<String, LegacyCommandValues>,
    problem_matchers: ProblemMatcherRegistry,
    annotations: Vec<CheckAnnotation>,
    annotation_counts: BTreeMap<(String, CheckAnnotationLevel), usize>,
    preprocessed_logs: BTreeSet<Uuid>,
    partial_commands: BTreeMap<(String, u8), String>,
    fatal_error: Option<String>,
}

enum WorkflowLineDisposition {
    Original,
    Suppress,
    Replace(String),
}

#[derive(Default)]
struct WorkflowCommandScope {
    masks: Vec<String>,
    mask_bytes: usize,
    stop_token: Option<String>,
}

#[derive(Default)]
struct LegacyCommandValues {
    outputs: BTreeMap<String, String>,
    state: BTreeMap<String, String>,
    command_count: usize,
    command_bytes: usize,
}

impl WorkflowCommandProcessor {
    fn with_global_masks(
        mut masks: Vec<String>,
        workspace: PathBuf,
        matcher_root: PathBuf,
    ) -> Self {
        masks.sort_by(|left, right| right.len().cmp(&left.len()).then_with(|| left.cmp(right)));
        masks.dedup();
        Self {
            global_masks: masks,
            problem_matchers: ProblemMatcherRegistry::new(workspace, matcher_root),
            ..Self::default()
        }
    }

    fn mark_preprocessed_log(&mut self, message_id: Uuid) {
        self.preprocessed_logs.insert(message_id);
    }

    fn take_preprocessed_log(&mut self, message_id: Uuid) -> bool {
        self.preprocessed_logs.remove(&message_id)
    }

    fn register_workspace(&mut self, scope: &str, workspace: PathBuf) {
        self.problem_matchers.register_workspace(scope, workspace);
    }

    fn ensure_healthy(&self) -> Result<()> {
        match &self.fatal_error {
            Some(error) => bail!("workflow command processing failed: {error}"),
            None => Ok(()),
        }
    }

    fn register_global_mask(&mut self, value: String) {
        if value.is_empty() || self.global_masks.iter().any(|mask| mask == &value) {
            return;
        }
        self.global_masks.push(value);
        self.global_masks
            .sort_by(|left, right| right.len().cmp(&left.len()).then_with(|| left.cmp(right)));
    }

    fn fail(&mut self, error: String) {
        self.fatal_error.get_or_insert(error);
    }

    fn process_log(&mut self, step_id: &str, stream: LogStream, data: &mut String) -> Result<()> {
        let partial_key = (step_id.to_owned(), log_stream_key(stream));
        if let Some(partial) = self.partial_commands.get_mut(&partial_key) {
            partial.push_str(data);
            if partial.len() > MAX_DYNAMIC_MASK_BYTES_PER_JOB {
                bail!("workflow command exceeded the dynamic-mask safety limit");
            }
            if data.ends_with('\n') || data.len() < MAX_LOG_CHUNK_BYTES {
                let complete = self
                    .partial_commands
                    .remove(&partial_key)
                    .expect("partial workflow command exists");
                *data = complete;
                self.process_complete_log(step_id, data)?;
            } else {
                data.clear();
            }
            return Ok(());
        }

        let line = data.trim_end_matches(['\r', '\n']);
        if data.len() >= MAX_LOG_CHUNK_BYTES
            && !data.ends_with('\n')
            && self.is_suppressible_command(step_id, line)
        {
            self.partial_commands
                .insert(partial_key, std::mem::take(data));
            return Ok(());
        }
        self.process_complete_log(step_id, data)
    }

    fn process_complete_log(&mut self, step_id: &str, data: &mut String) -> Result<()> {
        let mut visible = String::with_capacity(data.len());
        for segment in data.split_inclusive('\n') {
            let line = segment.trim_end_matches(['\r', '\n']);
            match self.process_line(step_id, line)? {
                WorkflowLineDisposition::Original => visible.push_str(segment),
                WorkflowLineDisposition::Suppress => {}
                WorkflowLineDisposition::Replace(replacement) => {
                    visible.push_str(&replacement);
                    if segment.ends_with('\n') {
                        visible.push('\n');
                    }
                }
            }
        }
        *data = visible;
        Ok(())
    }

    fn process_line(&mut self, step_id: &str, line: &str) -> Result<WorkflowLineDisposition> {
        let scope_id = workflow_command_scope(step_id);
        if let Some(token) = self
            .scopes
            .get(&scope_id)
            .and_then(|scope| scope.stop_token.as_ref())
        {
            if line == format!("::{token}::") {
                self.scopes.entry(scope_id).or_default().stop_token = None;
                return Ok(WorkflowLineDisposition::Suppress);
            }
            return Ok(WorkflowLineDisposition::Original);
        }
        if let Some(value) = strip_prefix_ignore_ascii_case(line, "::add-mask::") {
            let value = decode_workflow_command_data(value);
            self.scopes
                .entry(scope_id)
                .or_default()
                .register_mask(&value)?;
            return Ok(WorkflowLineDisposition::Suppress);
        }
        if let Some(token) = strip_prefix_ignore_ascii_case(line, "::stop-commands::") {
            let token = decode_workflow_command_data(token);
            if !token.is_empty() {
                self.scopes.entry(scope_id).or_default().stop_token = Some(token);
            }
            return Ok(WorkflowLineDisposition::Suppress);
        }
        if let Some(path) = strip_prefix_ignore_ascii_case(line, "::add-matcher::") {
            let path = decode_workflow_command_data(path);
            self.problem_matchers.add_from_file(&scope_id, &path)?;
            return Ok(WorkflowLineDisposition::Suppress);
        }
        if let Some(removal) = parse_remove_matcher_command(line)? {
            match removal {
                ProblemMatcherRemoval::Owner(owner) => {
                    self.problem_matchers.remove_owner(&scope_id, &owner)?;
                }
                ProblemMatcherRemoval::File(path) => {
                    self.problem_matchers.remove_from_file(&scope_id, &path)?;
                }
            }
            return Ok(WorkflowLineDisposition::Suppress);
        }
        if let Some(annotation) =
            parse_workflow_annotation(line, self.problem_matchers.workspace(&scope_id))?
        {
            let visible = annotation_log_line(&annotation);
            self.record_annotation(step_id, annotation);
            return Ok(WorkflowLineDisposition::Replace(visible));
        }
        if let Some((name, value)) = parse_named_workflow_command(line, "set-output")? {
            self.record_legacy_value(step_id, name, value, false)?;
            return Ok(WorkflowLineDisposition::Suppress);
        }
        if let Some((name, value)) = parse_named_workflow_command(line, "save-state")? {
            self.record_legacy_value(step_id, name, value, true)?;
            return Ok(WorkflowLineDisposition::Suppress);
        }
        if let Some(annotation) = self.problem_matchers.scan(&scope_id, line) {
            self.record_annotation(step_id, annotation);
        }
        Ok(WorkflowLineDisposition::Original)
    }

    fn record_annotation(&mut self, step_id: &str, annotation: CheckAnnotation) {
        let per_step_limit = match annotation.annotation_level {
            CheckAnnotationLevel::Failure => Some(MAX_ERROR_ANNOTATIONS_PER_STEP),
            CheckAnnotationLevel::Warning => Some(MAX_WARNING_ANNOTATIONS_PER_STEP),
            CheckAnnotationLevel::Notice => Some(MAX_NOTICE_ANNOTATIONS_PER_STEP),
        };
        if let Some(limit) = per_step_limit {
            let key = (step_id.to_owned(), annotation.annotation_level);
            let count = self.annotation_counts.entry(key).or_default();
            if *count >= limit {
                return;
            }
            *count += 1;
        }
        if self.annotations.len() < MAX_CHECK_ANNOTATIONS {
            self.annotations.push(annotation);
        }
    }

    fn take_annotations(&mut self) -> Vec<CheckAnnotation> {
        std::mem::take(&mut self.annotations)
    }

    fn record_legacy_value(
        &mut self,
        step_id: &str,
        name: String,
        value: String,
        state: bool,
    ) -> Result<()> {
        if name.is_empty()
            || name.len() > 1_024
            || name.contains(['\0', '\n', '\r', '='])
            || value.contains('\0')
        {
            bail!("legacy workflow command contains an invalid name or value");
        }
        let values = self.legacy_values.entry(step_id.to_owned()).or_default();
        values.command_count = values.command_count.saturating_add(1);
        values.command_bytes = values
            .command_bytes
            .saturating_add(name.len())
            .saturating_add(value.len());
        if values.command_count > MAX_LEGACY_COMMAND_VALUES_PER_STEP
            || values.command_bytes > MAX_LEGACY_COMMAND_BYTES_PER_STEP
        {
            bail!("legacy workflow commands exceeded the per-step safety limit");
        }
        if state {
            values.state.insert(name, value);
        } else {
            values.outputs.insert(name, value);
        }
        Ok(())
    }

    fn take_legacy_outputs(&mut self, step_id: &str) -> Result<BTreeMap<String, String>> {
        self.ensure_healthy()?;
        Ok(self
            .legacy_values
            .get_mut(step_id)
            .map(|values| std::mem::take(&mut values.outputs))
            .unwrap_or_default())
    }

    fn take_legacy_state(&mut self, step_id: &str) -> Result<BTreeMap<String, String>> {
        self.ensure_healthy()?;
        Ok(self
            .legacy_values
            .get_mut(step_id)
            .map(|values| std::mem::take(&mut values.state))
            .unwrap_or_default())
    }

    fn is_suppressible_command(&self, step_id: &str, line: &str) -> bool {
        let scope_id = workflow_command_scope(step_id);
        match self
            .scopes
            .get(&scope_id)
            .and_then(|scope| scope.stop_token.as_ref())
        {
            Some(token) => line == format!("::{token}::"),
            None => {
                strip_prefix_ignore_ascii_case(line, "::add-mask::").is_some()
                    || strip_prefix_ignore_ascii_case(line, "::stop-commands::").is_some()
                    || strip_prefix_ignore_ascii_case(line, "::set-output ").is_some()
                    || strip_prefix_ignore_ascii_case(line, "::save-state ").is_some()
                    || is_annotation_workflow_command(line)
                    || is_problem_matcher_workflow_command(line)
            }
        }
    }

    fn finish_step(&mut self, step_id: &str) -> Result<()> {
        let keys = self
            .partial_commands
            .keys()
            .filter(|(partial_step, _)| partial_step == step_id)
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            let mut complete = self
                .partial_commands
                .remove(&key)
                .expect("partial workflow command exists");
            self.process_complete_log(step_id, &mut complete)?;
        }
        if let Some(scope) = self.scopes.get_mut(&workflow_command_scope(step_id)) {
            scope.stop_token = None;
        }
        self.problem_matchers
            .reset_scope(&workflow_command_scope(step_id));
        self.legacy_values.remove(step_id);
        Ok(())
    }

    fn finish_stream(&mut self, step_id: &str, stream: LogStream) -> Result<()> {
        let key = (step_id.to_owned(), log_stream_key(stream));
        if let Some(mut complete) = self.partial_commands.remove(&key) {
            self.process_complete_log(step_id, &mut complete)?;
        }
        Ok(())
    }

    fn mask_for_step(&self, step_id: &str, value: &mut String) {
        mask_text(value, &self.global_masks);
        if let Some(scope) = self.scopes.get(&workflow_command_scope(step_id)) {
            mask_text(value, &scope.masks);
        }
    }

    fn mask_all(&self, value: &mut String) {
        mask_text(value, &self.global_masks);
        for scope in self.scopes.values() {
            mask_text(value, &scope.masks);
        }
    }

    fn value_is_masked_for_step(&self, step_id: &str, value: &str) -> bool {
        self.global_masks.iter().any(|mask| value.contains(mask))
            || self
                .scopes
                .get(&workflow_command_scope(step_id))
                .is_some_and(|scope| scope.masks.iter().any(|mask| value.contains(mask)))
    }
}

impl WorkflowCommandScope {
    fn register_mask(&mut self, value: &str) -> Result<()> {
        for candidate in std::iter::once(value).chain(value.split_whitespace()) {
            if candidate.is_empty() || self.masks.iter().any(|mask| mask == candidate) {
                continue;
            }
            if self.masks.len() >= MAX_DYNAMIC_MASKS_PER_JOB
                || self.mask_bytes.saturating_add(candidate.len()) > MAX_DYNAMIC_MASK_BYTES_PER_JOB
            {
                bail!("workflow added too many dynamic masks");
            }
            self.mask_bytes += candidate.len();
            self.masks.push(candidate.to_owned());
        }
        self.masks
            .sort_by_key(|value| std::cmp::Reverse(value.len()));
        Ok(())
    }
}

fn workflow_command_scope(step_id: &str) -> String {
    let mut components = step_id.splitn(3, '/');
    match (components.next(), components.next()) {
        (Some(plan), Some(job)) => format!("{plan}/{job}"),
        _ => step_id.to_owned(),
    }
}

fn log_stream_key(stream: LogStream) -> u8 {
    match stream {
        LogStream::Stdout => 0,
        LogStream::Stderr => 1,
        LogStream::System => 2,
    }
}

fn strip_prefix_ignore_ascii_case<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    value
        .get(..prefix.len())
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix))
        .then(|| &value[prefix.len()..])
}

fn decode_workflow_command_data(value: &str) -> String {
    value
        .replace("%0D", "\r")
        .replace("%0d", "\r")
        .replace("%0A", "\n")
        .replace("%0a", "\n")
        .replace("%25", "%")
}

fn decode_workflow_command_property(value: &str) -> String {
    value
        .replace("%0D", "\r")
        .replace("%0d", "\r")
        .replace("%0A", "\n")
        .replace("%0a", "\n")
        .replace("%3A", ":")
        .replace("%3a", ":")
        .replace("%2C", ",")
        .replace("%2c", ",")
        .replace("%25", "%")
}

enum ProblemMatcherRemoval {
    Owner(String),
    File(String),
}

fn is_problem_matcher_workflow_command(line: &str) -> bool {
    strip_prefix_ignore_ascii_case(line, "::add-matcher::").is_some()
        || strip_prefix_ignore_ascii_case(line, "::remove-matcher::").is_some()
        || strip_prefix_ignore_ascii_case(line, "::remove-matcher ").is_some()
}

fn parse_remove_matcher_command(line: &str) -> Result<Option<ProblemMatcherRemoval>> {
    let Some(body) = strip_prefix_ignore_ascii_case(line, "::remove-matcher") else {
        return Ok(None);
    };
    let (property_text, data) = if let Some(data) = body.strip_prefix("::") {
        ("", data)
    } else if let Some(properties) = body.strip_prefix(' ') {
        properties
            .split_once("::")
            .context("remove-matcher command is missing its data delimiter")?
    } else {
        return Ok(None);
    };
    let owner = property_text
        .split(',')
        .filter_map(|property| property.split_once('='))
        .find_map(|(key, value)| {
            key.trim()
                .eq_ignore_ascii_case("owner")
                .then(|| decode_workflow_command_property(value))
        })
        .filter(|owner| !owner.is_empty());
    let data = decode_workflow_command_data(data);
    match (owner, data.is_empty()) {
        (Some(owner), true) => Ok(Some(ProblemMatcherRemoval::Owner(owner))),
        (None, false) => Ok(Some(ProblemMatcherRemoval::File(data))),
        (Some(_), false) => bail!("remove-matcher command cannot specify both owner and file"),
        (None, true) => bail!("remove-matcher command must specify an owner or file"),
    }
}

fn is_annotation_workflow_command(line: &str) -> bool {
    let Some(body) = line.strip_prefix("::") else {
        return false;
    };
    let header = body.split_once("::").map_or(body, |(header, _)| header);
    let command = header
        .split_once(' ')
        .map_or(header, |(command, _)| command);
    matches!(
        command.to_ascii_lowercase().as_str(),
        "notice" | "warning" | "error"
    )
}

fn parse_workflow_annotation(
    line: &str,
    workspace: Option<&Path>,
) -> Result<Option<CheckAnnotation>> {
    if !is_annotation_workflow_command(line) {
        return Ok(None);
    }
    let body = line
        .strip_prefix("::")
        .expect("annotation workflow command has a prefix");
    let (header, data) = body
        .split_once("::")
        .context("annotation workflow command is missing its data delimiter")?;
    let (command, property_text) = header.split_once(' ').unwrap_or((header, ""));
    let annotation_level = match command.to_ascii_lowercase().as_str() {
        "notice" => CheckAnnotationLevel::Notice,
        "warning" => CheckAnnotationLevel::Warning,
        "error" => CheckAnnotationLevel::Failure,
        _ => return Ok(None),
    };
    let mut properties = BTreeMap::new();
    for property in property_text
        .split(',')
        .filter(|property| !property.is_empty())
    {
        let (key, value) = property
            .split_once('=')
            .context("annotation workflow command contains a property without a value")?;
        properties.insert(
            key.trim().to_ascii_lowercase(),
            decode_workflow_command_property(value),
        );
    }

    let message = decode_workflow_command_data(data);
    if message.is_empty()
        || message.len() > MAX_CHECK_ANNOTATION_MESSAGE_BYTES
        || message.contains('\0')
    {
        bail!("annotation workflow command contains an invalid message");
    }
    let path = normalize_annotation_path(
        properties
            .get("file")
            .map(String::as_str)
            .unwrap_or(".github"),
        workspace,
    )?;
    let start_line = annotation_coordinate(&properties, "line")?.unwrap_or(1);
    let end_line = annotation_coordinate(&properties, "endline")?.unwrap_or(start_line);
    if end_line < start_line {
        bail!("annotation workflow command endLine precedes line");
    }
    let start_column = annotation_coordinate(&properties, "col")?;
    let end_column = annotation_coordinate(&properties, "endcolumn")?;
    let (start_column, end_column) = if start_line == end_line {
        match (start_column, end_column) {
            (None, None) => (None, None),
            (Some(start), None) => (Some(start), Some(start)),
            (None, Some(end)) => (Some(end), Some(end)),
            (Some(start), Some(end)) if end >= start => (Some(start), Some(end)),
            (Some(_), Some(_)) => {
                bail!("annotation workflow command endColumn precedes col")
            }
        }
    } else {
        (None, None)
    };
    let title = properties
        .get("title")
        .filter(|title| !title.is_empty())
        .cloned();
    if title
        .as_ref()
        .is_some_and(|title| title.len() > MAX_CHECK_ANNOTATION_TITLE_BYTES || title.contains('\0'))
    {
        bail!("annotation workflow command contains an invalid title");
    }
    Ok(Some(CheckAnnotation {
        path,
        start_line,
        end_line,
        start_column,
        end_column,
        annotation_level,
        message,
        title,
    }))
}

fn annotation_coordinate(properties: &BTreeMap<String, String>, name: &str) -> Result<Option<u32>> {
    let Some(value) = properties.get(name) else {
        return Ok(None);
    };
    let coordinate = value.parse::<u32>().with_context(|| {
        format!("annotation workflow command property '{name}' is not an integer")
    })?;
    if coordinate == 0 || coordinate > i32::MAX as u32 {
        bail!("annotation workflow command property '{name}' is outside GitHub's range");
    }
    Ok(Some(coordinate))
}

fn normalize_annotation_path(value: &str, workspace: Option<&Path>) -> Result<String> {
    let value = value.replace('\\', "/");
    let candidate = Path::new(&value);
    let relative = if candidate.is_absolute() {
        workspace
            .and_then(|workspace| candidate.strip_prefix(workspace).ok())
            .unwrap_or_else(|| Path::new(".github"))
    } else if value.as_bytes().get(1) == Some(&b':') {
        Path::new(".github")
    } else {
        candidate
    };
    let mut normalized = Vec::new();
    for component in relative.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::Normal(component) => normalized.push(
                component
                    .to_str()
                    .context("annotation workflow command path is not UTF-8")?,
            ),
            std::path::Component::ParentDir
            | std::path::Component::RootDir
            | std::path::Component::Prefix(_) => {
                normalized.clear();
                normalized.push(".github");
                break;
            }
        }
    }
    let path = if normalized.is_empty() {
        ".github".to_owned()
    } else {
        normalized.join("/")
    };
    if path.len() > MAX_CHECK_ANNOTATION_PATH_BYTES || path.contains(['\0', '\r', '\n']) {
        bail!("annotation workflow command contains an invalid file path");
    }
    Ok(path)
}

fn annotation_log_line(annotation: &CheckAnnotation) -> String {
    let level = match annotation.annotation_level {
        CheckAnnotationLevel::Notice => "notice",
        CheckAnnotationLevel::Warning => "warning",
        CheckAnnotationLevel::Failure => "error",
    };
    match &annotation.title {
        Some(title) => format!("{level}: {title}: {}", annotation.message),
        None => format!("{level}: {}", annotation.message),
    }
}

fn bound_masked_annotation(annotation: &mut CheckAnnotation) {
    if annotation.path.len() > MAX_CHECK_ANNOTATION_PATH_BYTES {
        annotation.path = ".github".to_owned();
    }
    truncate_utf8_bytes(&mut annotation.message, MAX_CHECK_ANNOTATION_MESSAGE_BYTES);
    if let Some(title) = &mut annotation.title {
        truncate_utf8_bytes(title, MAX_CHECK_ANNOTATION_TITLE_BYTES);
    }
}

fn truncate_utf8_bytes(value: &mut String, maximum: usize) {
    if value.len() <= maximum {
        return;
    }
    let mut boundary = maximum;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.truncate(boundary);
}

fn parse_named_workflow_command(line: &str, command: &str) -> Result<Option<(String, String)>> {
    let prefix = format!("::{command} ");
    let Some(command_body) = strip_prefix_ignore_ascii_case(line, &prefix) else {
        return Ok(None);
    };
    let (properties, data) = command_body
        .split_once("::")
        .with_context(|| format!("legacy {command} command is missing its data delimiter"))?;
    let name = properties
        .split(',')
        .filter_map(|property| property.split_once('='))
        .find_map(|(key, value)| {
            key.trim()
                .eq_ignore_ascii_case("name")
                .then(|| decode_workflow_command_property(value))
        })
        .with_context(|| format!("legacy {command} command is missing its name property"))?;
    Ok(Some((name, decode_workflow_command_data(data))))
}

fn mask_text(value: &mut String, secrets: &[String]) {
    for secret in secrets {
        if value.contains(secret) {
            *value = value.replace(secret, "***");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gitzero_protocol::{PullRequestSpec, RepositorySpec};

    fn local_repository_access(
        workflow_commands: &Arc<StdMutex<WorkflowCommandProcessor>>,
    ) -> RunRepositoryAccess {
        RunRepositoryAccess::new(RepositoryAccessClient::local(), workflow_commands.clone())
    }

    #[test]
    fn only_accepts_full_commit_shas() {
        assert!(validate_sha("0123456789012345678901234567890123456789").is_ok());
        assert!(validate_sha(&"a".repeat(64)).is_ok());
        assert!(validate_sha("main").is_err());
        assert!(validate_sha("-012345678901234567890123456789012345678").is_err());
        assert!(validate_sha(&"a".repeat(63)).is_err());
        assert!(validate_sha(&format!("{}z", "a".repeat(63))).is_err());
    }

    #[test]
    fn actions_runtime_token_exposes_only_the_scoped_backend_ids() {
        let token = actions_runtime_token("run-backend", "job-backend").expect("runtime token");
        let segments = token.split('.').collect::<Vec<_>>();
        assert_eq!(segments.len(), 3);
        let payload = URL_SAFE_NO_PAD
            .decode(segments[1])
            .expect("decode runtime token payload");
        let payload: JsonValue = serde_json::from_slice(&payload).expect("runtime token JSON");
        assert_eq!(payload["scp"], "Actions.Results:run-backend:job-backend");
        assert!(segments[2].len() >= 64);
    }

    #[test]
    fn runner_selectors_require_every_label_and_the_configured_group() {
        let mut context = EvaluationContext::new();
        context
            .insert_json("matrix", json!({"channel": "stable", "arch": "ARM64"}))
            .expect("matrix context");
        let source = r#"
group: release-${{ matrix.channel }}
labels:
  - self-hosted
  - macOS
  - ${{ matrix.arch }}
  - xcode-16
"#;
        let value: YamlValue = serde_yaml_ng::from_str(source).expect("selector YAML");
        let selector = render_runner_selector(&value, &context).expect("runner selector");
        assert_eq!(
            selector,
            RunnerSelector {
                labels: vec![
                    "self-hosted".to_owned(),
                    "macOS".to_owned(),
                    "ARM64".to_owned(),
                    "xcode-16".to_owned(),
                ],
                group: Some("release-stable".to_owned()),
            }
        );

        let labels = ["self-hosted", "macos", "arm64", "xcode-16"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        ensure_runner_eligible(&selector, &labels, Some("release-stable")).expect("matching agent");
        assert!(
            ensure_runner_eligible(&selector, &labels, Some("other-group"))
                .expect_err("wrong group")
                .to_string()
                .contains("release-stable")
        );
        let missing_custom = default_runner_targeting();
        assert!(
            ensure_runner_eligible(&selector, &missing_custom.labels, Some("release-stable"))
                .expect_err("missing custom label")
                .to_string()
                .contains("xcode-16")
        );
    }

    #[test]
    fn runner_selectors_virtualize_only_a_single_hosted_macos_label() {
        let context = EvaluationContext::new();
        let targeting = default_runner_targeting();
        let hosted: YamlValue = serde_yaml_ng::from_str("macos-latest").expect("hosted YAML");
        let hosted = render_runner_selector(&hosted, &context).expect("hosted selector");
        ensure_runner_eligible(&hosted, &targeting.labels, None).expect("hosted macOS label");

        let linux: YamlValue = serde_yaml_ng::from_str("ubuntu-latest").expect("Linux YAML");
        let linux = render_runner_selector(&linux, &context).expect("Linux selector");
        assert!(ensure_runner_eligible(&linux, &targeting.labels, None).is_err());

        let mixed: YamlValue =
            serde_yaml_ng::from_str("[macos-latest, xcode-16]").expect("mixed YAML");
        let mixed = render_runner_selector(&mixed, &context).expect("mixed selector");
        assert!(ensure_runner_eligible(&mixed, &targeting.labels, None).is_err());
    }

    #[test]
    fn preflight_collects_deterministic_conditions_and_expression_selectors() {
        let workflow = parse(
            r#"
name: Targeted
on: pull_request
jobs:
  hosted:
    runs-on: macos-latest
    steps:
      - run: echo hosted
  release:
    runs-on:
      group: ${{ vars.runner_group }}
      labels: [self-hosted, macOS, ARM64, xcode-16]
    steps:
      - run: echo release
  conditional:
    if: false
    runs-on: [self-hosted, macOS, unavailable-when-skipped]
    steps:
      - run: echo skipped
  matrix-expression:
    runs-on: [self-hosted, macOS, "${{ matrix.runner }}"]
    strategy:
      matrix:
        runner: [xcode-15, xcode-16]
    steps:
      - run: echo dynamic-selector
  event-conditioned:
    if: github.repository == 'local/fixture'
    runs-on: [self-hosted, macOS, event-match]
    steps:
      - run: echo event-match
  input-expression:
    runs-on: [self-hosted, macOS, "${{ inputs.runner }}"]
    steps:
      - run: echo reusable-input
  runtime-conditioned:
    needs: hosted
    if: needs.hosted.result == 'success'
    runs-on: [self-hosted, macOS, runtime-only]
    steps:
      - run: echo runtime-only
  runtime-selector:
    needs: hosted
    runs-on: [self-hosted, macOS, "${{ needs.hosted.outputs.runner }}"]
    steps:
      - run: echo runtime-selector
  linux:
    runs-on: ubuntu-latest
    steps:
      - run: echo unsupported-hosted-platform
"#,
        )
        .expect("parse workflow");
        let mut plan =
            gitzero_workflow::compile(&workflow, Path::new(".github/workflows/targeted.yml"))
                .expect("compile workflow");
        plan.jobs
            .iter_mut()
            .find(|job| job.base_id == "input-expression")
            .expect("input expression job")
            .reusable_input_scopes
            .push(gitzero_workflow::PlannedReusableInputScope {
                inputs: BTreeMap::from([(
                    "runner".to_owned(),
                    gitzero_workflow::PlannedReusableInput {
                        value: YamlValue::String("${{ matrix.runner }}".to_owned()),
                        input_type: ReusableInputType::String,
                    },
                )]),
                matrix: BTreeMap::from([(
                    "runner".to_owned(),
                    JsonValue::String("xcode-17".to_owned()),
                )]),
            });
        let mut run = fixture_run(
            Uuid::nil(),
            "1".repeat(40),
            "0".repeat(40),
            "file:///fixture.git".to_owned(),
        );
        run.variables
            .insert("runner_group".to_owned(), "release-minis".to_owned());
        let directory = tempfile::tempdir().expect("preflight directory");
        let requirements = statically_resolvable_runner_requirements(
            &[plan],
            &run,
            directory.path(),
            directory.path(),
        )
        .expect("runner requirements");

        assert_eq!(requirements.len(), 6);
        assert!(requirements.contains(&RunnerRequirement {
            labels: vec!["macos-latest".to_owned()],
            runner_group: None,
        }));
        assert!(requirements.contains(&RunnerRequirement {
            labels: vec![
                "arm64".to_owned(),
                "macos".to_owned(),
                "self-hosted".to_owned(),
                "xcode-16".to_owned(),
            ],
            runner_group: Some("release-minis".to_owned()),
        }));
        for label in ["xcode-15", "xcode-16"] {
            assert!(requirements.contains(&RunnerRequirement {
                labels: vec![
                    "macos".to_owned(),
                    "self-hosted".to_owned(),
                    label.to_owned(),
                ],
                runner_group: None,
            }));
        }
        assert!(requirements.contains(&RunnerRequirement {
            labels: vec![
                "event-match".to_owned(),
                "macos".to_owned(),
                "self-hosted".to_owned(),
            ],
            runner_group: None,
        }));
        assert!(requirements.contains(&RunnerRequirement {
            labels: vec![
                "macos".to_owned(),
                "self-hosted".to_owned(),
                "xcode-17".to_owned(),
            ],
            runner_group: None,
        }));
        assert!(!requirements.iter().any(|requirement| {
            requirement
                .labels
                .iter()
                .any(|label| label == "unavailable-when-skipped" || label == "runtime-only")
        }));
        assert!(!runner_satisfies_requirements(
            &requirements,
            &default_runner_targeting()
        ));
        let specialized = RunnerTargeting {
            labels: [
                "self-hosted",
                "macos",
                "arm64",
                "xcode-15",
                "xcode-16",
                "xcode-17",
                "event-match",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            group: Some("release-minis".to_owned()),
        };
        assert!(runner_satisfies_requirements(&requirements, &specialized));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn rejects_an_incompatible_assignment_before_job_start() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        let repository = fixture.path().join("repository");
        std::fs::create_dir_all(repository.join(".github/workflows")).expect("workflow directory");
        git(&repository, ["init", "--initial-branch=main"]);
        git(
            &repository,
            ["config", "user.email", "gitzero@example.test"],
        );
        git(&repository, ["config", "user.name", "GitZero Test"]);
        std::fs::write(repository.join("README.md"), "fixture\n").expect("fixture file");
        std::fs::write(
            repository.join(".github/workflows/targeted.yml"),
            r#"
name: Targeted
on: pull_request
jobs:
  release:
    strategy:
      matrix:
        channel: [minis]
        arch: [ARM64]
        xcode: ['16']
    runs-on:
      group: release-${{ matrix.channel }}
      labels: [self-hosted, macOS, "${{ matrix.arch }}", "xcode-${{ matrix.xcode }}"]
    steps:
      - run: echo should-not-run
"#,
        )
        .expect("workflow");
        git(&repository, ["add", "."]);
        git(&repository, ["commit", "-m", "fixture"]);
        let head_sha = git_output(&repository, ["rev-parse", "HEAD"]);
        let remote = fixture.path().join("remote.git");
        git(
            fixture.path(),
            ["init", "--bare", remote.to_str().expect("remote path")],
        );
        git(
            &repository,
            [
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path"),
            ],
        );
        git(&repository, ["push", "origin", "main"]);
        git(&repository, ["push", "origin", "HEAD:refs/pull/1/head"]);
        git(&repository, ["push", "origin", "HEAD:refs/pull/1/merge"]);

        let work_root = fixture.path().join("work");
        let executor = Executor::new(ExecutorConfig {
            work_root: work_root.clone(),
            runner_name: "basic-mini".to_owned(),
            keep_failed_workspaces: true,
            max_parallelism: 1,
            cache_max_bytes: 10 * 1024 * 1024,
            cache_max_entry_bytes: 1024 * 1024,
            artifact_max_bytes: 10 * 1024 * 1024,
            artifact_max_entry_bytes: 1024 * 1024,
        });
        let run_id = Uuid::new_v4();
        let run = fixture_run(
            run_id,
            head_sha.clone(),
            head_sha,
            format!("file://{}", remote.display()),
        );
        let (outbound, mut incoming) = mpsc::channel(128);
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let execution = executor.execute(run, cancel_rx, outbound);
        tokio::pin!(execution);
        let mut events = Vec::new();
        loop {
            tokio::select! {
                result = &mut execution => {
                    result.expect("reject assignment");
                    break;
                }
                message = incoming.recv() => {
                    if let Some(message) = message {
                        events.push(message);
                    }
                }
            }
        }
        while let Ok(message) = incoming.try_recv() {
            events.push(message);
        }

        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::JobRejected { requirements, .. }
                if requirements.contains(&RunnerRequirement {
                    labels: vec![
                        "arm64".to_owned(),
                        "macos".to_owned(),
                        "self-hosted".to_owned(),
                        "xcode-16".to_owned(),
                    ],
                    runner_group: Some("release-minis".to_owned()),
                })
        )));
        assert!(!events.iter().any(|message| matches!(
            message,
            AgentMessage::JobStarted { .. } | AgentMessage::JobFinished { .. }
        )));
        assert!(!work_root.join(run_id.to_string()).exists());
    }

    #[test]
    fn checkout_paths_are_lexically_contained() {
        let workspace = Path::new("/tmp/gitzero-workspace");
        assert_eq!(
            checkout_directory(workspace, "").expect("root checkout"),
            workspace
        );
        assert_eq!(
            checkout_directory(workspace, ".").expect("dot checkout"),
            workspace
        );
        assert_eq!(
            checkout_directory(workspace, "nested/repository").expect("nested checkout"),
            workspace.join("nested/repository")
        );
        for invalid in ["../escape", "nested/../../escape", "/tmp/escape"] {
            assert!(
                checkout_directory(workspace, invalid).is_err(),
                "accepted unsafe checkout path {invalid:?}"
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn checkout_paths_cannot_traverse_existing_symlinks() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        let workspace = fixture.path().join("workspace");
        let outside = fixture.path().join("outside");
        tokio::fs::create_dir_all(&workspace)
            .await
            .expect("workspace directory");
        tokio::fs::create_dir_all(&outside)
            .await
            .expect("outside directory");
        std::os::unix::fs::symlink(&outside, workspace.join("linked")).expect("workspace symlink");

        let nested =
            checkout_directory(&workspace, "nested/./repository/").expect("nested checkout path");
        assert_eq!(
            validate_checkout_directory(&workspace, &nested)
                .await
                .expect("canonical nested checkout path"),
            tokio::fs::canonicalize(&workspace)
                .await
                .expect("canonical workspace")
                .join("nested/repository")
        );

        let directory =
            checkout_directory(&workspace, "linked/repository").expect("lexical checkout path");
        let error = validate_checkout_directory(&workspace, &directory)
            .await
            .expect_err("symlink traversal should fail");
        assert!(format!("{error:#}").contains("symbolic link"));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn checkout_repository_detection_never_discovers_a_parent_repository() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        git(fixture.path(), ["init", "--initial-branch=main"]);
        let workspace = fixture.path().join("nested/job-workspace");
        tokio::fs::create_dir_all(&workspace)
            .await
            .expect("nested workspace");
        assert!(
            !git_repository_exists(&workspace, &BTreeMap::new())
                .await
                .expect("inspect empty nested workspace"),
            "parent repository was mistaken for a job checkout"
        );

        git(&workspace, ["init"]);
        assert!(
            git_repository_exists(&workspace, &BTreeMap::new())
                .await
                .expect("inspect direct workspace repository"),
            "direct checkout repository was not recognized"
        );
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn checkout_safe_directory_is_ephemeral_and_can_be_disabled() {
        let fixture = tempfile::tempdir().expect("safe directory tempdir");
        let source = fixture.path().join("source");
        let run_dir = fixture.path().join("runs").join(Uuid::new_v4().to_string());
        let safe_workspace = run_dir.join("safe-workspace");
        let unsafe_workspace = run_dir.join("unsafe-workspace");
        for directory in [&source, &run_dir, &safe_workspace, &unsafe_workspace] {
            std::fs::create_dir_all(directory).expect("fixture directory");
        }
        git(&source, ["init", "--initial-branch=main"]);
        git(&source, ["config", "user.email", "gitzero@example.test"]);
        git(&source, ["config", "user.name", "GitZero Test"]);
        std::fs::write(source.join("README.md"), "safe directory fixture\n").expect("fixture file");
        git(&source, ["add", "README.md"]);
        git(&source, ["commit", "-m", "safe directory fixture"]);
        let commit = git_output(&source, ["rev-parse", "HEAD"]);
        let run = fixture_run(
            Uuid::new_v4(),
            commit.clone(),
            commit,
            source.display().to_string(),
        );
        let repository = checkout_repository(None, &run).expect("same repository");
        let inputs = BTreeMap::from([("show-progress".to_owned(), "false".to_owned())]);
        let source_config = fixture.path().join("source-safe.gitconfig");
        for directory in [
            source.join(".git"),
            std::fs::canonicalize(source.join(".git")).expect("canonical source Git directory"),
        ] {
            let status = std::process::Command::new("git")
                .args(["config", "--file"])
                .arg(&source_config)
                .args(["--add", CHECKOUT_SAFE_DIRECTORY])
                .arg(directory)
                .status()
                .expect("configure source safe directory");
            assert!(status.success());
        }
        let environment = BTreeMap::from([
            ("GIT_TEST_ASSUME_DIFFERENT_OWNER".to_owned(), "1".to_owned()),
            (
                "GIT_CONFIG_GLOBAL".to_owned(),
                source_config.display().to_string(),
            ),
        ]);
        let workflow_commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::default()));
        let repository_access = local_repository_access(&workflow_commands);
        let (outbound, mut incoming) = mpsc::channel(128);
        let (_cancel_tx, cancel) = watch::channel(false);

        let mut safe_environment = environment.clone();
        let safe_result = execute_checkout_step_with_repository(
            run.id,
            "0/safe-checkout",
            &inputs,
            &run,
            &source,
            &safe_workspace,
            &run_dir,
            &mut safe_environment,
            "",
            repository.clone(),
            None,
            &cancel,
            &outbound,
            &Arc::new(AtomicU64::new(0)),
            &repository_access,
            &BTreeSet::new(),
        )
        .await;
        if let Err(error) = safe_result {
            let mut logs = String::new();
            while let Ok(message) = incoming.try_recv() {
                if let AgentMessage::LogChunk { data, .. } = message {
                    logs.push_str(&data);
                }
            }
            panic!("checkout with the default safe directory: {error:#}\n{logs}");
        }
        assert_eq!(safe_environment, environment);
        let status = Command::new("git")
            .args(["status", "--short"])
            .current_dir(&safe_workspace)
            .envs(safe_environment.iter())
            .output()
            .await
            .expect("inspect checkout after the action scope");
        assert!(!status.status.success());
        assert!(String::from_utf8_lossy(&status.stderr).contains("dubious ownership"));

        let mut unsafe_inputs = inputs;
        unsafe_inputs.insert("set-safe-directory".to_owned(), "false".to_owned());
        let mut unsafe_environment = environment;
        let error = execute_checkout_step_with_repository(
            run.id,
            "1/unsafe-checkout",
            &unsafe_inputs,
            &run,
            &source,
            &unsafe_workspace,
            &run_dir,
            &mut unsafe_environment,
            "",
            repository,
            None,
            &cancel,
            &outbound,
            &Arc::new(AtomicU64::new(0)),
            &repository_access,
            &BTreeSet::new(),
        )
        .await
        .expect_err("checkout without a safe directory should honor Git's ownership rejection");
        assert!(format!("{error:#}").contains("configure checkout origin"));

        drop(outbound);
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn filtered_checkout_fetch_falls_back_to_the_immutable_snapshot() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        let source = fixture.path().join("source");
        let checkout = fixture.path().join("checkout");
        std::fs::create_dir_all(&source).expect("source directory");
        std::fs::create_dir_all(&checkout).expect("checkout directory");
        git(&source, ["init", "--initial-branch=main"]);
        git(&source, ["config", "user.email", "gitzero@example.test"]);
        git(&source, ["config", "user.name", "GitZero Test"]);
        std::fs::write(source.join("README.md"), "snapshot\n").expect("snapshot file");
        git(&source, ["add", "README.md"]);
        git(&source, ["commit", "-m", "snapshot"]);
        let head_sha = git_output(&source, ["rev-parse", "HEAD"]);
        let missing_remote = fixture.path().join("missing.git").display().to_string();
        git(&checkout, ["init"]);
        git(&checkout, ["remote", "add", "origin", &missing_remote]);
        let run = fixture_run(
            Uuid::new_v4(),
            head_sha.clone(),
            head_sha.clone(),
            missing_remote,
        );
        let (_cancel_tx, cancel) = watch::channel(false);
        let (outbound, mut incoming) = mpsc::channel(32);
        let sequence = Arc::new(AtomicU64::new(0));

        fetch_exact_checkout_commit(
            run.id,
            "checkout",
            &head_sha,
            Some(&source),
            &checkout,
            &BTreeMap::new(),
            Some("blob:none"),
            false,
            None,
            &cancel,
            &outbound,
            &sequence,
        )
        .await
        .expect("snapshot fallback");

        assert_eq!(git_output(&checkout, ["rev-parse", "FETCH_HEAD"]), head_sha);
        let mut saw_fallback = false;
        while let Ok(message) = incoming.try_recv() {
            saw_fallback |= matches!(
                message,
                AgentMessage::LogChunk { data, .. }
                    if data.contains("using the immutable run snapshot")
            );
        }
        assert!(saw_fallback, "filtered fetch fallback was not reported");
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn cross_repository_checkout_pins_anonymous_and_managed_secret_access() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        let source = fixture.path().join("public-source");
        let remote = fixture.path().join("public.git");
        let run_dir = fixture.path().join("runs").join(Uuid::new_v4().to_string());
        let first_workspace = run_dir.join("first-workspace");
        let second_workspace = run_dir.join("second-workspace");
        let first_default_workspace = run_dir.join("first-default-workspace");
        let second_default_workspace = run_dir.join("second-default-workspace");
        let managed_workspace = run_dir.join("managed-workspace");
        let tag_workspace = run_dir.join("tag-workspace");
        let source_repository = run_dir.join("repository");
        for directory in [
            &source,
            &run_dir,
            &first_workspace,
            &second_workspace,
            &first_default_workspace,
            &second_default_workspace,
            &managed_workspace,
            &tag_workspace,
            &source_repository,
        ] {
            std::fs::create_dir_all(directory).expect("fixture directory");
        }
        git(&source, ["init", "--initial-branch=main"]);
        git(&source, ["config", "user.email", "gitzero@example.test"]);
        git(&source, ["config", "user.name", "GitZero Test"]);
        std::fs::write(source.join("dependency.txt"), "public-v1\n").expect("public fixture file");
        git(&source, ["add", "dependency.txt"]);
        git(&source, ["commit", "-m", "public v1"]);
        let first_commit = git_output(&source, ["rev-parse", "HEAD"]);
        git(&source, ["tag", "v1"]);
        git(
            fixture.path(),
            ["init", "--bare", remote.to_str().expect("remote path")],
        );
        git(
            &source,
            [
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path"),
            ],
        );
        git(&source, ["push", "origin", "main"]);
        git(&source, ["push", "origin", "refs/tags/v1"]);
        git(&remote, ["symbolic-ref", "HEAD", "refs/heads/main"]);

        let mut run = fixture_run(
            Uuid::new_v4(),
            "a".repeat(40),
            "b".repeat(40),
            "https://github.com/local/fixture.git".to_owned(),
        );
        run.checkout_token = "source-repository-token".to_owned();
        run.checkout_token_expires_at_epoch_seconds = Some(4_102_444_800);
        let repository = CheckoutRepository {
            owner: "public-fixture".to_owned(),
            name: "dependency".to_owned(),
            clone_url: remote.display().to_string(),
            same_repository: false,
        };
        let inputs = BTreeMap::from([
            ("fetch-depth".to_owned(), "0".to_owned()),
            ("ref".to_owned(), "main".to_owned()),
            ("show-progress".to_owned(), "false".to_owned()),
            ("token".to_owned(), run.checkout_token.clone()),
        ]);
        let (_cancel_tx, cancel) = watch::channel(false);
        let (outbound, mut incoming) = mpsc::channel(256);
        let workflow_commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::default()));
        let repository_access = local_repository_access(&workflow_commands);
        let drain = tokio::spawn(async move {
            let mut events = Vec::new();
            while let Some(message) = incoming.recv().await {
                events.push(message);
            }
            events
        });
        let mut first_environment = BTreeMap::new();
        configure_checkout_credentials(&mut first_environment, &run.checkout_token, false)
            .expect("configure first checkout credentials");
        let first_outputs = execute_checkout_step_with_repository(
            run.id,
            "0/public-checkout",
            &inputs,
            &run,
            &source_repository,
            &first_workspace,
            &run_dir,
            &mut first_environment,
            &run.checkout_token,
            repository.clone(),
            None,
            &cancel,
            &outbound,
            &Arc::new(AtomicU64::new(0)),
            &repository_access,
            &BTreeSet::new(),
        )
        .await
        .expect("first public checkout");
        assert_eq!(first_outputs["commit"], first_commit);
        assert_eq!(first_outputs["ref"], "main");
        assert_eq!(
            std::fs::read_to_string(first_workspace.join("dependency.txt"))
                .expect("first checked out file"),
            "public-v1\n"
        );
        assert_eq!(
            git_output(&first_workspace, ["rev-parse", "HEAD"]),
            first_commit
        );
        assert_eq!(
            git_output(&first_workspace, ["symbolic-ref", "--short", "HEAD"]),
            "main"
        );
        assert_eq!(
            git_output(
                &first_workspace,
                [
                    "rev-parse",
                    "--abbrev-ref",
                    "--symbolic-full-name",
                    "@{upstream}"
                ]
            ),
            "origin/main"
        );
        assert_eq!(
            git_output(&first_workspace, ["rev-parse", "refs/remotes/origin/main"]),
            first_commit
        );
        git(&first_workspace, ["push", "--dry-run"]);
        assert!(
            !first_environment.contains_key("GIT_CONFIG_VALUE_0"),
            "the source repository token was persisted into a public checkout"
        );

        let mut default_inputs = inputs.clone();
        default_inputs.remove("ref");
        let mut first_default_environment = BTreeMap::new();
        let first_default_outputs = execute_checkout_step_with_repository(
            run.id,
            "1/default-checkout",
            &default_inputs,
            &run,
            &source_repository,
            &first_default_workspace,
            &run_dir,
            &mut first_default_environment,
            &run.checkout_token,
            repository.clone(),
            None,
            &cancel,
            &outbound,
            &Arc::new(AtomicU64::new(0)),
            &repository_access,
            &BTreeSet::new(),
        )
        .await
        .expect("first default-branch checkout");
        assert_eq!(first_default_outputs["commit"], first_commit);
        assert_eq!(first_default_outputs["ref"], "refs/heads/main");
        assert_eq!(
            git_output(
                &first_default_workspace,
                ["symbolic-ref", "--short", "HEAD"]
            ),
            "main"
        );
        assert_eq!(
            git_output(
                &first_default_workspace,
                [
                    "rev-parse",
                    "--abbrev-ref",
                    "--symbolic-full-name",
                    "@{upstream}"
                ]
            ),
            "origin/main"
        );

        std::fs::write(source.join("dependency.txt"), "public-v2\n")
            .expect("updated public fixture file");
        git(&source, ["add", "dependency.txt"]);
        git(&source, ["commit", "-m", "public v2"]);
        git(&source, ["push", "origin", "main"]);
        git(&source, ["push", "origin", "main:renamed"]);
        git(&remote, ["symbolic-ref", "HEAD", "refs/heads/renamed"]);

        let mut second_environment = BTreeMap::new();
        let second_outputs = execute_checkout_step_with_repository(
            run.id,
            "1/public-checkout",
            &inputs,
            &run,
            &source_repository,
            &second_workspace,
            &run_dir,
            &mut second_environment,
            &run.checkout_token,
            repository.clone(),
            None,
            &cancel,
            &outbound,
            &Arc::new(AtomicU64::new(0)),
            &repository_access,
            &BTreeSet::new(),
        )
        .await
        .expect("same-run repeated public checkout");
        assert_eq!(second_outputs["commit"], first_commit);
        assert_eq!(
            std::fs::read_to_string(second_workspace.join("dependency.txt"))
                .expect("second checked out file"),
            "public-v1\n",
            "a moving public ref changed within one PR run"
        );
        assert_eq!(
            git_output(&second_workspace, ["symbolic-ref", "--short", "HEAD"]),
            "main"
        );
        assert_eq!(
            git_output(&second_workspace, ["rev-parse", "refs/remotes/origin/main"]),
            first_commit
        );

        let mut second_default_environment = BTreeMap::new();
        let second_default_outputs = execute_checkout_step_with_repository(
            run.id,
            "2/default-checkout",
            &default_inputs,
            &run,
            &source_repository,
            &second_default_workspace,
            &run_dir,
            &mut second_default_environment,
            &run.checkout_token,
            repository.clone(),
            None,
            &cancel,
            &outbound,
            &Arc::new(AtomicU64::new(0)),
            &repository_access,
            &BTreeSet::new(),
        )
        .await
        .expect("same-run repeated default-branch checkout");
        assert_eq!(second_default_outputs["commit"], first_commit);
        assert_eq!(second_default_outputs["ref"], "refs/heads/main");
        assert_eq!(
            git_output(
                &second_default_workspace,
                ["symbolic-ref", "--short", "HEAD"]
            ),
            "main",
            "the default branch identity changed within one PR run"
        );
        assert_eq!(
            git_output(
                &second_default_workspace,
                ["rev-parse", "refs/remotes/origin/main"]
            ),
            first_commit
        );

        let managed_token = "managed-cross-repository-token";
        let managed_secret_values = BTreeSet::from([managed_token.to_owned()]);
        register_managed_secret_masks(
            &workflow_commands,
            &BTreeMap::from([("CHECKOUT_PAT".to_owned(), managed_token.to_owned())]),
        );
        let mut managed_inputs = inputs.clone();
        managed_inputs.insert("token".to_owned(), managed_token.to_owned());
        let mut managed_environment = BTreeMap::new();
        let managed_outputs = execute_checkout_step_with_repository(
            run.id,
            "3/managed-checkout",
            &managed_inputs,
            &run,
            &source_repository,
            &managed_workspace,
            &run_dir,
            &mut managed_environment,
            &run.checkout_token,
            repository.clone(),
            None,
            &cancel,
            &outbound,
            &Arc::new(AtomicU64::new(0)),
            &repository_access,
            &managed_secret_values,
        )
        .await
        .expect("managed secret checkout");
        assert_eq!(managed_outputs["commit"], first_commit);
        assert_eq!(
            std::fs::read_to_string(managed_workspace.join("dependency.txt"))
                .expect("managed checked out file"),
            "public-v1\n"
        );
        assert_eq!(
            git_output(&managed_workspace, ["symbolic-ref", "--short", "HEAD"]),
            "main"
        );
        let credential = STANDARD.encode(format!("x-access-token:{managed_token}"));
        let expected_credential = format!("AUTHORIZATION: basic {credential}");
        assert_eq!(
            managed_environment
                .get("GIT_CONFIG_VALUE_0")
                .map(String::as_str),
            Some(expected_credential.as_str())
        );

        let mut tag_inputs = inputs.clone();
        tag_inputs.insert("ref".to_owned(), "v1".to_owned());
        let mut tag_environment = BTreeMap::new();
        let tag_outputs = execute_checkout_step_with_repository(
            run.id,
            "4/tag-checkout",
            &tag_inputs,
            &run,
            &source_repository,
            &tag_workspace,
            &run_dir,
            &mut tag_environment,
            &run.checkout_token,
            repository,
            None,
            &cancel,
            &outbound,
            &Arc::new(AtomicU64::new(0)),
            &repository_access,
            &BTreeSet::new(),
        )
        .await
        .expect("tag checkout");
        assert_eq!(tag_outputs["commit"], first_commit);
        assert_eq!(tag_outputs["ref"], "v1");
        assert_eq!(
            git_output(&tag_workspace, ["rev-parse", "HEAD"]),
            first_commit
        );
        assert!(
            !std::process::Command::new("git")
                .args(["symbolic-ref", "--quiet", "HEAD"])
                .current_dir(&tag_workspace)
                .status()
                .expect("inspect tag checkout HEAD")
                .success(),
            "tag checkout unexpectedly attached to a branch"
        );

        drop(outbound);
        let events = drain.await.expect("join checkout event drain");
        assert!(!events.iter().any(|message| match message {
            AgentMessage::LogChunk { data, .. }
            | AgentMessage::JobFinished { summary: data, .. } => {
                data.contains(&run.checkout_token) || data.contains(managed_token)
            }
            _ => false,
        }));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn ssh_materialization_uses_the_selected_remote_without_rewriting_the_shared_cache() {
        use std::os::unix::fs::PermissionsExt as _;

        let fixture = tempfile::tempdir().expect("SSH materialization tempdir");
        let source = fixture.path().join("source");
        let remote = fixture.path().join("private.git");
        let work_root = fixture.path().join("runs");
        let run_dir = work_root.join(Uuid::new_v4().to_string());
        for directory in [&source, &run_dir] {
            std::fs::create_dir_all(directory).expect("fixture directory");
        }
        git(&source, ["init", "--initial-branch=main"]);
        git(&source, ["config", "user.email", "gitzero@example.test"]);
        git(&source, ["config", "user.name", "GitZero Test"]);
        std::fs::write(source.join("private.txt"), "SSH fixture\n").expect("fixture file");
        git(&source, ["add", "private.txt"]);
        git(&source, ["commit", "-m", "private fixture"]);
        git(
            fixture.path(),
            ["init", "--bare", remote.to_str().expect("remote path")],
        );
        git(
            &source,
            [
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path"),
            ],
        );
        git(&source, ["push", "origin", "main"]);
        git(&remote, ["symbolic-ref", "HEAD", "refs/heads/main"]);

        let fake_ssh = fixture.path().join("fake ssh");
        let ssh_log = fixture.path().join("ssh.log");
        std::fs::write(
            &fake_ssh,
            "#!/bin/sh\nprintf 'invoked\\n' >> \"$GITZERO_TEST_SSH_LOG\"\nexec git-upload-pack \"$GITZERO_TEST_SSH_REMOTE\"\n",
        )
        .expect("fake SSH client");
        std::fs::set_permissions(&fake_ssh, std::fs::Permissions::from_mode(0o700))
            .expect("fake SSH permissions");
        let ssh_environment = BTreeMap::from([
            (
                "GIT_SSH_COMMAND".to_owned(),
                shell_words::quote(&fake_ssh.to_string_lossy()).into_owned(),
            ),
            ("GIT_SSH_VARIANT".to_owned(), "ssh".to_owned()),
            (
                "GITZERO_TEST_SSH_LOG".to_owned(),
                ssh_log.display().to_string(),
            ),
            (
                "GITZERO_TEST_SSH_REMOTE".to_owned(),
                remote.display().to_string(),
            ),
        ]);
        let canonical_remote = "https://github.com/acme/private.git";
        let selected_remote = "deploy-user@github.com:acme/private.git";
        let (outbound, _incoming) = mpsc::channel(64);
        let (_cancel_tx, cancel) = watch::channel(false);

        let first = materialize_remote_repository_with_fetch(
            Uuid::new_v4(),
            "0/ssh-checkout",
            "acme",
            "private",
            "main",
            canonical_remote,
            selected_remote,
            RemoteRepositoryMaterializationScope::CheckoutSsh,
            None,
            Some(&ssh_environment),
            &run_dir,
            &cancel,
            &outbound,
            &Arc::new(AtomicU64::new(0)),
        )
        .await
        .expect("first SSH materialization");
        assert_eq!(
            std::fs::read_to_string(first.join("private.txt")).expect("materialized file"),
            "SSH fixture\n"
        );
        let cache = work_root.join("_action-cache/acme/private.git");
        assert_eq!(
            git_output(&cache, ["config", "--get", "remote.origin.url"]),
            canonical_remote
        );
        assert_eq!(
            std::fs::read_to_string(&ssh_log)
                .expect("fake SSH invocation log")
                .lines()
                .count(),
            2,
            "first SSH materialization must detect the object format before fetching"
        );

        let second = materialize_remote_repository_with_fetch(
            Uuid::new_v4(),
            "1/ssh-checkout",
            "acme",
            "private",
            "main",
            canonical_remote,
            selected_remote,
            RemoteRepositoryMaterializationScope::CheckoutSsh,
            None,
            Some(&ssh_environment),
            &run_dir,
            &cancel,
            &outbound,
            &Arc::new(AtomicU64::new(0)),
        )
        .await
        .expect("second SSH materialization");
        assert_eq!(
            std::fs::read_to_string(second.join("private.txt")).expect("rematerialized file"),
            "SSH fixture\n"
        );
        assert_eq!(
            std::fs::read_to_string(&ssh_log)
                .expect("fake SSH invocation log")
                .lines()
                .count(),
            3,
            "a second purpose did not prove SSH access before consuming the pinned ref"
        );
    }

    #[test]
    fn checkout_accepts_only_builtin_anonymous_or_exact_managed_credentials() {
        let managed = BTreeSet::from(["managed-secret".to_owned()]);
        assert_eq!(
            select_checkout_token(None, "builtin", &managed).expect("default token"),
            "builtin"
        );
        assert_eq!(
            select_checkout_token(Some("builtin"), "builtin", &managed)
                .expect("explicit built-in token"),
            "builtin"
        );
        assert_eq!(
            select_checkout_token(Some(""), "builtin", &managed).expect("anonymous checkout"),
            ""
        );
        assert_eq!(
            select_checkout_token(Some("managed-secret"), "builtin", &managed)
                .expect("managed secret token"),
            "managed-secret"
        );
        let error = select_checkout_token(Some("custom-secret"), "builtin", &managed)
            .expect_err("arbitrary token should fail");
        let message = format!("{error:#}");
        assert!(message.contains("exact managed secret value"));
        assert!(!message.contains("custom-secret"));
        assert!(
            select_checkout_token(Some("managed-secret-suffix"), "builtin", &managed).is_err(),
            "a transformed secret value was accepted"
        );
        assert_eq!(
            select_checkout_ssh_key(None, &managed).expect("missing SSH key"),
            None
        );
        assert_eq!(
            select_checkout_ssh_key(Some(""), &managed).expect("empty SSH key"),
            None
        );
        assert_eq!(
            select_checkout_ssh_key(Some("managed-secret"), &managed).expect("managed SSH key"),
            Some("managed-secret")
        );
        let error = select_checkout_ssh_key(Some("custom-private-key"), &managed)
            .expect_err("arbitrary SSH key should fail");
        let message = format!("{error:#}");
        assert!(message.contains("exact managed secret value"));
        assert!(!message.contains("custom-private-key"));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn checkout_ssh_credentials_are_private_bounded_and_persist_only_on_request() {
        use std::os::unix::fs::PermissionsExt as _;

        let fixture = tempfile::tempdir().expect("SSH credential tempdir");
        let runner_temp = fixture.path().join("runner temp");
        std::fs::create_dir_all(&runner_temp).expect("runner temp directory");
        let key =
            "-----BEGIN OPENSSH PRIVATE KEY-----\nfixture-key\n-----END OPENSSH PRIVATE KEY-----";
        let inputs = BTreeMap::from([
            ("ssh-key".to_owned(), key.to_owned()),
            (
                "ssh-known-hosts".to_owned(),
                "example.test ssh-ed25519 fixture-host-key".to_owned(),
            ),
            ("ssh-strict".to_owned(), "true".to_owned()),
            ("ssh-user".to_owned(), "deploy-user".to_owned()),
        ]);
        let managed = BTreeSet::from([key.to_owned()]);
        let environment =
            BTreeMap::from([("RUNNER_TEMP".to_owned(), runner_temp.display().to_string())]);

        let credentials = checkout_ssh_credentials(&inputs, &managed, &environment)
            .await
            .expect("checkout SSH credentials")
            .expect("configured SSH credentials");
        assert_eq!(credentials.user, "deploy-user");
        assert!(credentials.command.contains("StrictHostKeyChecking=yes"));
        assert!(credentials.command.contains("CheckHostIP=no"));
        assert!(!credentials.command.contains("fixture-key"));
        let credential_directory = runner_temp.join("_checkout-credentials");
        let paths = std::fs::read_dir(&credential_directory)
            .expect("credential directory")
            .map(|entry| entry.expect("credential entry").path())
            .collect::<Vec<_>>();
        assert_eq!(paths.len(), 2);
        for path in &paths {
            assert_eq!(
                std::fs::metadata(path)
                    .expect("credential metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        let key_path = paths
            .iter()
            .find(|path| path.extension().is_some_and(|extension| extension == "key"))
            .expect("private key path");
        assert_eq!(
            std::fs::read_to_string(key_path).expect("private key"),
            format!("{key}\n")
        );
        let known_hosts_path = paths
            .iter()
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "known_hosts")
            })
            .expect("known hosts path");
        let known_hosts = std::fs::read_to_string(known_hosts_path).expect("known hosts");
        assert!(known_hosts.contains("example.test ssh-ed25519 fixture-host-key"));
        assert!(known_hosts.contains(GITHUB_SSH_RSA_KNOWN_HOST));

        let mut job_environment = BTreeMap::new();
        configure_checkout_ssh(&mut job_environment, Some(&credentials.command));
        assert_eq!(
            job_environment.get("GIT_SSH_COMMAND"),
            Some(&credentials.command)
        );
        configure_checkout_ssh(&mut job_environment, None);
        assert!(!job_environment.contains_key("GIT_SSH_COMMAND"));

        {
            let _guard = SensitiveDirectoryGuard(credential_directory.clone());
        }
        assert!(!credential_directory.exists());

        for (name, value) in [
            ("ssh-user", "-option".to_owned()),
            (
                "ssh-known-hosts",
                "x".repeat(MAX_CHECKOUT_SSH_KNOWN_HOSTS_BYTES + 1),
            ),
            ("ssh-strict", "sometimes".to_owned()),
        ] {
            let mut invalid = inputs.clone();
            invalid.insert(name.to_owned(), value);
            assert!(
                checkout_ssh_credentials(&invalid, &managed, &environment)
                    .await
                    .is_err(),
                "accepted invalid {name}"
            );
        }
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn same_repository_checkout_persists_ssh_for_later_steps_and_keeps_exact_snapshot() {
        let fixture = tempfile::tempdir().expect("SSH checkout tempdir");
        let source = fixture.path().join("source");
        let run_dir = fixture.path().join("runs").join(Uuid::new_v4().to_string());
        let workspace = run_dir.join("workspace");
        let runner_temp = run_dir.join("_temp/job-ssh");
        for directory in [&source, &run_dir, &workspace, &runner_temp] {
            std::fs::create_dir_all(directory).expect("fixture directory");
        }
        git(&source, ["init", "--initial-branch=main"]);
        git(&source, ["config", "user.email", "gitzero@example.test"]);
        git(&source, ["config", "user.name", "GitZero Test"]);
        std::fs::write(source.join("README.md"), "exact SSH snapshot\n").expect("fixture file");
        git(&source, ["add", "README.md"]);
        git(&source, ["commit", "-m", "SSH fixture"]);
        let commit = git_output(&source, ["rev-parse", "HEAD"]);

        let mut run = fixture_run(
            Uuid::new_v4(),
            commit.clone(),
            commit.clone(),
            "https://github.com/local/fixture.git".to_owned(),
        );
        run.pull_request.merge_sha = commit.clone();
        let private_key =
            "-----BEGIN OPENSSH PRIVATE KEY-----\nfixture-key\n-----END OPENSSH PRIVATE KEY-----";
        let inputs = BTreeMap::from([
            ("ssh-key".to_owned(), private_key.to_owned()),
            ("ssh-user".to_owned(), "deploy".to_owned()),
            ("persist-credentials".to_owned(), "true".to_owned()),
            ("show-progress".to_owned(), "false".to_owned()),
        ]);
        let managed = BTreeSet::from([private_key.to_owned()]);
        let mut environment =
            BTreeMap::from([("RUNNER_TEMP".to_owned(), runner_temp.display().to_string())]);
        let repository = checkout_repository(None, &run).expect("same repository");
        let (outbound, _incoming) = mpsc::channel(64);
        let (_cancel_tx, cancel) = watch::channel(false);
        let workflow_commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::default()));
        let repository_access = local_repository_access(&workflow_commands);

        let outputs = execute_checkout_step_with_repository(
            run.id,
            "0/ssh-checkout",
            &inputs,
            &run,
            &source,
            &workspace,
            &run_dir,
            &mut environment,
            "",
            repository,
            None,
            &cancel,
            &outbound,
            &Arc::new(AtomicU64::new(0)),
            &repository_access,
            &managed,
        )
        .await
        .expect("same-repository SSH checkout");
        assert_eq!(outputs["commit"], commit);
        assert_eq!(
            git_output(&workspace, ["config", "--get", "remote.origin.url"]),
            "deploy@github.com:local/fixture.git"
        );
        let command = environment
            .get("GIT_SSH_COMMAND")
            .expect("persisted SSH command");
        assert!(!command.contains("fixture-key"));
        assert!(runner_temp.join("_checkout-credentials").is_dir());

        let credential_directory = runner_temp.join("_checkout-credentials");
        {
            let _guard = SensitiveDirectoryGuard(credential_directory.clone());
        }
        assert!(!credential_directory.exists());
    }

    #[test]
    fn cross_repository_checkout_validates_access_modes_and_repository_identity() {
        let run = fixture_run(
            Uuid::nil(),
            "a".repeat(40),
            "b".repeat(40),
            "https://github.com/acme/widget.git".to_owned(),
        );
        let same = checkout_repository(None, &run).expect("default repository");
        assert!(same.same_repository);
        assert_eq!(same.clone_url, run.repository.clone_url);
        let same_case = checkout_repository(Some(&"LOCAL/FIXTURE".to_owned()), &run)
            .expect("case-insensitive same repository");
        assert!(same_case.same_repository);

        let public = checkout_repository(Some(&"octocat/Hello-World".to_owned()), &run)
            .expect("public repository");
        assert_eq!(
            public,
            CheckoutRepository {
                owner: "octocat".to_owned(),
                name: "Hello-World".to_owned(),
                clone_url: "https://github.com/octocat/Hello-World.git".to_owned(),
                same_repository: false,
            }
        );
        for invalid in [
            "",
            "owner",
            "owner/repository/extra",
            "-owner/repository",
            "owner-/repository",
            "owner/repository name",
        ] {
            assert!(
                checkout_repository(Some(&invalid.to_owned()), &run).is_err(),
                "accepted invalid repository {invalid:?}"
            );
        }

        let managed = BTreeSet::from(["private-token".to_owned()]);
        assert_eq!(
            select_cross_repository_checkout_access(None, "builtin", &managed)
                .expect("implicit managed fallback"),
            CrossRepositoryCheckoutAccess::ManagedFallback
        );
        assert_eq!(
            select_cross_repository_checkout_access(Some("builtin"), "builtin", &managed)
                .expect("explicit built-in managed fallback"),
            CrossRepositoryCheckoutAccess::ManagedFallback
        );
        assert_eq!(
            select_cross_repository_checkout_access(Some(""), "builtin", &managed)
                .expect("explicit anonymous checkout"),
            CrossRepositoryCheckoutAccess::AnonymousOnly
        );
        assert_eq!(
            select_cross_repository_checkout_access(Some("private-token"), "builtin", &managed)
                .expect("explicit managed credential"),
            CrossRepositoryCheckoutAccess::ExplicitManagedToken("private-token".to_owned())
        );
        let error =
            select_cross_repository_checkout_access(Some("unknown-token"), "builtin", &managed)
                .expect_err("arbitrary token should fail");
        let message = format!("{error:#}");
        assert!(message.contains("another repository"));
        assert!(!message.contains("unknown-token"));
    }

    #[test]
    fn cross_repository_checkout_refs_are_bounded_and_safe() {
        for (input, expected) in [
            ("", "HEAD"),
            ("main", "main"),
            ("refs/tags/v1.2.3", "refs/tags/v1.2.3"),
            (
                "0123456789012345678901234567890123456789",
                "0123456789012345678901234567890123456789",
            ),
        ] {
            assert_eq!(
                public_checkout_ref(input).expect("valid public ref"),
                expected
            );
        }
        for invalid in [
            "-main",
            "refs/heads/with space",
            "refs/heads/a..b",
            "refs/heads/a.lock",
            "refs/heads/a:b",
            "refs/heads/trailing/",
        ] {
            assert!(
                public_checkout_ref(invalid).is_err(),
                "accepted invalid public ref {invalid:?}"
            );
        }
        assert!(public_checkout_ref(&"x".repeat(MAX_CHECKOUT_REF_BYTES + 1)).is_err());
        assert_eq!(public_checkout_output_ref("main", Some("main")), "main");
        assert_eq!(
            public_checkout_output_ref("HEAD", Some("trunk")),
            "refs/heads/trunk"
        );
        assert_eq!(
            public_checkout_output_ref("0123456789012345678901234567890123456789", None),
            ""
        );
        assert_eq!(
            parse_cross_repository_checkout_branch(
                "HEAD",
                "ref: refs/heads/trunk\tHEAD\n0123456789012345678901234567890123456789\tHEAD\n"
            )
            .expect("default branch advertisement"),
            Some("trunk".to_owned())
        );
        assert_eq!(
            parse_cross_repository_checkout_branch(
                "release",
                "0123456789012345678901234567890123456789\trefs/heads/release\n\
                 0123456789012345678901234567890123456789\trefs/tags/release\n"
            )
            .expect("branch and tag advertisement"),
            Some("release".to_owned())
        );
        assert_eq!(
            parse_cross_repository_checkout_branch(
                "v1.2.3",
                "0123456789012345678901234567890123456789\trefs/tags/v1.2.3\n"
            )
            .expect("tag advertisement"),
            None
        );
    }

    #[test]
    fn checkout_credentials_can_be_used_without_persisting_them() {
        let mut job_environment = BTreeMap::new();
        let mut checkout_environment = job_environment.clone();
        configure_checkout_credentials(&mut checkout_environment, "built-in-token", false)
            .expect("configure checkout credentials");
        configure_checkout_credentials(&mut job_environment, "", false)
            .expect("leave checkout credentials ephemeral");

        assert_eq!(checkout_environment["GIT_CONFIG_COUNT"], "1");
        assert_eq!(
            checkout_environment["GIT_CONFIG_KEY_0"],
            "http.https://github.com/.extraheader"
        );
        assert!(checkout_environment["GIT_CONFIG_VALUE_0"].starts_with("AUTHORIZATION: basic "));
        assert!(!job_environment.contains_key("GIT_CONFIG_COUNT"));

        configure_checkout_credentials(&mut job_environment, "built-in-token", false)
            .expect("persist checkout credentials");
        assert!(job_environment.contains_key("GIT_CONFIG_VALUE_0"));
        configure_checkout_credentials(&mut job_environment, "", false)
            .expect("remove checkout credentials");
        assert!(!job_environment.contains_key("GIT_CONFIG_VALUE_0"));
    }

    #[tokio::test]
    async fn checkout_safe_directory_uses_an_isolated_global_configuration() {
        let fixture = tempfile::tempdir().expect("safe directory config tempdir");
        let original_home = fixture.path().join("original-home");
        let runner_temp = fixture.path().join("runner-temp");
        let run_dir = fixture.path().join("run");
        let directory = fixture.path().join("workspace repository");
        for path in [&original_home, &runner_temp, &run_dir] {
            std::fs::create_dir_all(path).expect("fixture directory");
        }
        let original_config = original_home.join(".gitconfig");
        let original_contents = "[core]\n\tquotePath = false\n";
        std::fs::write(&original_config, original_contents).expect("original global Git config");
        let job_environment = BTreeMap::from([
            ("HOME".to_owned(), original_home.display().to_string()),
            ("RUNNER_TEMP".to_owned(), runner_temp.display().to_string()),
            ("GIT_CONFIG_COUNT".to_owned(), "1".to_owned()),
            ("GIT_CONFIG_KEY_0".to_owned(), "core.fileMode".to_owned()),
            ("GIT_CONFIG_VALUE_0".to_owned(), "false".to_owned()),
        ]);
        let mut checkout_environment = job_environment.clone();
        let guard = configure_checkout_safe_directory(
            &mut checkout_environment,
            &directory,
            &run_dir,
            true,
        )
        .await
        .expect("configure checkout safe directory")
        .expect("temporary Git home guard");
        configure_checkout_credentials(&mut checkout_environment, "built-in-token", true)
            .expect("compose checkout credentials");

        let temporary_home = PathBuf::from(&checkout_environment["HOME"]);
        assert_ne!(temporary_home, original_home);
        assert!(temporary_home.starts_with(&runner_temp));
        assert_eq!(
            checkout_environment["GIT_CONFIG_GLOBAL"],
            temporary_home.join(".gitconfig").display().to_string()
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(temporary_home.join(".gitconfig"))
                    .expect("temporary global Git config metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        let global = std::process::Command::new("git")
            .args(["config", "--global", "--get-regexp", ".*"])
            .envs(checkout_environment.iter())
            .output()
            .expect("inspect temporary global Git config");
        assert!(global.status.success());
        let global = String::from_utf8(global.stdout).expect("global Git config UTF-8");
        assert!(global.contains("core.quotepath false"));
        assert!(global.contains(&format!("safe.directory {}", directory.display())));
        assert_eq!(
            std::fs::read_to_string(&original_config).expect("unchanged original config"),
            original_contents
        );
        assert_eq!(job_environment["HOME"], original_home.display().to_string());
        assert!(!job_environment.contains_key("GIT_CONFIG_GLOBAL"));

        drop(guard);
        assert!(!temporary_home.exists());
        let mut disabled_environment = job_environment.clone();
        assert!(
            configure_checkout_safe_directory(
                &mut disabled_environment,
                &directory,
                &run_dir,
                false,
            )
            .await
            .expect("disable checkout safe directory")
            .is_none()
        );
        assert_eq!(disabled_environment, job_environment);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn checkout_composes_https_submodule_rewrites_with_existing_git_configuration() {
        let mut environment = BTreeMap::from([
            ("GIT_CONFIG_COUNT".to_owned(), "1".to_owned()),
            ("GIT_CONFIG_KEY_0".to_owned(), "core.quotePath".to_owned()),
            ("GIT_CONFIG_VALUE_0".to_owned(), "false".to_owned()),
        ]);
        configure_checkout_credentials(&mut environment, "managed-token", true)
            .expect("compose checkout configuration");

        assert_eq!(environment["GIT_CONFIG_COUNT"], "3");
        assert_eq!(environment["GIT_CONFIG_KEY_0"], "core.quotePath");
        assert_eq!(environment["GIT_CONFIG_VALUE_0"], "false");
        assert_eq!(environment["GIT_CONFIG_KEY_1"], CHECKOUT_HTTP_EXTRAHEADER);
        assert!(environment["GIT_CONFIG_VALUE_1"].starts_with("AUTHORIZATION: basic "));
        assert_eq!(environment["GIT_CONFIG_KEY_2"], CHECKOUT_SSH_INSTEAD_OF);
        assert_eq!(environment["GIT_CONFIG_VALUE_2"], "git@github.com:");

        let output = std::process::Command::new("git")
            .args([
                "ls-remote",
                "--get-url",
                "git@github.com:acme/dependency.git",
            ])
            .envs(environment.iter())
            .output()
            .expect("resolve rewritten SSH submodule URL");
        assert!(output.status.success());
        assert_eq!(
            String::from_utf8(output.stdout)
                .expect("rewritten URL UTF-8")
                .trim(),
            "https://github.com/acme/dependency.git"
        );

        configure_checkout_credentials(&mut environment, "", false)
            .expect("remove managed checkout configuration");
        assert_eq!(environment["GIT_CONFIG_COUNT"], "1");
        assert_eq!(environment["GIT_CONFIG_KEY_0"], "core.quotePath");
        assert!(!environment.values().any(|value| value == "managed-token"));
        assert!(
            !environment
                .values()
                .any(|value| value == CHECKOUT_HTTP_EXTRAHEADER)
        );
        assert!(
            !environment
                .values()
                .any(|value| value == CHECKOUT_SSH_INSTEAD_OF)
        );
        let output = std::process::Command::new("git")
            .args([
                "ls-remote",
                "--get-url",
                "git@github.com:acme/dependency.git",
            ])
            .envs(environment.iter())
            .output()
            .expect("resolve SSH submodule URL without HTTPS rewrite");
        assert!(output.status.success());
        assert_eq!(
            String::from_utf8(output.stdout)
                .expect("SSH URL UTF-8")
                .trim(),
            "git@github.com:acme/dependency.git"
        );

        let mut bounded = BTreeMap::from([(
            "GIT_CONFIG_COUNT".to_owned(),
            MAX_INLINE_GIT_CONFIG_PARAMETERS.to_string(),
        )]);
        for index in 0..MAX_INLINE_GIT_CONFIG_PARAMETERS {
            bounded.insert(
                format!("GIT_CONFIG_KEY_{index}"),
                format!("test.key{index}"),
            );
            bounded.insert(format!("GIT_CONFIG_VALUE_{index}"), index.to_string());
        }
        configure_checkout_credentials(&mut bounded, "bounded-token", true)
            .expect("compose bounded checkout configuration");
        assert_eq!(
            bounded["GIT_CONFIG_COUNT"],
            (MAX_INLINE_GIT_CONFIG_PARAMETERS + 2).to_string()
        );
        assert!(
            bounded
                .values()
                .any(|value| value == CHECKOUT_HTTP_EXTRAHEADER)
        );
        assert!(
            bounded
                .values()
                .any(|value| value == CHECKOUT_SSH_INSTEAD_OF)
        );
        configure_checkout_credentials(&mut bounded, "", false)
            .expect("remove bounded checkout configuration");
        assert_eq!(
            bounded["GIT_CONFIG_COUNT"],
            MAX_INLINE_GIT_CONFIG_PARAMETERS.to_string()
        );
        assert!(
            !bounded
                .values()
                .any(|value| value.starts_with("AUTHORIZATION: basic "))
        );
        assert!(
            !bounded
                .values()
                .any(|value| value == CHECKOUT_HTTP_EXTRAHEADER)
        );
        assert!(
            !bounded
                .values()
                .any(|value| value == CHECKOUT_SSH_INSTEAD_OF)
        );

        let mut invalid = BTreeMap::from([(
            "GIT_CONFIG_COUNT".to_owned(),
            (MAX_INLINE_GIT_CONFIG_PARAMETERS + 3).to_string(),
        )]);
        let original = invalid.clone();
        assert!(configure_checkout_credentials(&mut invalid, "secret", true).is_err());
        assert_eq!(
            invalid, original,
            "invalid inline configuration was mutated"
        );
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn checkout_submodules_are_synced_forced_shallow_and_recursive() {
        let fixture = tempfile::tempdir().expect("submodule checkout tempdir");
        let child = fixture.path().join("child");
        let source = fixture.path().join("source");
        let run_dir = fixture.path().join("runs").join(Uuid::new_v4().to_string());
        let workspace = run_dir.join("workspace");
        let trace = fixture.path().join("git-trace.json");
        for directory in [&child, &source, &run_dir, &workspace] {
            std::fs::create_dir_all(directory).expect("fixture directory");
        }

        git(&child, ["init", "--initial-branch=main"]);
        git(&child, ["config", "user.email", "gitzero@example.test"]);
        git(&child, ["config", "user.name", "GitZero Test"]);
        std::fs::write(child.join("child.txt"), "pinned child\n").expect("child fixture file");
        git(&child, ["add", "child.txt"]);
        git(&child, ["commit", "-m", "child fixture"]);
        let child_commit = git_output(&child, ["rev-parse", "HEAD"]);

        git(&source, ["init", "--initial-branch=main"]);
        git(&source, ["config", "user.email", "gitzero@example.test"]);
        git(&source, ["config", "user.name", "GitZero Test"]);
        std::fs::write(source.join("README.md"), "root fixture\n").expect("root fixture file");
        git(&source, ["add", "README.md"]);
        git(&source, ["commit", "-m", "root fixture"]);
        let child_url = format!("file://{}", child.display());
        let status = std::process::Command::new("git")
            .args([
                "-c",
                "protocol.file.allow=always",
                "submodule",
                "add",
                &child_url,
                "deps/child",
            ])
            .current_dir(&source)
            .status()
            .expect("add fixture submodule");
        assert!(status.success());
        git(&source, ["commit", "-am", "add fixture submodule"]);
        let root_commit = git_output(&source, ["rev-parse", "HEAD"]);

        let mut run = fixture_run(
            Uuid::new_v4(),
            root_commit.clone(),
            root_commit.clone(),
            "https://github.com/local/fixture.git".to_owned(),
        );
        run.pull_request.merge_sha = root_commit.clone();
        let inputs = BTreeMap::from([
            ("fetch-depth".to_owned(), "1".to_owned()),
            ("persist-credentials".to_owned(), "false".to_owned()),
            ("show-progress".to_owned(), "false".to_owned()),
            ("submodules".to_owned(), "recursive".to_owned()),
        ]);
        let mut environment = BTreeMap::from([
            ("GIT_ALLOW_PROTOCOL".to_owned(), "file".to_owned()),
            ("GIT_TRACE2_EVENT".to_owned(), trace.display().to_string()),
        ]);
        let repository = checkout_repository(None, &run).expect("same repository");
        let (outbound, mut incoming) = mpsc::channel(256);
        let drain = tokio::spawn(async move { while incoming.recv().await.is_some() {} });
        let (_cancel_tx, cancel) = watch::channel(false);
        let workflow_commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::default()));
        let repository_access = local_repository_access(&workflow_commands);

        let outputs = execute_checkout_step_with_repository(
            run.id,
            "0/submodules",
            &inputs,
            &run,
            &source,
            &workspace,
            &run_dir,
            &mut environment,
            "",
            repository,
            None,
            &cancel,
            &outbound,
            &Arc::new(AtomicU64::new(0)),
            &repository_access,
            &BTreeSet::new(),
        )
        .await
        .expect("checkout with recursive submodules");
        drop(outbound);
        drain.await.expect("drain checkout events");

        assert_eq!(outputs["commit"], root_commit);
        assert_eq!(
            std::fs::read_to_string(workspace.join("deps/child/child.txt"))
                .expect("checked out submodule file"),
            "pinned child\n"
        );
        assert_eq!(
            git_output(&workspace.join("deps/child"), ["rev-parse", "HEAD"]),
            child_commit
        );
        assert_eq!(
            git_output(
                &workspace.join("deps/child"),
                ["config", "--get", "gc.auto"]
            ),
            "0"
        );
        assert_eq!(
            git_output(
                &workspace.join("deps/child"),
                ["rev-parse", "--is-shallow-repository"]
            ),
            "true"
        );
        assert!(!environment.contains_key("GIT_CONFIG_COUNT"));

        let trace = std::fs::read_to_string(trace).expect("Git trace");
        assert!(trace.contains("submodule"));
        assert!(trace.contains("sync"));
        assert!(trace.contains("--force"));
        assert!(trace.contains("--depth=1"));
        assert!(trace.contains("--recursive"));
    }

    #[test]
    fn checkout_ref_aliases_select_only_authenticated_snapshots() {
        let mut run = fixture_run(
            Uuid::nil(),
            "a".repeat(40),
            "b".repeat(40),
            "https://github.com/local/fixture.git".to_owned(),
        );
        run.pull_request.merge_sha = "c".repeat(40);
        for git_ref in ["", &run.pull_request.merge_sha, "refs/pull/1/merge"] {
            assert_eq!(
                checkout_target(git_ref, &run),
                Some(CheckoutTarget::Execution),
                "rejected safe PR-merge alias {git_ref:?}"
            );
            assert_eq!(
                checkout_local_branch(git_ref, CheckoutTarget::Execution, &run)
                    .expect("valid merge checkout"),
                None
            );
        }
        for git_ref in [
            &run.pull_request.head_sha,
            "feature",
            "refs/heads/feature",
            "refs/pull/1/head",
        ] {
            assert_eq!(
                checkout_target(git_ref, &run),
                Some(CheckoutTarget::Head),
                "rejected safe PR-head alias {git_ref:?}"
            );
        }
        assert_eq!(
            checkout_local_branch("feature", CheckoutTarget::Head, &run)
                .expect("valid head branch"),
            Some("feature".to_owned())
        );
        assert_eq!(
            checkout_local_branch("refs/heads/feature", CheckoutTarget::Head, &run)
                .expect("valid fully qualified head branch"),
            Some("feature".to_owned())
        );
        for git_ref in [&run.pull_request.head_sha, "refs/pull/1/head"] {
            assert_eq!(
                checkout_local_branch(git_ref, CheckoutTarget::Head, &run)
                    .expect("valid detached head checkout"),
                None
            );
        }
        for git_ref in [&run.pull_request.base_sha, "main", "refs/heads/main"] {
            assert_eq!(
                checkout_target(git_ref, &run),
                Some(CheckoutTarget::Base),
                "rejected safe PR-base alias {git_ref:?}"
            );
        }
        assert_eq!(
            checkout_local_branch("main", CheckoutTarget::Base, &run).expect("valid base branch"),
            Some("main".to_owned())
        );
        assert_eq!(
            checkout_local_branch(&run.pull_request.base_sha, CheckoutTarget::Base, &run)
                .expect("valid detached base checkout"),
            None
        );
        for git_ref in [
            "refs/pull/2/head",
            "refs/pull/2/merge",
            "release",
            "refs/heads/release",
        ] {
            assert!(
                checkout_target(git_ref, &run).is_none(),
                "accepted unauthenticated ref {git_ref:?}"
            );
        }
        assert_eq!(checkout_output_ref(None, &run), "refs/pull/1/merge");
        assert_eq!(
            checkout_output_ref(Some(&run.pull_request.merge_sha), &run),
            ""
        );
        assert_eq!(
            checkout_output_ref(Some(&run.pull_request.head_sha), &run),
            ""
        );
        assert_eq!(
            checkout_output_ref(Some(&" feature ".to_owned()), &run),
            "feature"
        );
        assert_eq!(
            checkout_output_ref(Some(&run.pull_request.base_sha), &run),
            ""
        );
        assert_eq!(
            checkout_output_ref(Some(&" main ".to_owned()), &run),
            "main"
        );

        run.pull_request.action = "closed".to_owned();
        run.pull_request.execution_ref = "refs/heads/main".to_owned();
        assert!(validate_execution_ref(&run).is_ok());
        assert_eq!(execution_ref_name(&run), "main");
        assert_eq!(checkout_target("", &run), Some(CheckoutTarget::Execution));
        assert_eq!(
            checkout_target("refs/heads/main", &run),
            Some(CheckoutTarget::Execution)
        );
        assert_eq!(
            checkout_local_branch("", CheckoutTarget::Execution, &run)
                .expect("valid default closed branch"),
            Some("main".to_owned())
        );
        assert_eq!(
            checkout_local_branch("refs/heads/main", CheckoutTarget::Execution, &run)
                .expect("valid explicit closed branch"),
            Some("main".to_owned())
        );
        assert_eq!(checkout_output_ref(None, &run), "refs/heads/main");
        run.pull_request.execution_ref = "refs/heads/main\nforged".to_owned();
        assert!(validate_execution_ref(&run).is_err());
        run.pull_request.base_ref = ".hidden".to_owned();
        run.pull_request.execution_ref = "refs/heads/.hidden".to_owned();
        assert!(validate_execution_ref(&run).is_err());
        run.pull_request.base_ref = "topic.lock/child".to_owned();
        run.pull_request.execution_ref = "refs/heads/topic.lock/child".to_owned();
        assert!(validate_execution_ref(&run).is_err());
        run.pull_request.base_ref = "main".to_owned();
        run.pull_request.execution_ref = "refs/heads/main".to_owned();
        run.pull_request.action = "opened".to_owned();
        assert!(validate_execution_ref(&run).is_err());

        run.pull_request.head_ref = "-feature".to_owned();
        assert!(
            checkout_local_branch("-feature", CheckoutTarget::Head, &run).is_err(),
            "accepted an invalid local branch name"
        );
    }

    #[test]
    fn requires_checkout_tokens_and_expiries_as_a_pair() {
        let mut run = fixture_run(
            Uuid::nil(),
            "0".repeat(40),
            "1".repeat(40),
            "https://github.com/octocat/Hello-World.git".to_owned(),
        );
        assert!(validate_checkout_token_metadata(&run).is_ok());

        run.checkout_token = "source-token".to_owned();
        assert!(validate_checkout_token_metadata(&run).is_err());
        run.checkout_token_expires_at_epoch_seconds = Some(4_102_444_800);
        assert!(validate_checkout_token_metadata(&run).is_ok());

        run.checkout_token.clear();
        assert!(validate_checkout_token_metadata(&run).is_err());
    }

    #[test]
    fn checkout_filter_and_sparse_inputs_are_bounded_and_normalized() {
        assert_eq!(
            checkout_filter(Some(&" blob:none ".to_owned())).expect("filter"),
            Some("blob:none".to_owned())
        );
        assert!(checkout_filter(Some(&"blob:\nnone".to_owned())).is_err());
        assert!(checkout_filter(Some(&"x".repeat(MAX_CHECKOUT_FILTER_BYTES + 1))).is_err());

        assert_eq!(
            checkout_sparse_patterns(Some(&"\n .github \n src/app \n !docs \n".to_owned()))
                .expect("sparse patterns"),
            Some(vec![
                ".github".to_owned(),
                "src/app".to_owned(),
                "!docs".to_owned(),
            ])
        );
        assert_eq!(
            checkout_sparse_patterns(Some(&" \n\n".to_owned())).expect("empty patterns"),
            None
        );
        assert!(
            checkout_sparse_patterns(Some(&format!(
                "{}\0",
                "x".repeat(MAX_SPARSE_CHECKOUT_BYTES - 1)
            )))
            .is_err()
        );
    }

    #[test]
    fn masks_checkout_and_named_environment_secrets() {
        let mut run = fixture_run(
            Uuid::nil(),
            "0".repeat(40),
            "0".repeat(40),
            "https://github.com/octocat/Hello-World.git".to_owned(),
        );
        run.checkout_token = "checkout-token-value".to_owned();
        run.checkout_token_expires_at_epoch_seconds = Some(4_102_444_800);
        run.environment
            .insert("DEPLOY_SECRET".to_owned(), "environment-secret".to_owned());
        let secrets = secret_values(&run);
        let mut log = "checkout-token-value and environment-secret".to_owned();
        mask_text(&mut log, &secrets);
        assert_eq!(log, "*** and ***");
    }

    #[test]
    fn masks_dynamically_minted_shared_repository_tokens() {
        let commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::default()));
        let token = "shared-repository-token-value";
        register_repository_token_masks(&commands, token);
        let credential = STANDARD.encode(format!("x-access-token:{token}"));
        let mut log = format!("{token} {credential} AUTHORIZATION: basic {credential}");
        let commands = commands
            .lock()
            .expect("workflow command processor was poisoned");
        commands.mask_for_step("0/job/step", &mut log);
        assert!(!log.contains(token));
        assert!(!log.contains(&credential));
        assert!(commands.value_is_masked_for_step("0/job/step", token));
    }

    #[tokio::test]
    async fn repository_token_cache_is_separated_by_purpose() {
        let commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::default()));
        let (outbound, mut events) = mpsc::channel(4);
        let client = RepositoryAccessClient::remote(outbound);
        let access = RunRepositoryAccess::new(client.clone(), commands.clone());
        let run_id = Uuid::new_v4();
        let (_cancel_tx, cancel) = watch::channel(false);

        for (purpose, granted_token) in [
            (RepositoryTokenPurpose::Source, "source-token"),
            (RepositoryTokenPurpose::Environment, "environment-token"),
            (RepositoryTokenPurpose::SharedSource, "shared-source-token"),
            (RepositoryTokenPurpose::Checkout, "private-checkout-token"),
        ] {
            let request = {
                let access = access.clone();
                let cancel = cancel.clone();
                tokio::spawn(async move {
                    access
                        .request_token(run_id, purpose, "acme", "private-tools", false, &cancel)
                        .await
                })
            };
            let request_id = match events.recv().await.expect("repository token request") {
                AgentMessage::RepositoryTokenRequest {
                    request_id,
                    purpose: requested_purpose,
                    ..
                } => {
                    assert_eq!(requested_purpose, purpose);
                    request_id
                }
                message => panic!("unexpected event: {message:?}"),
            };
            client
                .handle_granted(request_id, granted_token.to_owned(), 4_102_444_800)
                .await;
            assert_eq!(
                request.await.expect("join token request").expect("token"),
                granted_token
            );
        }

        assert_eq!(
            access
                .cached_token(RepositoryTokenPurpose::Source, "acme", "private-tools")
                .await
                .as_deref(),
            Some("source-token")
        );
        assert_eq!(
            access
                .cached_token(RepositoryTokenPurpose::Environment, "acme", "private-tools",)
                .await
                .as_deref(),
            Some("environment-token")
        );
        assert_eq!(
            access
                .cached_token(
                    RepositoryTokenPurpose::SharedSource,
                    "ACME",
                    "PRIVATE-TOOLS",
                )
                .await
                .as_deref(),
            Some("shared-source-token")
        );
        assert_eq!(
            access
                .cached_token(RepositoryTokenPurpose::Checkout, "acme", "private-tools")
                .await
                .as_deref(),
            Some("private-checkout-token")
        );
        assert!(events.try_recv().is_err());
        let key = (
            RepositoryTokenPurpose::SharedSource,
            repository_access_key("acme", "private-tools"),
        );
        access
            .tokens
            .lock()
            .await
            .get_mut(&key)
            .expect("cached shared-source token")
            .expires_at_epoch_seconds = 1;
        let refresh = {
            let access = access.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move {
                access
                    .request_token(
                        run_id,
                        RepositoryTokenPurpose::SharedSource,
                        "acme",
                        "private-tools",
                        false,
                        &cancel,
                    )
                    .await
            })
        };
        let refresh_request_id = match events.recv().await.expect("token refresh request") {
            AgentMessage::RepositoryTokenRequest {
                request_id,
                purpose,
                ..
            } => {
                assert_eq!(purpose, RepositoryTokenPurpose::SharedSource);
                request_id
            }
            message => panic!("unexpected event: {message:?}"),
        };
        client
            .handle_granted(
                refresh_request_id,
                "refreshed-shared-source-token".to_owned(),
                4_102_444_800,
            )
            .await;
        assert_eq!(
            refresh
                .await
                .expect("join refresh")
                .expect("refreshed token"),
            "refreshed-shared-source-token"
        );
        let mut log =
            "source-token environment-token refreshed-shared-source-token private-checkout-token"
                .to_owned();
        commands
            .lock()
            .expect("workflow command processor was poisoned")
            .mask_for_step("0/job/step", &mut log);
        assert_eq!(log, "*** *** *** ***");
    }

    #[tokio::test]
    async fn refreshes_an_expiring_initial_source_token() {
        let commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::default()));
        let (outbound, mut events) = mpsc::channel(4);
        let client = RepositoryAccessClient::remote(outbound);
        let access = RunRepositoryAccess::new(client.clone(), commands);
        let mut run = fixture_run(
            Uuid::new_v4(),
            "0".repeat(40),
            "1".repeat(40),
            "https://github.com/acme/widget.git".to_owned(),
        );
        run.installation_id = 42;
        run.repository.owner = "acme".to_owned();
        run.repository.name = "widget".to_owned();
        run.checkout_token = "initial-source-token".to_owned();
        run.checkout_token_expires_at_epoch_seconds = Some(4_102_444_800);
        let (_cancel_tx, cancel) = watch::channel(false);

        assert_eq!(
            access
                .source_token(&run, false, &cancel)
                .await
                .expect("fresh source token"),
            Some("initial-source-token".to_owned())
        );
        assert!(events.try_recv().is_err());

        run.checkout_token_expires_at_epoch_seconds = Some(1);
        let refresh = {
            let access = access.clone();
            let run = run.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move { access.source_token(&run, false, &cancel).await })
        };
        let request_id = match events.recv().await.expect("source refresh request") {
            AgentMessage::RepositoryTokenRequest {
                request_id,
                purpose,
                owner,
                repository,
                ..
            } => {
                assert_eq!(purpose, RepositoryTokenPurpose::Source);
                assert_eq!(owner, "acme");
                assert_eq!(repository, "widget");
                request_id
            }
            message => panic!("unexpected event: {message:?}"),
        };
        client
            .handle_granted(
                request_id,
                "refreshed-source-token".to_owned(),
                4_102_444_800,
            )
            .await;
        assert_eq!(
            refresh
                .await
                .expect("join source refresh")
                .expect("refreshed source token"),
            Some("refreshed-source-token".to_owned())
        );
    }

    #[tokio::test]
    async fn private_checkout_falls_back_with_checkout_purpose_after_anonymous_failure() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        let source = fixture.path().join("source");
        let remote = fixture.path().join("private.git");
        let run_dir = fixture.path().join("runs").join(Uuid::new_v4().to_string());
        let workspace = run_dir.join("workspace");
        let source_repository = run_dir.join("repository");
        for directory in [&source, &run_dir, &workspace, &source_repository] {
            std::fs::create_dir_all(directory).expect("fixture directory");
        }
        git(&source, ["init", "--initial-branch=main"]);
        git(&source, ["config", "user.email", "gitzero@example.test"]);
        git(&source, ["config", "user.name", "GitZero Test"]);
        std::fs::write(source.join("private.txt"), "managed private checkout\n")
            .expect("private fixture file");
        git(&source, ["add", "private.txt"]);
        git(&source, ["commit", "-m", "private fixture"]);
        let expected_commit = git_output(&source, ["rev-parse", "HEAD"]);
        git(
            fixture.path(),
            ["init", "--bare", remote.to_str().expect("remote path")],
        );
        git(
            &source,
            [
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path"),
            ],
        );
        git(&source, ["push", "origin", "main"]);

        let mut run = fixture_run(
            Uuid::new_v4(),
            "a".repeat(40),
            "b".repeat(40),
            "https://github.com/acme/widget.git".to_owned(),
        );
        run.repository.owner = "acme".to_owned();
        run.repository.name = "widget".to_owned();
        run.checkout_token = "source-repository-token".to_owned();
        run.checkout_token_expires_at_epoch_seconds = Some(4_102_444_800);
        let repository = CheckoutRepository {
            owner: "ACME".to_owned(),
            name: "private-tools".to_owned(),
            clone_url: remote.display().to_string(),
            same_repository: false,
        };
        let inputs = BTreeMap::from([
            ("ref".to_owned(), "main".to_owned()),
            ("show-progress".to_owned(), "false".to_owned()),
            ("token".to_owned(), run.checkout_token.clone()),
        ]);
        let anonymous_scope = run_dir.join("_actions/0/checkout-anonymous");
        std::fs::create_dir_all(anonymous_scope.parent().expect("scope parent"))
            .expect("anonymous scope parent");
        std::fs::write(&anonymous_scope, "force anonymous materialization failure")
            .expect("block anonymous scope");
        let commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::default()));
        let (control_outbound, mut control_events) = mpsc::channel(4);
        let client = RepositoryAccessClient::remote(control_outbound);
        let access = RunRepositoryAccess::new(client.clone(), commands.clone());
        let (log_outbound, mut log_events) = mpsc::channel(256);
        let (_cancel_tx, cancel) = watch::channel(false);
        let checkout = {
            let run = run.clone();
            let repository = repository.clone();
            let inputs = inputs.clone();
            let run_dir = run_dir.clone();
            let workspace = workspace.clone();
            let source_repository = source_repository.clone();
            let cancel = cancel.clone();
            let log_outbound = log_outbound.clone();
            let access = access.clone();
            tokio::spawn(async move {
                let mut environment = BTreeMap::new();
                let outputs = execute_checkout_step_with_repository(
                    run.id,
                    "0/private-checkout",
                    &inputs,
                    &run,
                    &source_repository,
                    &workspace,
                    &run_dir,
                    &mut environment,
                    &run.checkout_token,
                    repository,
                    None,
                    &cancel,
                    &log_outbound,
                    &Arc::new(AtomicU64::new(0)),
                    &access,
                    &BTreeSet::new(),
                )
                .await;
                (outputs, environment)
            })
        };

        let request_id = match control_events
            .recv()
            .await
            .expect("private checkout token request")
        {
            AgentMessage::RepositoryTokenRequest {
                job_id,
                request_id,
                purpose,
                owner,
                repository,
                ..
            } => {
                assert_eq!(job_id, run.id);
                assert_eq!(purpose, RepositoryTokenPurpose::Checkout);
                assert_eq!(owner, "ACME");
                assert_eq!(repository, "private-tools");
                request_id
            }
            message => panic!("unexpected event: {message:?}"),
        };
        client
            .handle_granted(
                request_id,
                "managed-private-token".to_owned(),
                4_102_444_800,
            )
            .await;
        let (outputs, environment) = checkout.await.expect("join private checkout");
        let outputs = outputs.expect("managed private checkout");
        assert_eq!(outputs["commit"], expected_commit);
        assert_eq!(
            std::fs::read_to_string(workspace.join("private.txt"))
                .expect("checked out private fixture"),
            "managed private checkout\n"
        );
        let credential = STANDARD.encode("x-access-token:managed-private-token");
        let expected_credential = format!("AUTHORIZATION: basic {credential}");
        assert_eq!(
            environment.get("GIT_CONFIG_VALUE_0").map(String::as_str),
            Some(expected_credential.as_str())
        );
        assert!(control_events.try_recv().is_err());
        while let Ok(message) = log_events.try_recv() {
            if let AgentMessage::LogChunk { data, .. } = message {
                assert!(!data.contains("managed-private-token"));
            }
        }
        assert!(
            commands
                .lock()
                .expect("workflow command processor was poisoned")
                .value_is_masked_for_step("0/private-checkout", "managed-private-token")
        );
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn anonymous_checkout_cannot_reuse_an_authorized_source_materialization() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        let source = fixture.path().join("source");
        let remote = fixture.path().join("private.git");
        let unavailable = fixture.path().join("private-unavailable.git");
        let run_dir = fixture.path().join("runs").join(Uuid::new_v4().to_string());
        std::fs::create_dir_all(&source).expect("source directory");
        std::fs::create_dir_all(&run_dir).expect("run directory");
        git(&source, ["init", "--initial-branch=main"]);
        git(&source, ["config", "user.email", "gitzero@example.test"]);
        git(&source, ["config", "user.name", "GitZero Test"]);
        std::fs::write(source.join("private.txt"), "private contents\n")
            .expect("private fixture file");
        git(&source, ["add", "private.txt"]);
        git(&source, ["commit", "-m", "private fixture"]);
        git(
            fixture.path(),
            ["init", "--bare", remote.to_str().expect("remote path")],
        );
        git(
            &source,
            [
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path"),
            ],
        );
        git(&source, ["push", "origin", "main"]);

        let mut run = fixture_run(
            Uuid::new_v4(),
            "a".repeat(40),
            "b".repeat(40),
            "https://github.com/acme/widget.git".to_owned(),
        );
        run.repository.owner = "acme".to_owned();
        run.repository.name = "widget".to_owned();
        let (outbound, _incoming) = mpsc::channel(256);
        let (_cancel_tx, cancel) = watch::channel(false);
        materialize_remote_repository(
            run.id,
            "0/job/private-action",
            "acme",
            "private-tools",
            "main",
            remote.to_str().expect("remote path"),
            RemoteRepositoryMaterializationScope::SharedSource,
            Some("shared-source-token"),
            &run_dir,
            &cancel,
            &outbound,
            &Arc::new(AtomicU64::new(0)),
        )
        .await
        .expect("authorized source materialization");
        std::fs::rename(&remote, &unavailable).expect("make private remote unavailable");

        let commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::default()));
        let access = local_repository_access(&commands);
        let repository = CheckoutRepository {
            owner: "acme".to_owned(),
            name: "private-tools".to_owned(),
            clone_url: remote.display().to_string(),
            same_repository: false,
        };
        let error = materialize_cross_repository_checkout_snapshot(
            run.id,
            "0/job/anonymous-checkout",
            &repository,
            "main",
            &run,
            &access,
            CrossRepositoryCheckoutAccess::AnonymousOnly,
            &run_dir,
            &cancel,
            &outbound,
            &Arc::new(AtomicU64::new(0)),
        )
        .await
        .expect_err("anonymous checkout must not reuse authorized source files");
        assert!(format!("{error:#}").contains("anonymously"));
    }

    #[tokio::test]
    async fn caches_and_masks_exact_scoped_workflow_tokens() {
        let commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::default()));
        let (outbound, mut events) = mpsc::channel(4);
        let client = RepositoryAccessClient::remote(outbound);
        let access = RunRepositoryAccess::new(client.clone(), commands.clone());
        let mut run = fixture_run(
            Uuid::new_v4(),
            "0".repeat(40),
            "1".repeat(40),
            "https://github.com/octocat/Hello-World.git".to_owned(),
        );
        run.installation_id = 42;
        run.checkout_token = "baseline-checkout-token".to_owned();
        run.checkout_token_expires_at_epoch_seconds = Some(4_102_444_800);
        let permissions = PlannedPermissions {
            read: BTreeSet::new(),
            write: BTreeSet::from(["checks".to_owned()]),
        };
        let (_cancel_tx, cancel) = watch::channel(false);
        let request = {
            let access = access.clone();
            let run = run.clone();
            let permissions = permissions.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move { access.workflow_token(&run, &permissions, &cancel).await })
        };
        let request_id = match events.recv().await.expect("workflow token request") {
            AgentMessage::WorkflowTokenRequest {
                job_id,
                request_id,
                read_permissions,
                write_permissions,
                ..
            } => {
                assert_eq!(job_id, run.id);
                assert!(read_permissions.is_empty());
                assert_eq!(write_permissions, ["checks"]);
                request_id
            }
            message => panic!("unexpected event: {message:?}"),
        };
        client
            .handle_granted(request_id, "checks-read-token".to_owned(), 4_102_444_800)
            .await;
        assert_eq!(
            request.await.expect("join token request").expect("token"),
            Some("checks-read-token".to_owned())
        );
        assert_eq!(
            access
                .workflow_token(&run, &permissions, &cancel)
                .await
                .expect("cached token"),
            Some("checks-read-token".to_owned())
        );
        assert!(
            events.try_recv().is_err(),
            "cached token minted more than once"
        );
        access
            .workflow_tokens
            .lock()
            .await
            .get_mut(&permissions)
            .expect("cached workflow token")
            .expires_at_epoch_seconds = 1;
        let refresh = {
            let access = access.clone();
            let run = run.clone();
            let permissions = permissions.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move { access.workflow_token(&run, &permissions, &cancel).await })
        };
        let refresh_request_id = match events.recv().await.expect("workflow token refresh") {
            AgentMessage::WorkflowTokenRequest { request_id, .. } => request_id,
            message => panic!("unexpected event: {message:?}"),
        };
        client
            .handle_granted(
                refresh_request_id,
                "refreshed-checks-read-token".to_owned(),
                4_102_444_800,
            )
            .await;
        assert_eq!(
            refresh
                .await
                .expect("join workflow refresh")
                .expect("refreshed workflow token"),
            Some("refreshed-checks-read-token".to_owned())
        );
        let mut log = "checks-read-token refreshed-checks-read-token".to_owned();
        commands
            .lock()
            .expect("workflow command processor was poisoned")
            .mask_all(&mut log);
        assert_eq!(log, "*** ***");
    }

    #[test]
    fn environment_variables_are_job_scoped_and_override_case_insensitively() {
        let mut run = fixture_run(
            Uuid::nil(),
            "0".repeat(40),
            "0".repeat(40),
            "https://github.com/octocat/Hello-World.git".to_owned(),
        );
        run.variables = BTreeMap::from([
            ("CHANNEL".to_owned(), "repository".to_owned()),
            ("GLOBAL".to_owned(), "available".to_owned()),
        ]);
        let selected = merge_configuration_variables(
            &run.variables,
            BTreeMap::from([
                ("channel".to_owned(), "environment".to_owned()),
                ("REGION".to_owned(), "west".to_owned()),
            ]),
        );
        assert!(!selected.contains_key("CHANNEL"));
        assert_eq!(selected["channel"], "environment");
        assert_eq!(selected["GLOBAL"], "available");
        assert_eq!(selected["REGION"], "west");

        let workflow = parse(
            r#"
name: Environment context
on: pull_request
jobs:
  deploy:
    runs-on: macos-latest
    environment: production
    steps:
      - run: echo deploy
"#,
        )
        .expect("parse workflow");
        let plan =
            gitzero_workflow::compile(&workflow, Path::new(".github/workflows/environment.yml"))
                .expect("compile workflow");
        let job = &plan.jobs[0];
        let environment = BTreeMap::new();
        let steps = JsonValue::Object(Default::default());
        let inputs = JsonValue::Object(Default::default());
        let conclusions = BTreeMap::new();
        let outputs = BTreeMap::new();
        let pre_job = expression_context(
            &run,
            job,
            &conclusions,
            &outputs,
            &environment,
            &steps,
            &inputs,
            ExecutionStatus::Success,
            Path::new("/tmp/workspace"),
            Path::new("/tmp/run"),
            None,
        )
        .expect("pre-job context");
        let runtime = expression_context_with_variables(
            &run,
            &selected,
            job,
            &conclusions,
            &outputs,
            &environment,
            &steps,
            &inputs,
            ExecutionStatus::Success,
            Path::new("/tmp/workspace"),
            Path::new("/tmp/run"),
            None,
            &BTreeMap::new(),
        )
        .expect("runtime context");
        assert_eq!(
            pre_job
                .render("${{ vars.CHANNEL }}")
                .expect("base variable"),
            "repository"
        );
        assert_eq!(
            runtime
                .render("${{ vars.CHANNEL }}-${{ vars.REGION }}")
                .expect("environment variables"),
            "environment-west"
        );
    }

    #[test]
    fn environment_metadata_rejects_protection_gates_and_selects_branch_policy_modes() {
        let protected = GitHubEnvironmentMetadata {
            name: "production".to_owned(),
            protection_rules: vec![json!({"type": "required_reviewers"})],
            deployment_branch_policy: None,
        };
        let error = validate_environment_metadata("production", &protected)
            .expect_err("protected environment should fail");
        assert!(format!("{error:#}").contains("cannot safely emulate"));

        let restricted = GitHubEnvironmentMetadata {
            name: "production".to_owned(),
            protection_rules: Vec::new(),
            deployment_branch_policy: Some(GitHubDeploymentBranchPolicySettings {
                protected_branches: true,
                custom_branch_policies: false,
            }),
        };
        assert_eq!(
            validate_environment_metadata("production", &restricted)
                .expect("protected branch policy"),
            DeploymentBranchPolicyMode::ProtectedBranches
        );

        let custom = GitHubEnvironmentMetadata {
            name: "production".to_owned(),
            protection_rules: Vec::new(),
            deployment_branch_policy: Some(GitHubDeploymentBranchPolicySettings {
                protected_branches: false,
                custom_branch_policies: true,
            }),
        };
        assert_eq!(
            validate_environment_metadata("production", &custom).expect("custom branch policy"),
            DeploymentBranchPolicyMode::Custom
        );

        let invalid = GitHubEnvironmentMetadata {
            name: "production".to_owned(),
            protection_rules: Vec::new(),
            deployment_branch_policy: Some(GitHubDeploymentBranchPolicySettings {
                protected_branches: true,
                custom_branch_policies: true,
            }),
        };
        assert!(validate_environment_metadata("production", &invalid).is_err());
    }

    #[test]
    fn deployment_branch_patterns_follow_githubs_path_aware_fnmatch_rules() {
        for (pattern, candidate) in [
            ("releases/*", "releases/v1"),
            ("release/*/*", "release/2026/v1"),
            ("refs/pull/*/merge", "refs/pull/42/merge"),
            ("feature/?", "feature/x"),
            ("release/[0-9]", "release/7"),
            ("release/[!a-z]", "release/7"),
            (r"release/\*", "release/*"),
            ("**/release", "nested/deep/release"),
            ("**/.release", "nested/.release"),
        ] {
            assert!(
                deployment_pattern_matches(pattern, candidate),
                "{pattern:?} did not match {candidate:?}"
            );
        }
        for (pattern, candidate) in [
            ("releases/*", "releases/2026/v1"),
            ("refs/pull/*/merge", "refs/pull/nested/42/merge"),
            ("release/?", "release/xy"),
            ("release/[!a-z]", "release/x"),
            ("*", ".hidden"),
            ("**/*", "nested/.hidden"),
            ("[abc", "a"),
        ] {
            assert!(
                !deployment_pattern_matches(pattern, candidate),
                "{pattern:?} unexpectedly matched {candidate:?}"
            );
        }
        assert_eq!(
            deployment_policy_reference("refs/heads/releases/v1").expect("branch reference"),
            ("branch", "releases/v1")
        );
        assert_eq!(
            deployment_policy_reference("refs/pull/42/merge").expect("pull request reference"),
            ("branch", "refs/pull/42/merge")
        );
        assert_eq!(
            deployment_policy_reference("refs/tags/v1").expect("tag reference"),
            ("tag", "v1")
        );
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn executes_environment_scoped_variables_after_job_selection() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        let repository = fixture.path().join("repository");
        std::fs::create_dir_all(&repository).expect("repository directory");
        git(&repository, ["init", "--initial-branch=main"]);
        git(
            &repository,
            ["config", "user.email", "gitzero@example.test"],
        );
        git(&repository, ["config", "user.name", "GitZero Test"]);
        std::fs::write(repository.join("README.md"), "fixture\n").expect("fixture file");
        git(&repository, ["add", "."]);
        git(&repository, ["commit", "-m", "fixture"]);
        let head_sha = git_output(&repository, ["rev-parse", "HEAD"]);
        let workflow = parse(
            r#"
name: Environment variables
on: pull_request
jobs:
  deploy:
    runs-on: macos-latest
    strategy:
      matrix:
        target: [production]
    environment:
      name: ${{ matrix.target }}
      url: https://example.test/${{ matrix.target }}?channel=${{ vars.CHANNEL }}
    env:
      SELECTED_CHANNEL: ${{ vars.CHANNEL }}
    steps:
      - if: ${{ vars.CHANNEL == 'environment' && vars.REPOSITORY_ONLY == 'available' }}
        run: echo environment-variable-$SELECTED_CHANNEL
"#,
        )
        .expect("parse workflow");
        let plan =
            gitzero_workflow::compile(&workflow, Path::new(".github/workflows/environment.yml"))
                .expect("compile workflow");
        let run_dir = fixture.path().join("run");
        let tool_cache = fixture.path().join("toolcache");
        tokio::fs::create_dir_all(&run_dir).await.expect("run dir");
        tokio::fs::create_dir_all(&tool_cache)
            .await
            .expect("tool cache");
        let mut run = fixture_run(
            Uuid::new_v4(),
            head_sha.clone(),
            head_sha,
            repository.display().to_string(),
        );
        run.variables = BTreeMap::from([
            ("CHANNEL".to_owned(), "repository".to_owned()),
            ("REPOSITORY_ONLY".to_owned(), "available".to_owned()),
        ]);
        let environment =
            github_environment(&run, &repository, &run_dir, &tool_cache, "GitZero Test")
                .await
                .expect("GitHub environment");
        let (_cancel_tx, cancel) = watch::channel(false);
        let (outbound, mut incoming) = mpsc::channel(128);
        let sequence = Arc::new(AtomicU64::new(0));
        let execution_slots = Arc::new(Semaphore::new(1));
        let runner_targeting = default_runner_targeting();
        let job_summaries = Arc::new(Mutex::new(Vec::new()));
        let environment_variable_cache = Arc::new(Mutex::new(BTreeMap::from([(
            "production".to_owned(),
            BTreeMap::from([("channel".to_owned(), "environment".to_owned())]),
        )])));
        let workflow_commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::default()));
        let concurrency = ConcurrencyClient::local();
        let repository_access = local_repository_access(&workflow_commands);
        let execution = execute_plan(
            run.id,
            &plan,
            0,
            &run,
            &repository,
            &run_dir,
            &environment,
            &cancel,
            &outbound,
            &sequence,
            &execution_slots,
            &runner_targeting,
            &job_summaries,
            &environment_variable_cache,
            &workflow_commands,
            &concurrency,
            &repository_access,
        );
        tokio::pin!(execution);
        let mut events = Vec::new();
        loop {
            tokio::select! {
                result = &mut execution => {
                    result.expect("execute environment workflow");
                    break;
                }
                message = incoming.recv() => {
                    events.push(message.expect("environment event"));
                }
            }
        }
        while let Ok(message) = incoming.try_recv() {
            events.push(message);
        }

        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. }
                if data == "environment-variable-environment\n"
        )));
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::DeploymentStarted { environment, .. }
                if environment == "production"
        )));
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::DeploymentFinished {
                conclusion: Conclusion::Success,
                environment_url: Some(environment_url),
                ..
            } if environment_url == "https://example.test/production?channel=environment"
        )));
        let summaries = job_summaries.lock().await;
        assert!(
            summaries
                .iter()
                .any(|summary| summary.steps.iter().any(|step| {
                    step.name == "Deployment environment"
                        && step
                            .markdown
                            .contains("https://example.test/production?channel=environment")
                }))
        );
    }

    #[tokio::test]
    async fn fetches_and_paginates_url_encoded_environment_variables() {
        use std::io::{Read, Write};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let first_page = (0..ENVIRONMENT_VARIABLE_PAGE_SIZE)
            .map(|index| {
                json!({
                    "name": format!("VARIABLE_{index:02}"),
                    "value": format!("value-{index}")
                })
            })
            .collect::<Vec<_>>();
        let responses = vec![
            json!({
                "name": "production/us",
                "protection_rules": [],
                "deployment_branch_policy": null
            }),
            json!({"total_count": 31, "variables": first_page}),
            json!({
                "total_count": 31,
                "variables": [{"name": "VARIABLE_LAST", "value": "last"}]
            }),
        ];
        let (requests_tx, requests_rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().expect("accept request");
                let mut request = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    let read = stream.read(&mut buffer).expect("read request");
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                requests_tx
                    .send(String::from_utf8(request).expect("HTTP request UTF-8"))
                    .expect("record request");
                let body = serde_json::to_vec(&response).expect("response JSON");
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .expect("write response headers");
                stream.write_all(&body).expect("write response body");
            }
        });
        let (_cancel_tx, cancel) = watch::channel(false);
        let api_base = format!("http://{address}");

        let variables = fetch_github_environment_variables_from(
            GitHubEnvironmentRequest {
                api_base: &api_base,
                owner: "acme",
                repository: "widget",
                environment_name: "production/us",
                github_ref: "refs/pull/7/merge",
                api_version: "2026-03-10",
                token: "environment-token",
            },
            &cancel,
        )
        .await
        .expect("fetch environment variables");
        server.join().expect("join test server");
        let requests = requests_rx.iter().collect::<Vec<_>>();

        assert_eq!(variables.len(), 31);
        assert_eq!(variables["VARIABLE_LAST"], "last");
        assert!(
            requests[0].starts_with("GET /repos/acme/widget/environments/production%2Fus HTTP/1.1")
        );
        assert!(requests[1].starts_with(
            "GET /repos/acme/widget/environments/production%2Fus/variables?per_page=30&page=1 HTTP/1.1"
        ));
        assert!(requests[2].contains("page=2"));
        assert!(requests.iter().all(|request| {
            request
                .to_ascii_lowercase()
                .contains("authorization: bearer environment-token")
        }));
    }

    #[tokio::test]
    async fn fetches_paginated_custom_environment_branch_policies_before_variables() {
        use std::io::{Read, Write};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let first_page = (0..DEPLOYMENT_BRANCH_POLICY_PAGE_SIZE)
            .map(|index| {
                json!({
                    "name": format!("feature/{index}"),
                    "type": "branch"
                })
            })
            .collect::<Vec<_>>();
        let responses = vec![
            json!({
                "total_count": 1,
                "branch_policies": [{"name": "refs/heads/main", "type": "branch"}]
            }),
            json!({
                "name": "production/us",
                "protection_rules": [],
                "deployment_branch_policy": {
                    "protected_branches": false,
                    "custom_branch_policies": true
                }
            }),
            json!({"total_count": 101, "branch_policies": first_page}),
            json!({
                "total_count": 101,
                "branch_policies": [{"name": "refs/pull/*/merge", "type": "branch"}]
            }),
            json!({"total_count": 0, "variables": []}),
        ];
        let (requests_tx, requests_rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().expect("accept request");
                let mut request = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    let read = stream.read(&mut buffer).expect("read request");
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                requests_tx
                    .send(String::from_utf8(request).expect("HTTP request UTF-8"))
                    .expect("record request");
                let body = serde_json::to_vec(&response).expect("response JSON");
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .expect("write response headers");
                stream.write_all(&body).expect("write response body");
            }
        });
        let (_cancel_tx, cancel) = watch::channel(false);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("test client");
        let api_base = format!("http://{address}");
        let request = GitHubEnvironmentRequest {
            api_base: &api_base,
            owner: "acme",
            repository: "widget",
            environment_name: "production/us",
            github_ref: "refs/pull/7/merge",
            api_version: "2026-03-10",
            token: "environment-token",
        };

        let error = enforce_custom_branch_policy(&client, request, &cancel)
            .await
            .expect_err("nonmatching deployment branch policy should fail");
        assert!(format!("{error:#}").contains("do not allow ref"));

        let variables = fetch_github_environment_variables_from(request, &cancel)
            .await
            .expect("fetch branch-gated environment variables");
        server.join().expect("join test server");
        let requests = requests_rx.iter().collect::<Vec<_>>();

        assert!(variables.is_empty());
        assert_eq!(requests.len(), 5);
        assert!(requests[0].starts_with(
            "GET /repos/acme/widget/environments/production%2Fus/deployment-branch-policies?per_page=100&page=1 HTTP/1.1"
        ));
        assert!(requests[2].starts_with(
            "GET /repos/acme/widget/environments/production%2Fus/deployment-branch-policies?per_page=100&page=1 HTTP/1.1"
        ));
        assert!(requests[3].contains("page=2"));
        assert!(requests[4].starts_with(
            "GET /repos/acme/widget/environments/production%2Fus/variables?per_page=30&page=1 HTTP/1.1"
        ));
    }

    #[tokio::test]
    async fn protected_environment_branch_policy_handles_pull_and_branch_refs() {
        use std::io::{Read, Write};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let protected_branch = json!({"name": "main", "protected": true});
        let responses = vec![
            json!([]),
            json!([protected_branch.clone()]),
            json!([protected_branch.clone()]),
            protected_branch,
        ];
        let (requests_tx, requests_rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().expect("accept request");
                let mut request = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    let read = stream.read(&mut buffer).expect("read request");
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                requests_tx
                    .send(String::from_utf8(request).expect("HTTP request UTF-8"))
                    .expect("record request");
                let body = serde_json::to_vec(&response).expect("response JSON");
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .expect("write response headers");
                stream.write_all(&body).expect("write response body");
            }
        });
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("test client");
        let (_cancel_tx, cancel) = watch::channel(false);
        let api_base = format!("http://{address}");
        let request = GitHubEnvironmentRequest {
            api_base: &api_base,
            owner: "acme",
            repository: "widget",
            environment_name: "production",
            github_ref: "refs/pull/7/merge",
            api_version: "2026-03-10",
            token: "environment-token",
        };

        enforce_protected_branch_policy(&client, request, &cancel)
            .await
            .expect("an empty repository protection set allows every ref");
        let error = enforce_protected_branch_policy(&client, request, &cancel)
            .await
            .expect_err("pull request ref should not count as a protected branch");
        assert!(format!("{error:#}").contains("not a repository branch"));
        enforce_protected_branch_policy(
            &client,
            GitHubEnvironmentRequest {
                github_ref: "refs/heads/main",
                ..request
            },
            &cancel,
        )
        .await
        .expect("protected branch");

        server.join().expect("join test server");
        let requests = requests_rx.iter().collect::<Vec<_>>();
        assert_eq!(requests.len(), 4);
        assert!(requests[..3].iter().all(|request| request.starts_with(
            "GET /repos/acme/widget/branches?protected=true&per_page=1&page=1 HTTP/1.1"
        )));
        assert!(requests[3].starts_with("GET /repos/acme/widget/branches/main HTTP/1.1"));
    }

    #[tokio::test]
    async fn workflow_commands_add_scoped_masks_and_honor_command_suspension() {
        let job_id = Uuid::new_v4();
        let (input, incoming) = mpsc::channel(32);
        let (outbound, mut output) = mpsc::channel(32);
        let workflow_commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::default()));
        let relay = tokio::spawn(relay_masked_events(
            incoming,
            outbound,
            vec!["static-secret".to_owned()],
            workflow_commands,
        ));
        let log = |step_id: &str, sequence: u64, data: &str| AgentMessage::LogChunk {
            message_id: Uuid::new_v4(),
            job_id,
            step_id: step_id.to_owned(),
            sequence,
            stream: LogStream::Stdout,
            data: data.to_owned(),
        };

        input
            .send(log("0/job-a/mask", 0, "::add-mask::Mona The Octocat\n"))
            .await
            .expect("mask command");
        input
            .send(log(
                "0/job-a/mask",
                1,
                "Mona The Octocat and Mona static-secret\n",
            ))
            .await
            .expect("masked log");
        input
            .send(AgentMessage::StepStarted {
                message_id: Uuid::new_v4(),
                job_id,
                step_id: "0/job-a/later".to_owned(),
                name: "Mona later".to_owned(),
            })
            .await
            .expect("masked step name");
        input
            .send(log("0/job-b/other", 2, "Mona remains visible\n"))
            .await
            .expect("independent job log");
        input
            .send(log(
                "0/job-a/suspended",
                3,
                "::stop-commands::resume-token\n",
            ))
            .await
            .expect("stop commands");
        input
            .send(log("0/job-a/suspended", 4, "::add-mask::paused-secret\n"))
            .await
            .expect("suspended command");
        input
            .send(log("0/job-a/suspended", 5, "::resume-token::\n"))
            .await
            .expect("resume commands");
        input
            .send(log(
                "0/job-a/suspended",
                6,
                "paused-secret remains visible\n",
            ))
            .await
            .expect("unmasked suspended value");
        input
            .send(log(
                "0/job-a/unclosed",
                7,
                "::stop-commands::unclosed-token\n",
            ))
            .await
            .expect("unclosed stop command");
        input
            .send(AgentMessage::StepFinished {
                message_id: Uuid::new_v4(),
                job_id,
                step_id: "0/job-a/unclosed".to_owned(),
                conclusion: Conclusion::Success,
                exit_code: Some(0),
            })
            .await
            .expect("finish suspended step");
        input
            .send(log(
                "0/job-a/after-stop",
                8,
                "::add-mask::after-stop-secret\n",
            ))
            .await
            .expect("mask after stopped step");
        input
            .send(log("0/job-a/after-stop", 9, "after-stop-secret\n"))
            .await
            .expect("masked value after stopped step");
        input
            .send(log(
                "0/job-a/escaped",
                10,
                "::add-mask::line%0Asecret%25value\n",
            ))
            .await
            .expect("escaped mask");
        input
            .send(log("0/job-a/escaped", 11, "line secret%value\n"))
            .await
            .expect("escaped masked log");
        input
            .send(AgentMessage::JobFinished {
                message_id: Uuid::new_v4(),
                job_id,
                conclusion: Conclusion::Success,
                summary: "Mona secret%value paused-secret after-stop-secret".to_owned(),
                annotations: Vec::new(),
            })
            .await
            .expect("final summary");
        drop(input);
        relay.await.expect("join relay").expect("relay events");

        let mut events = Vec::new();
        while let Some(message) = output.recv().await {
            events.push(message);
        }
        assert_eq!(
            events
                .iter()
                .filter(|message| matches!(message, AgentMessage::LogChunk { .. }))
                .count(),
            6,
            "workflow command lines should be suppressed: {events:?}"
        );
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. } if data == "*** and *** ***\n"
        )));
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::StepStarted { name, .. } if name == "*** later"
        )));
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. } if data == "Mona remains visible\n"
        )));
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. }
                if data == "::add-mask::paused-secret\n"
        )));
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. }
                if data == "paused-secret remains visible\n"
        )));
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. } if data == "***\n"
        )));
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. } if data == "*** ***\n"
        )));
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::JobFinished { summary, .. }
                if summary == "*** *** paused-secret ***"
        )));
    }

    #[tokio::test]
    async fn workflow_commands_publish_bounded_masked_check_annotations() {
        let directory = tempfile::tempdir().expect("temporary workspace");
        let source = directory.path().join("src/lib.rs");
        let job_id = Uuid::new_v4();
        let (input, incoming) = mpsc::channel(64);
        let (outbound, mut output) = mpsc::channel(64);
        let commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::with_global_masks(
            Vec::new(),
            directory.path().to_path_buf(),
            directory.path().to_path_buf(),
        )));
        let relay = tokio::spawn(relay_masked_events(
            incoming,
            outbound,
            vec!["sensitive".to_owned()],
            commands,
        ));
        let log = |sequence: u64, data: String| AgentMessage::LogChunk {
            message_id: Uuid::new_v4(),
            job_id,
            step_id: "0/job-a/annotate".to_owned(),
            sequence,
            stream: LogStream::Stdout,
            data,
        };

        input
            .send(log(
                0,
                format!(
                    "::warning file={},line=3,col=2,endColumn=4,title=sensitive%3Atitle::leak sensitive%25value\n",
                    source.display()
                ),
            ))
            .await
            .expect("file annotation");
        input
            .send(log(1, "::error::generic failure\n".to_owned()))
            .await
            .expect("default annotation");
        input
            .send(log(
                2,
                "::notice file=README.md,line=4,endLine=5,col=9::multiline notice\n".to_owned(),
            ))
            .await
            .expect("multiline annotation");
        for index in 0..10 {
            input
                .send(log(
                    3 + index,
                    format!("::warning file=README.md,line=1::warning-{index}\n"),
                ))
                .await
                .expect("bounded warning");
        }
        input
            .send(AgentMessage::JobFinished {
                message_id: Uuid::new_v4(),
                job_id,
                conclusion: Conclusion::Failure,
                summary: "annotation run".to_owned(),
                annotations: Vec::new(),
            })
            .await
            .expect("finish annotated job");
        drop(input);
        relay.await.expect("join relay").expect("relay annotations");

        let mut events = Vec::new();
        while let Some(message) = output.recv().await {
            events.push(message);
        }
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. }
                if data == "warning: ***:title: leak ***%value\n"
        )));
        let annotations = events
            .iter()
            .find_map(|message| match message {
                AgentMessage::JobFinished { annotations, .. } => Some(annotations),
                _ => None,
            })
            .expect("annotated completion");
        assert_eq!(annotations.len(), 12);
        assert_eq!(annotations[0].path, "src/lib.rs");
        assert_eq!(annotations[0].start_line, 3);
        assert_eq!(annotations[0].start_column, Some(2));
        assert_eq!(annotations[0].end_column, Some(4));
        assert_eq!(annotations[0].title.as_deref(), Some("***:title"));
        assert_eq!(annotations[0].message, "leak ***%value");
        assert_eq!(annotations[1].path, ".github");
        assert_eq!(annotations[1].start_line, 1);
        assert_eq!(
            annotations[1].annotation_level,
            CheckAnnotationLevel::Failure
        );
        assert_eq!(annotations[2].start_column, None);
        assert_eq!(annotations[2].end_column, None);
        assert_eq!(
            annotations
                .iter()
                .filter(|annotation| annotation.annotation_level == CheckAnnotationLevel::Warning)
                .count(),
            MAX_WARNING_ANNOTATIONS_PER_STEP
        );
        assert!(annotations.iter().all(|annotation| {
            !annotation.path.contains("sensitive")
                && !annotation.message.contains("sensitive")
                && annotation
                    .title
                    .as_ref()
                    .is_none_or(|title| !title.contains("sensitive"))
        }));

        let mut expanded = CheckAnnotation {
            path: "x".repeat(MAX_CHECK_ANNOTATION_PATH_BYTES),
            start_line: 1,
            end_line: 1,
            start_column: None,
            end_column: None,
            annotation_level: CheckAnnotationLevel::Notice,
            message: "x".repeat(MAX_CHECK_ANNOTATION_MESSAGE_BYTES),
            title: Some("x".repeat(MAX_CHECK_ANNOTATION_TITLE_BYTES)),
        };
        mask_text(&mut expanded.path, &["x".to_owned()]);
        mask_text(&mut expanded.message, &["x".to_owned()]);
        mask_text(
            expanded.title.as_mut().expect("expanded title"),
            &["x".to_owned()],
        );
        bound_masked_annotation(&mut expanded);
        assert_eq!(expanded.path, ".github");
        assert_eq!(expanded.message.len(), MAX_CHECK_ANNOTATION_MESSAGE_BYTES);
        assert_eq!(
            expanded.title.as_ref().expect("bounded title").len(),
            MAX_CHECK_ANNOTATION_TITLE_BYTES
        );
    }

    #[tokio::test]
    async fn workflow_problem_matchers_publish_once_and_remain_job_scoped() {
        let directory = tempfile::tempdir().expect("temporary run root");
        let discovery = directory.path().join("repository");
        let workspace = directory.path().join("jobs/0-job-a");
        std::fs::create_dir_all(&discovery).expect("discovery checkout");
        std::fs::create_dir_all(workspace.join("src")).expect("workspace");
        std::fs::write(workspace.join("src/main.rs"), "fn main() {}\n").expect("source");
        std::fs::write(
            workspace.join("matcher.json"),
            r#"{
              "problemMatcher": [{
                "owner": "rust-test",
                "pattern": [{
                  "regexp": "^([^:]+):(\\d+):(\\d+): (warning|error) ([^:]+): (.*)$",
                  "file": 1, "line": 2, "column": 3, "severity": 4,
                  "code": 5, "message": 6
                }]
              }]
            }"#,
        )
        .expect("matcher config");
        let job_id = Uuid::new_v4();
        let step_id = "0/job-a/lint";
        let commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::with_global_masks(
            Vec::new(),
            discovery,
            directory.path().to_path_buf(),
        )));
        commands
            .lock()
            .expect("workflow command processor")
            .register_workspace("0/job-a", workspace.clone());

        let mut registration = "::add-matcher::matcher.json\n".to_owned();
        assert!(prepare_workflow_log(
            Some(&commands),
            step_id,
            LogStream::Stdout,
            &mut registration,
        ));
        assert!(registration.is_empty());

        let mut diagnostic =
            "\u{1b}[31msrc/main.rs:3:7: warning GZ100: leak sensitive\u{1b}[0m\n".to_owned();
        assert!(prepare_workflow_log(
            Some(&commands),
            step_id,
            LogStream::Stdout,
            &mut diagnostic,
        ));
        let diagnostic_id = Uuid::new_v4();
        mark_preprocessed_workflow_log(Some(&commands), diagnostic_id);

        let mut direct_annotation = format!(
            "::notice file={},line=5::direct annotation\n",
            workspace.join("src/main.rs").display()
        );
        assert!(prepare_workflow_log(
            Some(&commands),
            step_id,
            LogStream::Stdout,
            &mut direct_annotation,
        ));
        let direct_annotation_id = Uuid::new_v4();
        mark_preprocessed_workflow_log(Some(&commands), direct_annotation_id);

        let mut removal = "::remove-matcher owner=RUST-TEST::\n".to_owned();
        assert!(prepare_workflow_log(
            Some(&commands),
            step_id,
            LogStream::Stdout,
            &mut removal,
        ));
        assert!(removal.is_empty());
        let mut after_removal = "src/main.rs:4:1: error GZ200: removed\n".to_owned();
        assert!(prepare_workflow_log(
            Some(&commands),
            step_id,
            LogStream::Stdout,
            &mut after_removal,
        ));
        let after_removal_id = Uuid::new_v4();
        mark_preprocessed_workflow_log(Some(&commands), after_removal_id);

        let (input, incoming) = mpsc::channel(8);
        let (outbound, mut output) = mpsc::channel(8);
        let relay = tokio::spawn(relay_masked_events(
            incoming,
            outbound,
            vec!["sensitive".to_owned()],
            commands,
        ));
        for (message_id, data) in [
            (diagnostic_id, diagnostic),
            (direct_annotation_id, direct_annotation),
            (after_removal_id, after_removal),
        ] {
            input
                .send(AgentMessage::LogChunk {
                    message_id,
                    job_id,
                    step_id: step_id.to_owned(),
                    sequence: 0,
                    stream: LogStream::Stdout,
                    data,
                })
                .await
                .expect("diagnostic log");
        }
        input
            .send(AgentMessage::LogChunk {
                message_id: Uuid::new_v4(),
                job_id,
                step_id: "0/job-b/lint".to_owned(),
                sequence: 1,
                stream: LogStream::Stdout,
                data: "src/main.rs:8:2: error GZ300: other job\n".to_owned(),
            })
            .await
            .expect("other job diagnostic");
        input
            .send(AgentMessage::JobFinished {
                message_id: Uuid::new_v4(),
                job_id,
                conclusion: Conclusion::Failure,
                summary: "problem matcher run".to_owned(),
                annotations: Vec::new(),
            })
            .await
            .expect("terminal result");
        drop(input);
        relay
            .await
            .expect("join relay")
            .expect("relay matcher events");

        let mut annotations = None;
        let mut logs = Vec::new();
        while let Some(message) = output.recv().await {
            match message {
                AgentMessage::LogChunk { data, .. } => logs.push(data),
                AgentMessage::JobFinished {
                    annotations: found, ..
                } => annotations = Some(found),
                _ => {}
            }
        }
        assert!(logs.iter().any(|line| line.contains("leak ***")));
        let annotations = annotations.expect("annotated terminal result");
        assert_eq!(annotations.len(), 2, "preprocessed log was matched twice");
        assert_eq!(annotations[0].path, "src/main.rs");
        assert_eq!(annotations[0].start_line, 3);
        assert_eq!(annotations[0].start_column, Some(7));
        assert_eq!(annotations[0].title.as_deref(), Some("GZ100"));
        assert_eq!(annotations[0].message, "leak ***");
        assert_eq!(annotations[1].path, "src/main.rs");
        assert_eq!(annotations[1].start_line, 5);
        assert_eq!(
            annotations[1].annotation_level,
            CheckAnnotationLevel::Notice
        );
    }

    #[test]
    fn workflow_command_processor_buffers_masks_split_across_log_chunks() {
        let mut commands = WorkflowCommandProcessor::default();
        let secret = "s".repeat(MAX_LOG_CHUNK_BYTES);
        let prefix = "::add-mask::";
        let split = MAX_LOG_CHUNK_BYTES - prefix.len();
        let mut first = format!("{prefix}{}", &secret[..split]);
        commands
            .process_log("0/job/step", LogStream::Stdout, &mut first)
            .expect("first mask chunk");
        assert!(first.is_empty());

        let mut second = format!("{}\n", &secret[split..]);
        commands
            .process_log("0/job/step", LogStream::Stdout, &mut second)
            .expect("final mask chunk");
        assert!(second.is_empty());

        let mut later = format!("{secret}\n");
        commands.mask_for_step("0/job/later", &mut later);
        assert_eq!(later, "***\n");
    }

    #[test]
    fn workflow_commands_capture_legacy_outputs_and_action_state() {
        let mut commands = WorkflowCommandProcessor::default();
        let step_id = "0/job/action";
        let mut commands_log = concat!(
            "::set-output name=result%2Cpart::line%0Avalue%25encoded\n",
            "::save-state name=process-id::12345\n",
            "::SET-OUTPUT name=result%2Cpart::replacement\n",
        )
        .to_owned();
        commands
            .process_log(step_id, LogStream::Stdout, &mut commands_log)
            .expect("legacy commands");
        assert!(commands_log.is_empty());
        assert_eq!(
            commands.take_legacy_outputs(step_id).expect("outputs"),
            BTreeMap::from([("result,part".to_owned(), "replacement".to_owned())])
        );
        assert_eq!(
            commands.take_legacy_state(step_id).expect("state"),
            BTreeMap::from([("process-id".to_owned(), "12345".to_owned())])
        );

        let mut suspended = concat!(
            "::stop-commands::resume\n",
            "::set-output name=ignored::value\n",
            "::resume::\n",
        )
        .to_owned();
        commands
            .process_log(step_id, LogStream::Stdout, &mut suspended)
            .expect("suspended legacy command");
        assert_eq!(suspended, "::set-output name=ignored::value\n");
        assert!(
            commands
                .take_legacy_outputs(step_id)
                .expect("empty outputs")
                .is_empty()
        );

        assert!(
            commands
                .process_line(step_id, "::set-output name=missing-delimiter")
                .is_err()
        );
    }

    #[test]
    fn check_summary_orders_completed_jobs_and_stays_within_protocol_limit() {
        let summary = compose_check_summary(
            "GitZero completed 2 steps successfully.".to_owned(),
            vec![
                JobSummary {
                    completed_order: 2,
                    job_name: "Second job".to_owned(),
                    steps: vec![StepSummary {
                        name: "Second step".to_owned(),
                        markdown: "second body".to_owned(),
                    }],
                    omitted_steps: false,
                },
                JobSummary {
                    completed_order: 1,
                    job_name: "First job".to_owned(),
                    steps: vec![StepSummary {
                        name: "First step".to_owned(),
                        markdown: "first body".to_owned(),
                    }],
                    omitted_steps: false,
                },
            ],
        );
        assert!(summary.find("## First job") < summary.find("## Second job"));

        let oversized = compose_check_summary(
            "status".to_owned(),
            vec![JobSummary {
                completed_order: 1,
                job_name: "Large job".to_owned(),
                steps: vec![StepSummary {
                    name: "Large step".to_owned(),
                    markdown: "🦀".repeat(MAX_CHECK_SUMMARY_UTF16_UNITS),
                }],
                omitted_steps: false,
            }],
        );
        assert!(oversized.encode_utf16().count() <= MAX_CHECK_SUMMARY_UTF16_UNITS);
        assert!(oversized.ends_with("_Step summaries truncated to fit the GitHub Check limit._"));
    }

    #[tokio::test]
    async fn step_summary_enforces_github_size_and_count_limits() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("summary.md");
        let (outbound, mut incoming) = mpsc::channel(4);
        let sequence = Arc::new(AtomicU64::new(0));
        let mut summary = JobSummaryBuilder::new("Test job".to_owned());

        capture_step_summary(
            &mut summary,
            &path,
            "Deleted step",
            Uuid::nil(),
            "deleted",
            &outbound,
            &sequence,
        )
        .await
        .expect("ignore deleted summary");
        assert!(summary.steps.is_empty());

        tokio::fs::write(&path, vec![b'x'; MAX_STEP_SUMMARY_BYTES + 1])
            .await
            .expect("oversized summary");
        capture_step_summary(
            &mut summary,
            &path,
            "Oversized step",
            Uuid::nil(),
            "oversized",
            &outbound,
            &sequence,
        )
        .await
        .expect("ignore oversized summary");
        assert!(summary.steps.is_empty());
        assert!(matches!(
            incoming.recv().await,
            Some(AgentMessage::LogChunk { data, .. })
                if data.contains("1 MiB per-step limit")
        ));

        tokio::fs::write(&path, "summary body")
            .await
            .expect("valid summary");
        for index in 0..=MAX_STEP_SUMMARIES_PER_JOB {
            capture_step_summary(
                &mut summary,
                &path,
                &format!("Step {index}"),
                Uuid::nil(),
                "valid",
                &outbound,
                &sequence,
            )
            .await
            .expect("capture valid summary");
        }
        assert_eq!(summary.steps.len(), MAX_STEP_SUMMARIES_PER_JOB);
        assert!(summary.omitted_steps);
    }

    #[test]
    fn exposes_builtin_github_token_only_in_job_execution_contexts() {
        let workflow = parse(
            r#"
name: Token
on: pull_request
jobs:
  test:
    runs-on: macos-latest
    steps:
      - run: echo token
"#,
        )
        .expect("parse workflow");
        let plan =
            gitzero_workflow::compile(&workflow, Path::new("token.yml")).expect("compile workflow");
        let mut run = fixture_run(
            Uuid::nil(),
            "0".repeat(40),
            "0".repeat(40),
            "https://github.com/octocat/Hello-World.git".to_owned(),
        );
        run.checkout_token = "repository-installation-token".to_owned();
        run.checkout_token_expires_at_epoch_seconds = Some(4_102_444_800);
        let completed = BTreeMap::new();
        let outputs = BTreeMap::new();
        let environment = BTreeMap::new();
        let steps = JsonValue::Object(Default::default());
        let workspace = Path::new("/tmp/gitzero-token-test");
        let run_dir = Path::new("/tmp/gitzero-token-run");

        let pre_job = expression_context(
            &run,
            &plan.jobs[0],
            &completed,
            &outputs,
            &environment,
            &steps,
            &JsonValue::Object(Default::default()),
            ExecutionStatus::Success,
            workspace,
            run_dir,
            None,
        )
        .expect("pre-job context");
        assert_eq!(
            pre_job.evaluate_json("github.token").expect("token"),
            JsonValue::Null
        );
        assert!(pre_job.evaluate("secrets.GITHUB_TOKEN").is_err());

        let job_token = "job-read-scope-token";
        let job = expression_context(
            &run,
            &plan.jobs[0],
            &completed,
            &outputs,
            &environment,
            &steps,
            &JsonValue::Object(Default::default()),
            ExecutionStatus::Success,
            workspace,
            run_dir,
            Some(job_token),
        )
        .expect("job context");
        assert_eq!(
            job.evaluate_json("github.token").expect("github token"),
            JsonValue::String(job_token.to_owned())
        );
        assert_eq!(
            job.evaluate_json("secrets.GITHUB_TOKEN")
                .expect("secret token"),
            JsonValue::String(job_token.to_owned())
        );
        assert_ne!(job_token, run.checkout_token);
        assert_eq!(
            job.evaluate_json("github.secret_source")
                .expect("secret source"),
            JsonValue::String("Actions".to_owned())
        );
    }

    #[test]
    fn exposes_and_masks_managed_secrets_without_adding_them_to_process_environment() {
        let workflow = parse(
            r#"
name: Managed secrets
on: pull_request
jobs:
  build:
    runs-on: macos-latest
    env:
      FROM_SECRET: ${{ secrets.API_TOKEN }}
    steps:
      - run: test "$FROM_SECRET" = expected
"#,
        )
        .expect("parse workflow");
        let plan = gitzero_workflow::compile(
            &workflow,
            Path::new(".github/workflows/managed-secrets.yml"),
        )
        .expect("compile workflow");
        let run = fixture_run(
            Uuid::new_v4(),
            "1".repeat(40),
            "2".repeat(40),
            "https://github.com/octocat/Hello-World.git".to_owned(),
        );
        let process_environment = BTreeMap::new();
        let managed =
            BTreeMap::from([("API_TOKEN".to_owned(), "managed-runtime-secret".to_owned())]);
        let context = expression_context_with_variables(
            &run,
            &BTreeMap::new(),
            &plan.jobs[0],
            &BTreeMap::new(),
            &BTreeMap::new(),
            &process_environment,
            &JsonValue::Object(Default::default()),
            &JsonValue::Object(Default::default()),
            ExecutionStatus::Success,
            Path::new("/tmp/workspace"),
            Path::new("/tmp/run"),
            None,
            &managed,
        )
        .expect("managed secret context");
        assert_eq!(
            context
                .render("${{ secrets.API_TOKEN }}")
                .expect("render secret"),
            "managed-runtime-secret"
        );
        assert_eq!(
            context
                .render("${{ secrets.UNSET_VALUE }}")
                .expect("render unset secret"),
            ""
        );
        assert!(!process_environment.contains_key("API_TOKEN"));
        assert_eq!(
            context
                .evaluate_json("github.secret_source")
                .expect("secret source"),
            JsonValue::String("Actions".to_owned())
        );

        let commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::default()));
        register_managed_secret_masks(&commands, &managed);
        let mut log = "value=managed-runtime-secret".to_owned();
        commands
            .lock()
            .expect("workflow commands")
            .mask_all(&mut log);
        assert_eq!(log, "value=***");
    }

    #[test]
    fn reusable_secret_aliases_are_scoped_to_each_direct_call() {
        let caller_source = r#"
name: Caller
on: pull_request
jobs:
  outer:
    uses: ./.github/workflows/outer.yml
    secrets:
      outer_token: ${{ secrets.GITHUB_TOKEN }}
"#;
        let outer = parse(
            r#"
name: Outer
on:
  workflow_call:
    secrets:
      outer_token:
        required: true
jobs:
  inner:
    uses: ./.github/workflows/inner.yml
    secrets:
      inner_token: ${{ secrets.outer_token }}
"#,
        )
        .expect("parse outer workflow");
        let inner = parse(
            r#"
name: Inner
on:
  workflow_call:
    secrets:
      inner_token:
        required: true
jobs:
  build:
    runs-on: macos-latest
    steps:
      - run: test -n "${{ secrets.inner_token }}"
"#,
        )
        .expect("parse inner workflow");
        let workflows = BTreeMap::from([
            (".github/workflows/outer.yml".to_owned(), outer),
            (".github/workflows/inner.yml".to_owned(), inner),
        ]);
        let compile = |source: &str| {
            let caller = parse(source).expect("parse caller workflow");
            compile_with_reusables(&caller, Path::new(".github/workflows/ci.yml"), &workflows)
        };
        let plan = compile(caller_source).expect("compile named secret chain");
        let inner_job = plan
            .jobs
            .iter()
            .find(|job| job.base_id.ends_with("inner::build"))
            .expect("inner build job");
        let names = resolve_reusable_secret_names(inner_job).expect("resolve secret aliases");
        assert!(names.contains("GITHUB_TOKEN"));
        assert!(names.contains("inner_token"));
        assert!(
            !names.contains("outer_token"),
            "a named secret leaked beyond the directly called workflow"
        );
        let values = resolve_reusable_secrets(
            inner_job,
            &BTreeMap::from([
                ("GITHUB_TOKEN".to_owned(), "scoped-token".to_owned()),
                ("UNRELATED".to_owned(), "must-not-leak".to_owned()),
            ]),
        )
        .expect("resolve reusable secret values");
        assert_eq!(
            values.get("inner_token").map(String::as_str),
            Some("scoped-token")
        );
        assert!(!values.contains_key("outer_token"));
        assert!(!values.contains_key("UNRELATED"));

        let managed_source = caller_source.replace("secrets.GITHUB_TOKEN", "secrets.API_TOKEN");
        let managed_plan = compile(&managed_source).expect("compile managed secret chain");
        let managed_inner_job = managed_plan
            .jobs
            .iter()
            .find(|job| job.base_id.ends_with("inner::build"))
            .expect("managed inner build job");
        let checkout_values = authorized_checkout_secret_values(
            managed_inner_job,
            &BTreeMap::from([
                ("API_TOKEN".to_owned(), "managed-checkout-token".to_owned()),
                ("UNRELATED".to_owned(), "must-not-leak".to_owned()),
            ]),
            Some("scoped-github-token"),
        )
        .expect("resolve checkout secret authorization");
        assert_eq!(
            checkout_values,
            BTreeSet::from(["managed-checkout-token".to_owned()])
        );

        let unavailable =
            compile(&caller_source.replace("secrets.GITHUB_TOKEN", "secrets.UNAVAILABLE_TOKEN"))
                .expect("compile symbolic unavailable secret");
        let unavailable_job = unavailable
            .jobs
            .iter()
            .find(|job| job.base_id.ends_with("inner::build"))
            .expect("unavailable inner build job");
        let error = resolve_reusable_secret_names(unavailable_job)
            .expect_err("unavailable required secret must fail");
        assert!(error.to_string().contains("outer_token"));
        assert!(!error.to_string().contains("UNAVAILABLE_TOKEN"));
    }

    #[tokio::test]
    async fn environment_file_updates_following_steps() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("env");
        tokio::fs::write(
            &path,
            "FOO=bar\nCOUNT=2\nGITHUB_SHA=untrusted\nRUNNER_OS=Linux\nNODE_OPTIONS=--require=untrusted.js\nnode_options=--require=also-untrusted.js\n",
        )
        .await
        .expect("write");
        let mut environment = BTreeMap::from([
            ("GITHUB_SHA".to_owned(), "trusted".to_owned()),
            ("RUNNER_OS".to_owned(), "macOS".to_owned()),
            ("NODE_OPTIONS".to_owned(), "--no-warnings".to_owned()),
        ]);
        apply_environment_file(&mut environment, &path)
            .await
            .expect("parse");
        assert_eq!(environment["FOO"], "bar");
        assert_eq!(environment["COUNT"], "2");
        assert_eq!(environment["GITHUB_SHA"], "trusted");
        assert_eq!(environment["RUNNER_OS"], "macOS");
        assert_eq!(environment["NODE_OPTIONS"], "--no-warnings");
        assert!(!environment.contains_key("node_options"));
    }

    #[tokio::test]
    async fn path_file_gives_the_last_added_path_highest_precedence() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("path");
        let mut environment =
            BTreeMap::from([("PATH".to_owned(), "/usr/local/bin:/usr/bin".to_owned())]);

        tokio::fs::write(&path, "/tools/first\n/tools/second\n/tools/first\n")
            .await
            .expect("write first path file");
        apply_path_file(&mut environment, &path)
            .await
            .expect("apply first path file");
        assert_eq!(
            environment["PATH"],
            "/tools/first:/tools/second:/usr/local/bin:/usr/bin"
        );

        tokio::fs::write(&path, "/tools/third\n/tools/second\n")
            .await
            .expect("write second path file");
        apply_path_file(&mut environment, &path)
            .await
            .expect("apply second path file");
        assert_eq!(
            environment["PATH"],
            "/tools/second:/tools/third:/tools/first:/usr/local/bin:/usr/bin"
        );
    }

    #[tokio::test]
    async fn exposes_full_webhook_and_standard_github_run_metadata() {
        let directory = tempfile::tempdir().expect("tempdir");
        let repository = directory.path().join("repository");
        let run_dir = directory.path().join("run");
        let tool_cache = directory.path().join("toolcache");
        for path in [&repository, &run_dir, &tool_cache] {
            tokio::fs::create_dir_all(path)
                .await
                .expect("create directory");
        }
        let mut run = fixture_run(
            Uuid::from_u128(123),
            "1".repeat(40),
            "2".repeat(40),
            "https://github.com/acme/widget.git".to_owned(),
        );
        run.repository.owner = "acme".to_owned();
        run.repository.name = "widget".to_owned();
        run.run_number = 19;
        run.variables = BTreeMap::from([
            ("RUNTIME".to_owned(), "24".to_owned()),
            ("CHANNEL".to_owned(), "stable".to_owned()),
        ]);
        run.event = json!({
            "action": "synchronize",
            "number": 42,
            "repository": {
                "id": 9001,
                "full_name": "acme/widget",
                "owner": {"id": 8001, "login": "acme"}
            },
            "pull_request": {
                "number": 42,
                "title": "Keep the complete event",
                "labels": [{"name": "ci"}]
            },
            "sender": {"id": 1234, "login": "octocat"}
        });

        let defaults = github_environment(&run, &repository, &run_dir, &tool_cache, "GitZero Test")
            .await
            .expect("GitHub environment");
        assert_eq!(defaults["GITHUB_ACTOR"], "octocat");
        assert_eq!(defaults["GITHUB_ACTOR_ID"], "1234");
        assert_eq!(defaults["GITHUB_REPOSITORY_ID"], "9001");
        assert_eq!(defaults["GITHUB_REPOSITORY_OWNER_ID"], "8001");
        assert_eq!(defaults["GITHUB_RUN_ID"], "123");
        assert_eq!(defaults["GITHUB_RUN_NUMBER"], "19");
        assert_eq!(defaults["GITHUB_API_URL"], "https://api.github.com");
        assert_eq!(defaults["RUNNER_NAME"], "GitZero Test");
        let persisted_event: JsonValue = serde_json::from_slice(
            &tokio::fs::read(run_dir.join("event.json"))
                .await
                .expect("read event"),
        )
        .expect("parse event");
        assert_eq!(persisted_event, run.event);

        let workflow = parse(
            r#"
name: Metadata
on: pull_request
jobs:
  inspect:
    runs-on: macos-latest
    strategy:
      fail-fast: false
      max-parallel: 1
      matrix:
        shard: [alpha, beta]
    steps:
      - run: echo metadata
"#,
        )
        .expect("parse workflow");
        let plan =
            gitzero_workflow::compile(&workflow, Path::new(".github/workflows/metadata.yml"))
                .expect("compile workflow");
        let mut environment = defaults;
        environment.extend(github_workflow_environment(&run, &plan));
        let context = expression_context(
            &run,
            &plan.jobs[1],
            &BTreeMap::new(),
            &BTreeMap::new(),
            &environment,
            &JsonValue::Object(Default::default()),
            &JsonValue::Object(Default::default()),
            ExecutionStatus::Success,
            &repository,
            &run_dir,
            None,
        )
        .expect("expression context");
        assert_eq!(
            context.evaluate_json("github.actor").expect("actor"),
            JsonValue::String("octocat".to_owned())
        );
        assert_eq!(
            context
                .evaluate_json("github.event.pull_request.title")
                .expect("event title"),
            JsonValue::String("Keep the complete event".to_owned())
        );
        assert_eq!(
            context
                .evaluate_json("github.workflow_ref")
                .expect("workflow ref"),
            JsonValue::String(
                "acme/widget/.github/workflows/metadata.yml@refs/pull/1/merge".to_owned()
            )
        );
        assert_eq!(environment["GITHUB_WORKFLOW"], "Metadata");
        assert_eq!(environment["GITHUB_WORKFLOW_SHA"], "1".repeat(40));
        assert_eq!(
            context.evaluate_json("runner.name").expect("runner name"),
            JsonValue::String("GitZero Test".to_owned())
        );
        assert_eq!(
            context
                .evaluate_json("runner.environment")
                .expect("runner environment"),
            JsonValue::String("self-hosted".to_owned())
        );
        assert_eq!(
            context
                .evaluate_json("runner.debug")
                .expect("unset runner debug"),
            JsonValue::String(String::new())
        );
        assert_eq!(
            context.evaluate_json("runner.temp").expect("runner temp"),
            JsonValue::String(environment["RUNNER_TEMP"].clone())
        );
        assert_eq!(
            context
                .evaluate_json("strategy.job-index")
                .expect("strategy index"),
            JsonValue::from(1)
        );
        assert_eq!(
            context
                .evaluate_json("strategy.job-total")
                .expect("strategy total"),
            JsonValue::from(2)
        );
        assert_eq!(
            context
                .evaluate_json("strategy.max-parallel")
                .expect("strategy parallelism"),
            JsonValue::from(1)
        );
        assert_eq!(
            context
                .evaluate_json("strategy.fail-fast")
                .expect("strategy fail-fast"),
            JsonValue::Bool(false)
        );
        assert_eq!(
            context
                .evaluate_json("vars.RUNTIME")
                .expect("runtime variable"),
            JsonValue::String("24".to_owned())
        );
        assert_eq!(
            context
                .evaluate_json("vars.VERSION || vars.CHANNEL")
                .expect("unset variable fallback"),
            JsonValue::String("stable".to_owned())
        );
    }

    #[test]
    fn command_files_support_multiline_values() {
        let values = parse_command_source(
            "simple=value\nliteral=value<<not-a-delimiter\nmessage<<END\nline one\nline two\nEND\n",
        )
        .expect("parse command file");
        assert_eq!(values["simple"], "value");
        assert_eq!(values["literal"], "value<<not-a-delimiter");
        assert_eq!(values["message"], "line one\nline two");
    }

    #[test]
    fn resolves_job_concurrency_with_needs_matrix_and_variables() {
        let workflow = parse(
            r#"
name: CI
on: pull_request
jobs:
  build:
    runs-on: macos-latest
    steps:
      - run: true
  deploy:
    needs: build
    runs-on: macos-latest
    strategy:
      matrix:
        region: [west, east]
    concurrency:
      group: ${{ github.workflow }}-${{ matrix.region }}-${{ needs.build.outputs.target }}-${{ vars.CHANNEL }}
      cancel-in-progress: ${{ needs.build.result == 'failure' }}
      queue: max
    steps:
      - run: true
"#,
        )
        .expect("parse concurrency workflow");
        let plan =
            gitzero_workflow::compile(&workflow, Path::new(".github/workflows/concurrency.yml"))
                .expect("compile concurrency workflow");
        let job = plan
            .jobs
            .iter()
            .find(|job| job.matrix.get("region") == Some(&JsonValue::String("west".to_owned())))
            .expect("west deployment job");
        let mut run = fixture_run(
            Uuid::new_v4(),
            "0123456789012345678901234567890123456789".to_owned(),
            "abcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned(),
            "https://github.com/local/fixture.git".to_owned(),
        );
        run.variables
            .insert("CHANNEL".to_owned(), "stable".to_owned());
        let completed = BTreeMap::from([("build".to_owned(), JobConclusion::Success)]);
        let outputs = BTreeMap::from([(
            "build".to_owned(),
            BTreeMap::from([("target".to_owned(), "production".to_owned())]),
        )]);
        let environment = BTreeMap::from([("GITHUB_WORKFLOW".to_owned(), "CI".to_owned())]);
        assert_eq!(
            resolve_job_concurrency(
                job,
                &run,
                &completed,
                &outputs,
                &environment,
                &JsonValue::Object(Default::default()),
                Path::new("/tmp/workspace"),
                Path::new("/tmp/run"),
            )
            .expect("resolve job concurrency"),
            Some((
                "CI-west-production-stable".to_owned(),
                false,
                ConcurrencyQueue::Max,
            ))
        );
    }

    #[test]
    fn conditions_apply_implicit_success_semantics() {
        let success = EvaluationContext::new().with_status(ExecutionStatus::Success);
        assert!(condition_allows(Some("true"), ExecutionStatus::Success, &success).expect("true"));

        let skipped = EvaluationContext::new().with_status(ExecutionStatus::Skipped);
        assert!(!condition_allows(None, ExecutionStatus::Skipped, &skipped).expect("default"));
        assert!(
            !condition_allows(Some("failure()"), ExecutionStatus::Skipped, &skipped)
                .expect("failure")
        );
        assert!(
            condition_allows(Some("always()"), ExecutionStatus::Skipped, &skipped).expect("always")
        );
    }

    #[test]
    fn shell_invocations_match_githubs_file_based_commands() {
        let script = Path::new("/tmp/script path.sh");
        assert_eq!(
            shell_arguments_for("bash -e {0}", script).expect("default bash args"),
            ["-e", "/tmp/script path.sh"]
        );
        assert_eq!(
            shell_arguments_for("bash", script).expect("explicit bash args"),
            [
                "--noprofile",
                "--norc",
                "-eo",
                "pipefail",
                "/tmp/script path.sh"
            ]
        );
        assert_eq!(
            shell_arguments_for("sh", script).expect("sh args"),
            ["-e", "/tmp/script path.sh"]
        );
        assert_eq!(
            shell_arguments_for("bash --noprofile '{0}'", script).expect("template args"),
            ["--noprofile", "/tmp/script path.sh"]
        );
        assert!(shell_arguments_for("zsh", script).is_err());
        assert_eq!(shell_script_extension("pwsh").expect("extension"), "ps1");
        assert_eq!(
            shell_script_extension("/opt/homebrew/bin/python3 {0}").expect("extension"),
            "py"
        );
    }

    #[tokio::test]
    async fn default_and_explicit_bash_preserve_github_pipeline_behavior() {
        let directory = tempfile::tempdir().expect("tempdir");
        let script = directory.path().join("pipeline.sh");
        tokio::fs::write(&script, "false | true\nprintf 'continued\\n'\n")
            .await
            .expect("script");

        let default = Command::new(shell_program("bash -e {0}").expect("program"))
            .args(shell_arguments_for("bash -e {0}", &script).expect("default arguments"))
            .output()
            .await
            .expect("default bash");
        assert!(default.status.success());
        assert_eq!(default.stdout, b"continued\n");

        let explicit = Command::new(shell_program("bash").expect("program"))
            .args(shell_arguments_for("bash", &script).expect("explicit arguments"))
            .output()
            .await
            .expect("explicit bash");
        assert!(!explicit.status.success());
    }

    #[tokio::test]
    async fn process_output_is_forwarded_as_protocol_events() {
        let (outbound, mut incoming) = mpsc::channel(8);
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let sequence = Arc::new(AtomicU64::new(0));
        let mut command = Command::new("/bin/zsh");
        command.args(["-c", "printf 'hello\\n'"]);
        let status = run_process(
            Uuid::nil(),
            "step-1",
            &mut command,
            None,
            cancel_rx,
            outbound,
            sequence,
        )
        .await
        .expect("process");
        assert_eq!(status, 0);
        let message = incoming.recv().await.expect("log event");
        assert!(matches!(
            message,
            AgentMessage::LogChunk { data, .. } if data == "hello\n"
        ));
    }

    #[tokio::test]
    async fn process_output_is_bounded_to_protocol_sized_chunks() {
        let (outbound, mut incoming) = mpsc::channel(16);
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let sequence = Arc::new(AtomicU64::new(0));
        let mut command = Command::new("/bin/zsh");
        command.args(["-c", "printf '%0100000d' 0"]);
        let status = run_process(
            Uuid::nil(),
            "large-output",
            &mut command,
            None,
            cancel_rx,
            outbound,
            sequence,
        )
        .await
        .expect("process");
        assert_eq!(status, 0);

        let mut chunks = Vec::new();
        while let Some(message) = incoming.recv().await {
            if let AgentMessage::LogChunk { data, .. } = message {
                assert!(data.len() <= MAX_LOG_CHUNK_BYTES);
                chunks.push(data);
            }
        }
        assert!(chunks.len() > 1);
        assert_eq!(chunks.concat().len(), 100_000);
    }

    #[tokio::test]
    async fn workflow_discovery_uses_preloaded_changed_paths() {
        let directory = tempfile::tempdir().expect("tempdir");
        let workflows = directory.path().join(".github/workflows");
        tokio::fs::create_dir_all(&workflows)
            .await
            .expect("workflow directory");
        tokio::fs::write(
            workflows.join("ci.yml"),
            r#"name: CI
on:
  pull_request:
    paths: ['src/**']
jobs:
  test:
    runs-on: macos-latest
    steps:
      - run: echo ok
"#,
        )
        .await
        .expect("workflow");
        let mut run = fixture_run(
            Uuid::new_v4(),
            "0".repeat(40),
            "0".repeat(40),
            "https://github.com/example/repository.git".to_owned(),
        );
        run.changed_paths = Some(vec!["docs/readme.md".to_owned()]);
        let (_cancel_tx, cancel) = watch::channel(false);
        let (outbound, _incoming) = mpsc::channel(8);
        let sequence = Arc::new(AtomicU64::new(0));
        let workflow_commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::default()));
        let repository_access = local_repository_access(&workflow_commands);
        let run_dir = directory.path().join("run");
        tokio::fs::create_dir_all(&run_dir).await.expect("run dir");
        assert!(
            discover_workflows(
                directory.path(),
                &run_dir,
                &run,
                &cancel,
                &outbound,
                &sequence,
                &repository_access,
            )
            .await
            .expect("discover unmatched")
            .is_empty()
        );

        run.changed_paths = Some(vec!["src/lib.rs".to_owned()]);
        assert_eq!(
            discover_workflows(
                directory.path(),
                &run_dir,
                &run,
                &cancel,
                &outbound,
                &sequence,
                &repository_access,
            )
            .await
            .expect("discover matched")
            .len(),
            1
        );
    }

    #[tokio::test]
    async fn process_timeout_terminates_the_child() {
        let (outbound, _incoming) = mpsc::channel(8);
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let mut command = Command::new("/bin/zsh");
        command.args(["-c", "sleep 2"]);
        let result = run_process(
            Uuid::nil(),
            "timeout-step",
            &mut command,
            Some(Duration::from_millis(20)),
            cancel_rx,
            outbound,
            Arc::new(AtomicU64::new(0)),
        )
        .await;
        assert!(matches!(result, Err(ProcessError::TimedOut)));
    }

    #[test]
    fn timeout_minutes_requires_a_positive_integer_within_githubs_limit() {
        let context = EvaluationContext::new();
        assert_eq!(
            parse_timeout(Some("1"), &context).expect("one minute"),
            Some(Duration::from_secs(60))
        );
        for invalid in ["0", "1.5", "361", "invalid"] {
            assert!(
                parse_timeout(Some(invalid), &context).is_err(),
                "accepted {invalid}"
            );
        }
    }

    #[tokio::test]
    async fn external_cancellation_is_not_reported_as_a_job_timeout() {
        let (outer, outer_rx) = watch::channel(false);
        let timed_out = Arc::new(AtomicBool::new(false));
        let (mut cancel, task) =
            deadline_cancellation(&outer_rx, Some(Duration::from_secs(60)), timed_out.clone());
        outer.send_replace(true);
        cancel.changed().await.expect("derived cancellation");
        task.expect("timeout relay").await.expect("join relay");
        assert!(*cancel.borrow());
        assert!(!timed_out.load(Ordering::Acquire));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn job_timeout_terminates_the_process_tree_and_reports_timed_out() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        let repository = fixture.path().join("repository");
        std::fs::create_dir_all(&repository).expect("repository directory");
        git(&repository, ["init", "--initial-branch=main"]);
        git(
            &repository,
            ["config", "user.email", "gitzero@example.test"],
        );
        git(&repository, ["config", "user.name", "GitZero Test"]);
        std::fs::write(repository.join("README.md"), "fixture\n").expect("fixture file");
        let workflow_path = repository.join(".github/workflows/job-timeout.yml");
        std::fs::create_dir_all(workflow_path.parent().expect("workflow parent"))
            .expect("workflow directory");
        std::fs::write(
            &workflow_path,
            r#"
name: Job timeout
on: pull_request
jobs:
  limited:
    runs-on: macos-latest
    timeout-minutes: 1
    strategy:
      fail-fast: true
      max-parallel: 1
      matrix:
        case: [hang, queued]
    steps:
      - run: |
          if [[ "${{ matrix.case }}" == "hang" ]]; then
            echo job-timeout-started
            sleep 600
            echo job-timeout-finished
          else
            echo job-timeout-queued
          fi
      - run: echo job-timeout-next-step
"#,
        )
        .expect("workflow");
        git(&repository, ["add", "."]);
        git(&repository, ["commit", "-m", "fixture"]);
        let head_sha = git_output(&repository, ["rev-parse", "HEAD"]);
        let remote = fixture.path().join("remote.git");
        git(
            fixture.path(),
            ["init", "--bare", remote.to_str().expect("remote path")],
        );
        git(
            &repository,
            [
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path"),
            ],
        );
        git(&repository, ["push", "origin", "main"]);
        git(&repository, ["push", "origin", "HEAD:refs/pull/1/head"]);
        git(&repository, ["push", "origin", "HEAD:refs/pull/1/merge"]);

        let executor = Executor::new(ExecutorConfig {
            work_root: fixture.path().join("runs"),
            runner_name: "GitZero Test".to_owned(),
            keep_failed_workspaces: false,
            max_parallelism: 1,
            cache_max_bytes: 10 * 1024 * 1024,
            cache_max_entry_bytes: 1024 * 1024,
            artifact_max_bytes: 10 * 1024 * 1024,
            artifact_max_entry_bytes: 1024 * 1024,
        });
        let run = fixture_run(
            Uuid::new_v4(),
            head_sha.clone(),
            head_sha,
            remote.display().to_string(),
        );
        let (_cancel_tx, cancel) = watch::channel(false);
        let (outbound, mut incoming) = mpsc::channel(128);
        let execution = executor.execute(run, cancel, outbound);
        tokio::pin!(execution);
        let mut events = Vec::new();
        let mut advanced = false;
        loop {
            tokio::select! {
                result = &mut execution => {
                    result.expect("execute timed job");
                    break;
                },
                message = incoming.recv() => {
                    let Some(message) = message else {
                        execution.await.expect("execute timed job");
                        break;
                    };
                    if !advanced
                        && matches!(
                            &message,
                            AgentMessage::LogChunk { data, .. }
                                if data == "job-timeout-started\n"
                        )
                    {
                        advanced = true;
                        tokio::time::pause();
                        tokio::time::advance(Duration::from_secs(61)).await;
                        tokio::time::resume();
                    }
                    events.push(message);
                }
            }
        }
        while let Ok(message) = incoming.try_recv() {
            events.push(message);
        }

        assert!(advanced, "job never reached the timed step");
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::JobFinished {
                conclusion: Conclusion::TimedOut,
                summary,
                ..
            } if summary.contains("Job timeout / limited")
        )));
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::StepFinished {
                conclusion: Conclusion::TimedOut,
                ..
            }
        )));
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. }
                if data == "Skipped by matrix fail-fast.\n"
        )));
        assert!(!events.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. }
                if matches!(
                    data.as_str(),
                    "job-timeout-finished\n"
                        | "job-timeout-next-step\n"
                        | "job-timeout-queued\n"
                )
        )));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn composite_step_timeout_terminates_nested_process_and_honors_continue_on_error() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        let repository = fixture.path().join("repository");
        std::fs::create_dir_all(&repository).expect("repository directory");
        git(&repository, ["init", "--initial-branch=main"]);
        git(
            &repository,
            ["config", "user.email", "gitzero@example.test"],
        );
        git(&repository, ["config", "user.name", "GitZero Test"]);
        std::fs::write(repository.join("README.md"), "fixture\n").expect("fixture file");

        let action_path = repository.join(".github/actions/hang/action.yml");
        std::fs::create_dir_all(action_path.parent().expect("action parent"))
            .expect("action directory");
        std::fs::write(
            &action_path,
            r#"
name: Hanging composite
runs:
  using: composite
  steps:
    - shell: bash
      run: |
        echo composite-timeout-started
        sleep 600
        echo composite-timeout-finished
    - shell: bash
      run: echo composite-timeout-nested-followup
"#,
        )
        .expect("action metadata");

        let workflow_path = repository.join(".github/workflows/composite-timeout.yml");
        std::fs::create_dir_all(workflow_path.parent().expect("workflow parent"))
            .expect("workflow directory");
        std::fs::write(
            &workflow_path,
            r#"
name: Composite timeout
on: pull_request
jobs:
  limited:
    runs-on: macos-latest
    steps:
      - uses: actions/checkout@v4
      - id: timed
        uses: ./.github/actions/hang
        timeout-minutes: 1
        continue-on-error: true
      - if: ${{ always() }}
        run: |
          test "${{ steps.timed.outcome }}" = failure
          test "${{ steps.timed.conclusion }}" = success
          echo composite-timeout-followup
"#,
        )
        .expect("workflow");
        git(&repository, ["add", "."]);
        git(&repository, ["commit", "-m", "fixture"]);
        let head_sha = git_output(&repository, ["rev-parse", "HEAD"]);
        let remote = fixture.path().join("remote.git");
        git(
            fixture.path(),
            ["init", "--bare", remote.to_str().expect("remote path")],
        );
        git(
            &repository,
            [
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path"),
            ],
        );
        git(&repository, ["push", "origin", "main"]);
        git(&repository, ["push", "origin", "HEAD:refs/pull/1/head"]);
        git(&repository, ["push", "origin", "HEAD:refs/pull/1/merge"]);

        let executor = Executor::new(ExecutorConfig {
            work_root: fixture.path().join("runs"),
            runner_name: "GitZero Test".to_owned(),
            keep_failed_workspaces: false,
            max_parallelism: 1,
            cache_max_bytes: 10 * 1024 * 1024,
            cache_max_entry_bytes: 1024 * 1024,
            artifact_max_bytes: 10 * 1024 * 1024,
            artifact_max_entry_bytes: 1024 * 1024,
        });
        let run = fixture_run(
            Uuid::new_v4(),
            head_sha.clone(),
            head_sha,
            remote.display().to_string(),
        );
        let (_cancel_tx, cancel) = watch::channel(false);
        let (outbound, mut incoming) = mpsc::channel(128);
        let execution = executor.execute(run, cancel, outbound);
        tokio::pin!(execution);
        let mut events = Vec::new();
        let mut advanced = false;
        loop {
            tokio::select! {
                result = &mut execution => {
                    result.expect("execute timed composite action");
                    break;
                },
                message = incoming.recv() => {
                    let Some(message) = message else {
                        execution.await.expect("execute timed composite action");
                        break;
                    };
                    if !advanced
                        && matches!(
                            &message,
                            AgentMessage::LogChunk { data, .. }
                                if data == "composite-timeout-started\n"
                        )
                    {
                        advanced = true;
                        tokio::time::pause();
                        tokio::time::advance(Duration::from_secs(61)).await;
                        tokio::time::resume();
                    }
                    events.push(message);
                }
            }
        }
        while let Ok(message) = incoming.try_recv() {
            events.push(message);
        }

        assert!(advanced, "composite action never reached the timed step");
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::JobFinished {
                conclusion: Conclusion::Success,
                ..
            }
        )));
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::StepFinished {
                step_id,
                conclusion: Conclusion::Success,
                exit_code: None,
                ..
            } if step_id == "0/limited/timed"
        )));
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. }
                if data == "composite-timeout-followup\n"
        )));
        assert!(!events.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. }
                if matches!(
                    data.as_str(),
                    "composite-timeout-finished\n" | "composite-timeout-nested-followup\n"
                )
        )));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn materializes_sha256_remote_repository_objects() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        let source = fixture.path().join("action-source");
        let remote = fixture.path().join("action.git");
        let work_root = fixture.path().join("runs");
        let run_dir = work_root.join(Uuid::new_v4().to_string());
        std::fs::create_dir_all(&source).expect("source directory");
        std::fs::create_dir_all(&run_dir).expect("run directory");
        git(
            &source,
            ["init", "--object-format=sha256", "--initial-branch=main"],
        );
        git(&source, ["config", "user.email", "gitzero@example.test"]);
        git(&source, ["config", "user.name", "GitZero Test"]);
        std::fs::write(source.join("action.yml"), "name: SHA-256 action\n")
            .expect("action metadata");
        git(&source, ["add", "."]);
        git(&source, ["commit", "-m", "sha256 action"]);
        git(
            fixture.path(),
            [
                "init",
                "--bare",
                "--object-format=sha256",
                remote.to_str().expect("remote path"),
            ],
        );
        git(
            &source,
            [
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path"),
            ],
        );
        git(&source, ["push", "origin", "main"]);
        git(&remote, ["symbolic-ref", "HEAD", "refs/heads/main"]);

        let stale_cache = work_root.join("_action-cache/fixture/sha256-action.git");
        std::fs::create_dir_all(stale_cache.parent().expect("cache parent"))
            .expect("cache parent directory");
        git(
            fixture.path(),
            ["init", "--bare", stale_cache.to_str().expect("cache path")],
        );
        git(
            &stale_cache,
            [
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path"),
            ],
        );

        let (outbound, _incoming) = mpsc::channel(64);
        let (_cancel_tx, cancel) = watch::channel(false);
        let checkout = materialize_remote_repository(
            Uuid::new_v4(),
            "0/sha256-action",
            "fixture",
            "sha256-action",
            "main",
            remote.to_str().expect("remote path"),
            RemoteRepositoryMaterializationScope::SharedSource,
            None,
            &run_dir,
            &cancel,
            &outbound,
            &Arc::new(AtomicU64::new(0)),
        )
        .await
        .expect("materialize SHA-256 action");

        assert_eq!(
            git_output(&checkout, ["rev-parse", "--show-object-format"]),
            "sha256"
        );
        assert_eq!(git_output(&checkout, ["rev-parse", "HEAD"]).len(), 64);
        assert_eq!(
            git_output(&stale_cache, ["config", "--get", "extensions.objectFormat"],),
            "sha256"
        );
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn shares_remote_action_objects_and_pins_refs_per_run() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        let source = fixture.path().join("action-source");
        let remote = fixture.path().join("action.git");
        let work_root = fixture.path().join("runs");
        let first_run = work_root.join(Uuid::new_v4().to_string());
        std::fs::create_dir_all(&source).expect("source directory");
        std::fs::create_dir_all(&first_run).expect("first run directory");
        git(&source, ["init", "--initial-branch=main"]);
        git(&source, ["config", "user.email", "gitzero@example.test"]);
        git(&source, ["config", "user.name", "GitZero Test"]);
        std::fs::write(
            source.join("action.yml"),
            "name: Cached action v1\nruns:\n  using: node24\n  main: index.js\n",
        )
        .expect("first action metadata");
        std::fs::write(source.join("index.js"), "console.log('v1')\n")
            .expect("first action source");
        git(&source, ["add", "."]);
        git(&source, ["commit", "-m", "v1"]);
        git(
            fixture.path(),
            ["init", "--bare", remote.to_str().expect("remote path")],
        );
        git(
            &source,
            [
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path"),
            ],
        );
        git(&source, ["push", "origin", "main"]);

        let (outbound, _incoming) = mpsc::channel(256);
        let (_cancel_tx, cancel) = watch::channel(false);
        let workflow_commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::default()));
        let repository_access = local_repository_access(&workflow_commands);
        let checkout_run = fixture_run(
            Uuid::new_v4(),
            "a".repeat(40),
            "b".repeat(40),
            "https://github.com/local/fixture.git".to_owned(),
        );
        let first_sequence = Arc::new(AtomicU64::new(0));
        let second_sequence = Arc::new(AtomicU64::new(0));
        let first = materialize_remote_repository(
            Uuid::new_v4(),
            "0/first/action",
            "fixture",
            "cached-action",
            "main",
            remote.to_str().expect("remote path"),
            RemoteRepositoryMaterializationScope::SharedSource,
            None,
            &first_run,
            &cancel,
            &outbound,
            &first_sequence,
        );
        let second = materialize_remote_repository(
            Uuid::new_v4(),
            "0/second/action",
            "fixture",
            "cached-action",
            "main",
            remote.to_str().expect("remote path"),
            RemoteRepositoryMaterializationScope::SharedSource,
            None,
            &first_run,
            &cancel,
            &outbound,
            &second_sequence,
        );
        let (first, second) = tokio::join!(first, second);
        let first = first.expect("first concurrent materialization");
        let second = second.expect("second concurrent materialization");
        assert_eq!(
            tokio::fs::read_to_string(first.join("index.js"))
                .await
                .expect("first action source"),
            "console.log('v1')\n"
        );
        assert_eq!(
            tokio::fs::read_to_string(second.join("index.js"))
                .await
                .expect("second action source"),
            "console.log('v1')\n"
        );

        std::fs::write(source.join("index.js"), "console.log('v2')\n")
            .expect("second action source");
        git(&source, ["add", "."]);
        git(&source, ["commit", "-m", "v2"]);
        git(&source, ["push", "origin", "main"]);

        let pinned = materialize_remote_repository(
            Uuid::new_v4(),
            "0/third/action",
            "fixture",
            "cached-action",
            "main",
            remote.to_str().expect("remote path"),
            RemoteRepositoryMaterializationScope::SharedSource,
            None,
            &first_run,
            &cancel,
            &outbound,
            &Arc::new(AtomicU64::new(0)),
        )
        .await
        .expect("same-run materialization");
        assert_eq!(
            tokio::fs::read_to_string(pinned.join("index.js"))
                .await
                .expect("pinned action source"),
            "console.log('v1')\n",
            "one run must resolve a moving action ref only once"
        );
        let public_checkout_repository = CheckoutRepository {
            owner: "fixture".to_owned(),
            name: "cached-action".to_owned(),
            clone_url: remote.display().to_string(),
            same_repository: false,
        };
        let (pinned_checkout_commit, pinned_checkout, pinned_token) =
            materialize_cross_repository_checkout_snapshot(
                checkout_run.id,
                "0/public-checkout",
                &public_checkout_repository,
                "main",
                &checkout_run,
                &repository_access,
                CrossRepositoryCheckoutAccess::ManagedFallback,
                &first_run,
                &cancel,
                &outbound,
                &Arc::new(AtomicU64::new(0)),
            )
            .await
            .expect("same-run public checkout snapshot");
        assert!(pinned_token.is_none());
        assert_eq!(
            tokio::fs::read_to_string(pinned_checkout.join("index.js"))
                .await
                .expect("pinned public checkout source"),
            "console.log('v1')\n",
            "a public checkout must reuse the run's pinned ref resolution"
        );
        assert_eq!(
            git_output(&pinned_checkout, ["rev-parse", "HEAD"]),
            pinned_checkout_commit
        );

        let second_run = work_root.join(Uuid::new_v4().to_string());
        std::fs::create_dir_all(&second_run).expect("second run directory");
        let second_checkout_run = RunSpec {
            id: Uuid::new_v4(),
            ..checkout_run.clone()
        };
        let refreshed = materialize_remote_repository(
            Uuid::new_v4(),
            "0/first/action",
            "fixture",
            "cached-action",
            "main",
            remote.to_str().expect("remote path"),
            RemoteRepositoryMaterializationScope::SharedSource,
            None,
            &second_run,
            &cancel,
            &outbound,
            &Arc::new(AtomicU64::new(0)),
        )
        .await
        .expect("next-run materialization");
        assert_eq!(
            tokio::fs::read_to_string(refreshed.join("index.js"))
                .await
                .expect("refreshed action source"),
            "console.log('v2')\n",
            "a new run must refresh a moving action ref"
        );
        let (refreshed_checkout_commit, refreshed_checkout, refreshed_token) =
            materialize_cross_repository_checkout_snapshot(
                second_checkout_run.id,
                "0/public-checkout",
                &public_checkout_repository,
                "main",
                &second_checkout_run,
                &repository_access,
                CrossRepositoryCheckoutAccess::ManagedFallback,
                &second_run,
                &cancel,
                &outbound,
                &Arc::new(AtomicU64::new(0)),
            )
            .await
            .expect("next-run public checkout snapshot");
        assert!(refreshed_token.is_none());
        assert_eq!(
            tokio::fs::read_to_string(refreshed_checkout.join("index.js"))
                .await
                .expect("refreshed public checkout source"),
            "console.log('v2')\n",
            "a public checkout must refresh the moving ref in a new run"
        );
        assert_eq!(
            git_output(&refreshed_checkout, ["rev-parse", "HEAD"]),
            refreshed_checkout_commit
        );
        assert!(
            work_root
                .join("_action-cache/fixture/cached-action.git/objects")
                .is_dir(),
            "both runs should share one persistent object store"
        );
    }

    #[tokio::test]
    #[ignore = "network smoke test"]
    async fn downloads_public_javascript_action_metadata() {
        let directory = tempfile::tempdir().expect("tempdir");
        let workspace = directory.path().join("workspace");
        let run_dir = directory.path().join("run");
        tokio::fs::create_dir_all(&workspace)
            .await
            .expect("workspace");
        tokio::fs::create_dir_all(&run_dir).await.expect("run dir");
        let run = fixture_run(
            Uuid::new_v4(),
            "0".repeat(40),
            "0".repeat(40),
            "https://github.com/octocat/Hello-World.git".to_owned(),
        );
        let reference = ActionReference::parse("actions/setup-node@v6").expect("reference");
        let (outbound, _incoming) = mpsc::channel(128);
        let (_cancel_tx, cancel) = watch::channel(false);
        let workflow_commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::default()));
        let repository_access = local_repository_access(&workflow_commands);
        let action = materialize_action(
            run.id,
            "0/test/setup",
            &reference,
            &run,
            &workspace,
            &run_dir,
            &cancel,
            &outbound,
            &Arc::new(AtomicU64::new(0)),
            &repository_access,
        )
        .await
        .expect("download action");
        let definition = load_definition(&action).await.expect("metadata");
        assert!(definition.runs.using.starts_with("node"));
        assert!(definition.runs.main.is_some());
    }

    #[tokio::test]
    #[ignore = "network smoke test"]
    async fn lists_public_pull_request_changed_paths() {
        let mut run = fixture_run(
            Uuid::new_v4(),
            "0".repeat(40),
            "0".repeat(40),
            "https://github.com/actions/checkout.git".to_owned(),
        );
        run.repository.owner = "actions".to_owned();
        run.repository.name = "checkout".to_owned();
        run.pull_request.number = 1;

        let (_cancel_tx, cancel) = watch::channel(false);
        let paths = fetch_pull_request_changed_paths(&run, None, &cancel)
            .await
            .expect("list public pull request files");

        assert!(paths.iter().any(|path| path == "action.yml"));
    }

    #[tokio::test]
    #[ignore = "network smoke test"]
    async fn downloads_public_remote_reusable_workflow() {
        let directory = tempfile::tempdir().expect("tempdir");
        let run_dir = directory.path().join("run");
        tokio::fs::create_dir_all(&run_dir).await.expect("run dir");
        let run = fixture_run(
            Uuid::new_v4(),
            "0".repeat(40),
            "0".repeat(40),
            "https://github.com/octocat/Hello-World.git".to_owned(),
        );
        let reference = RemoteReusableWorkflowReference::parse(
            "rmcrackan/Libation/.github/workflows/build-mac.yml@6fba3d61f324d26ea6eacf3405af23a061b2fbce",
        )
        .expect("remote reusable reference");
        let (outbound, _incoming) = mpsc::channel(128);
        let (_cancel_tx, cancel) = watch::channel(false);
        let workflow_commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::default()));
        let repository_access = local_repository_access(&workflow_commands);
        let workflow = load_remote_reusable_workflow(
            &reference,
            &run_dir,
            &run,
            &cancel,
            &outbound,
            &Arc::new(AtomicU64::new(0)),
            &repository_access,
        )
        .await
        .expect("download reusable workflow");
        assert!(workflow.trigger.as_mapping().is_some_and(|trigger| {
            trigger.contains_key(YamlValue::String("workflow_call".to_owned()))
        }));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn matrix_fail_fast_cancels_running_and_queued_instances() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        let repository = fixture.path().join("repository");
        std::fs::create_dir_all(&repository).expect("repository directory");
        git(&repository, ["init", "--initial-branch=main"]);
        git(
            &repository,
            ["config", "user.email", "gitzero@example.test"],
        );
        git(&repository, ["config", "user.name", "GitZero Test"]);
        std::fs::write(repository.join("README.md"), "fixture\n").expect("fixture file");
        git(&repository, ["add", "."]);
        git(&repository, ["commit", "-m", "fixture"]);
        let head_sha = git_output(&repository, ["rev-parse", "HEAD"]);

        let workflow = parse(
            r#"
name: Fail fast
on: pull_request
jobs:
  matrix:
    runs-on: macos-latest
    strategy:
      fail-fast: true
      max-parallel: 2
      matrix:
        case: [fail, slow, queued]
    steps:
      - run: |
          barrier="$(dirname "$GITHUB_EVENT_PATH")/_test-barriers/fail-fast"
          mkdir -p "$barrier"
          case "${{ matrix.case }}" in
            fail)
              for attempt in {1..400}; do
                [[ -f "$barrier/slow-started" ]] && break
                sleep 0.025
              done
              test -f "$barrier/slow-started"
              exit 17
              ;;
            slow)
              touch "$barrier/slow-started"
              echo slow-started
              sleep 10
              echo slow-completed
              ;;
            queued)
              echo queued-ran
              ;;
          esac
"#,
        )
        .expect("parse workflow");
        let plan =
            gitzero_workflow::compile(&workflow, Path::new(".github/workflows/fail-fast.yml"))
                .expect("compile workflow");
        let run_dir = fixture.path().join("run");
        let tool_cache = fixture.path().join("toolcache");
        tokio::fs::create_dir_all(&run_dir).await.expect("run dir");
        tokio::fs::create_dir_all(&tool_cache)
            .await
            .expect("tool cache");
        let run = fixture_run(
            Uuid::new_v4(),
            head_sha.clone(),
            head_sha,
            repository.display().to_string(),
        );
        let environment =
            github_environment(&run, &repository, &run_dir, &tool_cache, "GitZero Test")
                .await
                .expect("GitHub environment");
        let (_cancel_tx, cancel) = watch::channel(false);
        let (outbound, mut incoming) = mpsc::channel(128);
        let sequence = Arc::new(AtomicU64::new(0));
        let execution_slots = Arc::new(Semaphore::new(4));
        let runner_targeting = default_runner_targeting();
        let job_summaries = Arc::new(Mutex::new(Vec::new()));
        let environment_variable_cache = Arc::new(Mutex::new(BTreeMap::new()));
        let workflow_commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::default()));
        let concurrency = ConcurrencyClient::local();
        let repository_access = local_repository_access(&workflow_commands);
        let execution = execute_plan(
            run.id,
            &plan,
            0,
            &run,
            &repository,
            &run_dir,
            &environment,
            &cancel,
            &outbound,
            &sequence,
            &execution_slots,
            &runner_targeting,
            &job_summaries,
            &environment_variable_cache,
            &workflow_commands,
            &concurrency,
            &repository_access,
        );
        tokio::pin!(execution);
        let mut events = Vec::new();
        let result = loop {
            tokio::select! {
                result = &mut execution => break result.expect("execute plan"),
                message = incoming.recv() => {
                    events.push(message.expect("event channel closed during plan execution"));
                }
            }
        };
        while let Ok(message) = incoming.try_recv() {
            events.push(message);
        }

        assert_eq!(result.failed_jobs, ["Fail fast / matrix"]);
        let logs = events
            .iter()
            .filter_map(|message| match message {
                AgentMessage::LogChunk { data, .. } => Some(data.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(logs.contains(&"slow-started\n"));
        assert!(logs.contains(&"Skipped by matrix fail-fast.\n"));
        assert!(!logs.contains(&"slow-completed\n"));
        assert!(!logs.contains(&"queued-ran\n"));
        assert!(
            events.iter().any(|message| matches!(
                message,
                AgentMessage::StepFinished {
                    conclusion: Conclusion::Cancelled,
                    ..
                }
            )),
            "expected cancelled step event; received {events:?}"
        );
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn reusable_call_matrix_fail_fast_cancels_active_and_queued_workflows() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        let repository = fixture.path().join("repository");
        std::fs::create_dir_all(&repository).expect("repository directory");
        git(&repository, ["init", "--initial-branch=main"]);
        git(
            &repository,
            ["config", "user.email", "gitzero@example.test"],
        );
        git(&repository, ["config", "user.name", "GitZero Test"]);
        std::fs::write(repository.join("README.md"), "fixture\n").expect("fixture file");
        git(&repository, ["add", "."]);
        git(&repository, ["commit", "-m", "fixture"]);
        let head_sha = git_output(&repository, ["rev-parse", "HEAD"]);

        let workflow = parse(
            r#"
name: Reusable fail fast
on: pull_request
jobs:
  reusable:
    strategy:
      fail-fast: true
      max-parallel: 2
      matrix:
        case: [fail, slow, queued]
    uses: ./.github/workflows/reusable.yml
    with:
      case: ${{ matrix.case }}
"#,
        )
        .expect("parse caller workflow");
        let reusable = parse(
            r#"
name: Called
on:
  workflow_call:
    inputs:
      case:
        required: true
        type: string
jobs:
  execute:
    runs-on: macos-latest
    steps:
      - run: |
          barrier="$(dirname "$GITHUB_EVENT_PATH")/_test-barriers/reusable-fail-fast"
          mkdir -p "$barrier"
          case "${{ inputs.case }}" in
            fail)
              for attempt in {1..400}; do
                [[ -f "$barrier/slow-started" ]] && break
                sleep 0.025
              done
              test -f "$barrier/slow-started"
              exit 17
              ;;
            slow)
              touch "$barrier/slow-started"
              echo reusable-slow-started
              sleep 10
              echo reusable-slow-completed
              ;;
            queued)
              echo reusable-queued-ran
              ;;
          esac
"#,
        )
        .expect("parse called workflow");
        let catalog = BTreeMap::from([(".github/workflows/reusable.yml".to_owned(), reusable)]);
        let plan =
            compile_with_reusables(&workflow, Path::new(".github/workflows/ci.yml"), &catalog)
                .expect("compile reusable matrix");
        let run_dir = fixture.path().join("run");
        let tool_cache = fixture.path().join("toolcache");
        tokio::fs::create_dir_all(&run_dir).await.expect("run dir");
        tokio::fs::create_dir_all(&tool_cache)
            .await
            .expect("tool cache");
        let run = fixture_run(
            Uuid::new_v4(),
            head_sha.clone(),
            head_sha,
            repository.display().to_string(),
        );
        let environment =
            github_environment(&run, &repository, &run_dir, &tool_cache, "GitZero Test")
                .await
                .expect("GitHub environment");
        let (_cancel_tx, cancel) = watch::channel(false);
        let (outbound, mut incoming) = mpsc::channel(128);
        let sequence = Arc::new(AtomicU64::new(0));
        let execution_slots = Arc::new(Semaphore::new(4));
        let runner_targeting = default_runner_targeting();
        let job_summaries = Arc::new(Mutex::new(Vec::new()));
        let environment_variable_cache = Arc::new(Mutex::new(BTreeMap::new()));
        let workflow_commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::default()));
        let concurrency = ConcurrencyClient::local();
        let repository_access = local_repository_access(&workflow_commands);
        let execution = execute_plan(
            run.id,
            &plan,
            0,
            &run,
            &repository,
            &run_dir,
            &environment,
            &cancel,
            &outbound,
            &sequence,
            &execution_slots,
            &runner_targeting,
            &job_summaries,
            &environment_variable_cache,
            &workflow_commands,
            &concurrency,
            &repository_access,
        );
        tokio::pin!(execution);
        let mut events = Vec::new();
        let result = loop {
            tokio::select! {
                result = &mut execution => break result.expect("execute plan"),
                message = incoming.recv() => {
                    events.push(message.expect("event channel closed during plan execution"));
                }
            }
        };
        while let Ok(message) = incoming.try_recv() {
            events.push(message);
        }

        assert_eq!(result.failed_jobs, ["Reusable fail fast / reusable"]);
        let logs = events
            .iter()
            .filter_map(|message| match message {
                AgentMessage::LogChunk { data, .. } => Some(data.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(logs.contains(&"reusable-slow-started\n"));
        assert!(logs.contains(&"Skipped by reusable-call matrix fail-fast.\n"));
        assert!(!logs.contains(&"reusable-slow-completed\n"));
        assert!(!logs.contains(&"reusable-queued-ran\n"));
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::StepFinished {
                conclusion: Conclusion::Cancelled,
                ..
            }
        )));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn run_cancellation_drains_all_concurrent_job_processes() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        let repository = fixture.path().join("repository");
        std::fs::create_dir_all(&repository).expect("repository directory");
        git(&repository, ["init", "--initial-branch=main"]);
        git(
            &repository,
            ["config", "user.email", "gitzero@example.test"],
        );
        git(&repository, ["config", "user.name", "GitZero Test"]);
        std::fs::write(repository.join("README.md"), "fixture\n").expect("fixture file");
        git(&repository, ["add", "."]);
        git(&repository, ["commit", "-m", "fixture"]);
        let head_sha = git_output(&repository, ["rev-parse", "HEAD"]);

        let workflow = parse(
            r#"
name: Cancellation
on: pull_request
jobs:
  first:
    runs-on: macos-latest
    steps:
      - run: |
          barrier="$(dirname "$GITHUB_EVENT_PATH")/_test-barriers/cancellation"
          mkdir -p "$barrier"
          touch "$barrier/first"
          for attempt in {1..400}; do
            [[ -f "$barrier/second" ]] && break
            sleep 0.025
          done
          test -f "$barrier/second"
          echo first-started
          sleep 10
  second:
    runs-on: macos-latest
    steps:
      - run: |
          barrier="$(dirname "$GITHUB_EVENT_PATH")/_test-barriers/cancellation"
          mkdir -p "$barrier"
          touch "$barrier/second"
          for attempt in {1..400}; do
            [[ -f "$barrier/first" ]] && break
            sleep 0.025
          done
          test -f "$barrier/first"
          echo second-started
          sleep 10
"#,
        )
        .expect("parse workflow");
        let plan =
            gitzero_workflow::compile(&workflow, Path::new(".github/workflows/cancellation.yml"))
                .expect("compile workflow");
        let run_dir = fixture.path().join("run");
        let tool_cache = fixture.path().join("toolcache");
        tokio::fs::create_dir_all(&run_dir).await.expect("run dir");
        tokio::fs::create_dir_all(&tool_cache)
            .await
            .expect("tool cache");
        let run = fixture_run(
            Uuid::new_v4(),
            head_sha.clone(),
            head_sha,
            repository.display().to_string(),
        );
        let environment =
            github_environment(&run, &repository, &run_dir, &tool_cache, "GitZero Test")
                .await
                .expect("GitHub environment");
        let (cancel_tx, cancel) = watch::channel(false);
        let (outbound, mut incoming) = mpsc::channel(128);
        let sequence = Arc::new(AtomicU64::new(0));
        let execution_slots = Arc::new(Semaphore::new(2));
        let runner_targeting = default_runner_targeting();
        let job_summaries = Arc::new(Mutex::new(Vec::new()));
        let environment_variable_cache = Arc::new(Mutex::new(BTreeMap::new()));
        let workflow_commands = Arc::new(StdMutex::new(WorkflowCommandProcessor::default()));
        let concurrency = ConcurrencyClient::local();
        let repository_access = local_repository_access(&workflow_commands);
        let execution = execute_plan(
            run.id,
            &plan,
            0,
            &run,
            &repository,
            &run_dir,
            &environment,
            &cancel,
            &outbound,
            &sequence,
            &execution_slots,
            &runner_targeting,
            &job_summaries,
            &environment_variable_cache,
            &workflow_commands,
            &concurrency,
            &repository_access,
        );
        tokio::pin!(execution);
        let mut events = Vec::new();
        let mut started = BTreeSet::new();
        let mut cancellation_started = None;
        let result = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                tokio::select! {
                    result = &mut execution => break result,
                    message = incoming.recv() => {
                        let message = message.expect("event channel closed during plan execution");
                        if let AgentMessage::LogChunk { data, .. } = &message
                            && matches!(data.as_str(), "first-started\n" | "second-started\n")
                        {
                            started.insert(data.clone());
                            if started.len() == 2 && cancellation_started.is_none() {
                                cancellation_started = Some(tokio::time::Instant::now());
                                cancel_tx.send_replace(true);
                            }
                        }
                        events.push(message);
                    }
                }
            }
        })
        .await
        .expect("all concurrent jobs should stop promptly after cancellation");
        let error = result.expect_err("plan should report cancellation");
        while let Ok(message) = incoming.try_recv() {
            events.push(message);
        }

        assert!(format!("{error:#}").contains("run cancelled"));
        assert!(
            cancellation_started
                .expect("cancellation trigger")
                .elapsed()
                < Duration::from_secs(2)
        );
        assert_eq!(
            events
                .iter()
                .filter(|message| matches!(
                    message,
                    AgentMessage::StepFinished {
                        conclusion: Conclusion::Cancelled,
                        ..
                    }
                ))
                .count(),
            2,
            "expected both running steps to finish as cancelled; received {events:?}"
        );
        let mut temp_directories = tokio::fs::read_dir(run_dir.join("_temp"))
            .await
            .expect("read run temp root");
        let mut isolated_job_temps = Vec::new();
        while let Some(entry) = temp_directories
            .next_entry()
            .await
            .expect("read temp entry")
        {
            if entry.file_name().to_string_lossy().starts_with("job-")
                && entry.file_type().await.expect("temp entry type").is_dir()
            {
                isolated_job_temps.push(entry.path());
            }
        }
        assert_eq!(
            isolated_job_temps.len(),
            2,
            "concurrent jobs did not receive distinct temp roots: {isolated_job_temps:?}"
        );
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn executes_exact_pull_request_execution_snapshots_and_activity_filters() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        let source = fixture.path().join("source");
        let remote = fixture.path().join("remote.git");
        std::fs::create_dir_all(source.join(".github/workflows")).expect("workflow directory");
        git(&source, ["init", "--initial-branch=main"]);
        git(&source, ["config", "user.email", "gitzero@example.test"]);
        git(&source, ["config", "user.name", "GitZero Test"]);
        std::fs::write(source.join("common.txt"), "common\n").expect("common file");
        git(&source, ["add", "."]);
        git(&source, ["commit", "-m", "common base"]);

        git(&source, ["checkout", "-b", "feature"]);
        std::fs::write(source.join("head-only.txt"), "head\n").expect("head file");
        std::fs::write(
            source.join(".github/workflows/merge.yml"),
            r#"
name: Merge snapshot parity
on:
  pull_request:
    types: [opened, closed]
jobs:
  merge:
    runs-on: macos-latest
    steps:
      - id: merge
        uses: actions/checkout@v4
        with:
          show-progress: false
      - run: |
          test "$(git rev-parse HEAD)" = "$GITHUB_SHA"
          test "$GITHUB_REF" = "$GITZERO_EXPECTED_REF"
          test "$GITHUB_REF_NAME" = "$GITZERO_EXPECTED_REF_NAME"
          test "$GITHUB_WORKFLOW_SHA" = "$GITHUB_SHA"
          test "$GITHUB_WORKFLOW_REF" = "local/fixture/.github/workflows/merge.yml@$GITZERO_EXPECTED_REF"
          test "${{ github.sha }}" = "$GITHUB_SHA"
          test "${{ github.ref }}" = "$GITZERO_EXPECTED_REF"
          test "${{ steps.merge.outputs.commit }}" = "$GITHUB_SHA"
          test "${{ steps.merge.outputs.ref }}" = "$GITZERO_EXPECTED_REF"
          if [[ "$GITZERO_EXPECTED_REF" == refs/heads/* ]]; then
            test "$(git symbolic-ref --quiet --short HEAD)" = "$GITZERO_EXPECTED_REF_NAME"
            test "$(git rev-parse --abbrev-ref --symbolic-full-name '@{upstream}')" = "origin/$GITZERO_EXPECTED_REF_NAME"
            test "$(git rev-parse "refs/remotes/origin/$GITZERO_EXPECTED_REF_NAME")" = "$GITHUB_SHA"
          else
            test -z "$(git symbolic-ref --quiet --short HEAD || true)"
          fi
          test -f head-only.txt
          test -f base-only.txt
      - id: head
        uses: actions/checkout@v4
        with:
          path: head-snapshot
          ref: ${{ github.event.pull_request.head.sha }}
          show-progress: false
      - run: |
          test "$(git -C head-snapshot rev-parse HEAD)" = "${{ github.event.pull_request.head.sha }}"
          test -z "$(git -C head-snapshot symbolic-ref --quiet --short HEAD || true)"
          test -f head-snapshot/head-only.txt
          test ! -e head-snapshot/base-only.txt
          test -z "${{ steps.head.outputs.ref }}"
          echo "exact-execution-snapshot-parity:$GITHUB_REF"
"#,
        )
        .expect("merge workflow");
        git(&source, ["add", "."]);
        git(&source, ["commit", "-m", "feature head"]);
        let head_sha = git_output(&source, ["rev-parse", "HEAD"]);

        git(&source, ["checkout", "main"]);
        std::fs::write(source.join("base-only.txt"), "base\n").expect("base file");
        git(&source, ["add", "."]);
        git(&source, ["commit", "-m", "advance base"]);
        let base_sha = git_output(&source, ["rev-parse", "HEAD"]);
        git(&source, ["checkout", "-b", "merge-snapshot"]);
        git(&source, ["merge", "--no-ff", "feature", "-m", "test merge"]);
        let merge_sha = git_output(&source, ["rev-parse", "HEAD"]);
        assert_ne!(merge_sha, head_sha);
        assert_ne!(merge_sha, base_sha);

        git(
            fixture.path(),
            ["init", "--bare", remote.to_str().expect("remote path")],
        );
        git(
            &source,
            [
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path"),
            ],
        );
        git(&source, ["push", "origin", "main"]);
        git(&source, ["push", "origin", "feature:refs/pull/1/head"]);
        git(
            &source,
            ["push", "origin", "merge-snapshot:refs/pull/1/merge"],
        );

        let executor = Executor::new(ExecutorConfig {
            work_root: fixture.path().join("runs"),
            runner_name: "GitZero Merge Test".to_owned(),
            keep_failed_workspaces: false,
            max_parallelism: 2,
            cache_max_bytes: 10 * 1024 * 1024,
            cache_max_entry_bytes: 1024 * 1024,
            artifact_max_bytes: 10 * 1024 * 1024,
            artifact_max_entry_bytes: 1024 * 1024,
        });
        let mut run = fixture_run(
            Uuid::new_v4(),
            head_sha.clone(),
            base_sha.clone(),
            remote.display().to_string(),
        );
        run.pull_request.merge_sha = merge_sha.clone();
        run.environment.insert(
            "GITZERO_EXPECTED_REF".to_owned(),
            "refs/pull/1/merge".to_owned(),
        );
        run.environment
            .insert("GITZERO_EXPECTED_REF_NAME".to_owned(), "1/merge".to_owned());
        let (outbound, mut incoming) = mpsc::channel(256);
        let (_cancel_tx, cancel) = watch::channel(false);

        executor
            .execute(run, cancel, outbound)
            .await
            .expect("execute merge snapshot workflow");
        let mut events = Vec::new();
        while let Ok(message) = incoming.try_recv() {
            events.push(message);
        }
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. }
                if data == "exact-execution-snapshot-parity:refs/pull/1/merge\n"
        )));
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::JobFinished {
                conclusion: Conclusion::Success,
                ..
            }
        )));

        git(&source, ["push", "origin", "merge-snapshot:main"]);
        let mut closed_run = fixture_run(
            Uuid::new_v4(),
            head_sha.clone(),
            base_sha.clone(),
            remote.display().to_string(),
        );
        closed_run.pull_request.action = "closed".to_owned();
        closed_run.pull_request.merge_sha = merge_sha.clone();
        closed_run.pull_request.execution_ref = "refs/heads/main".to_owned();
        closed_run.environment.insert(
            "GITZERO_EXPECTED_REF".to_owned(),
            "refs/heads/main".to_owned(),
        );
        closed_run
            .environment
            .insert("GITZERO_EXPECTED_REF_NAME".to_owned(), "main".to_owned());
        let (outbound, mut incoming) = mpsc::channel(256);
        let (_cancel_tx, cancel) = watch::channel(false);
        executor
            .execute(closed_run, cancel, outbound)
            .await
            .expect("execute merged closed workflow from the base ref");
        let mut events = Vec::new();
        while let Ok(message) = incoming.try_recv() {
            events.push(message);
        }
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. }
                if data == "exact-execution-snapshot-parity:refs/heads/main\n"
        )));
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::JobFinished {
                conclusion: Conclusion::Success,
                ..
            }
        )));

        let mut unmatched_run = fixture_run(
            Uuid::new_v4(),
            head_sha,
            base_sha,
            remote.display().to_string(),
        );
        unmatched_run.pull_request.action = "labeled".to_owned();
        unmatched_run.pull_request.merge_sha = merge_sha;
        let (outbound, mut incoming) = mpsc::channel(256);
        let (_cancel_tx, cancel) = watch::channel(false);
        executor
            .execute(unmatched_run, cancel, outbound)
            .await
            .expect("complete an unmatched activity neutrally");
        let mut events = Vec::new();
        while let Ok(message) = incoming.try_recv() {
            events.push(message);
        }
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::JobFinished {
                conclusion: Conclusion::Neutral,
                summary,
                ..
            } if summary.contains("No workflows matched this pull_request activity")
        )));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn executes_sha256_pull_request_workflow_with_unchanged_checkout() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        let source = fixture.path().join("source");
        let remote = fixture.path().join("remote.git");
        std::fs::create_dir_all(source.join(".github/workflows")).expect("workflow directory");
        git(
            &source,
            ["init", "--object-format=sha256", "--initial-branch=main"],
        );
        git(&source, ["config", "user.email", "gitzero@example.test"]);
        git(&source, ["config", "user.name", "GitZero Test"]);
        std::fs::write(
            source.join(".github/workflows/sha256.yml"),
            r#"
name: SHA-256 parity
on: pull_request
jobs:
  verify:
    runs-on: [self-hosted, macOS]
    steps:
      - run: |
          git init --quiet
          test "$(git rev-parse --show-object-format)" = sha1
      - uses: actions/checkout@v4
      - run: |
          test "${#GITHUB_SHA}" -eq 64
          test "$(git rev-parse --show-object-format)" = sha256
          test "$(git rev-parse HEAD)" = "$GITHUB_SHA"
          echo sha256-checkout-parity
"#,
        )
        .expect("workflow");
        std::fs::write(source.join("README.md"), "SHA-256 fixture\n").expect("fixture file");
        git(&source, ["add", "."]);
        git(&source, ["commit", "-m", "sha256 workflow"]);
        let commit = git_output(&source, ["rev-parse", "HEAD"]);
        assert_eq!(commit.len(), 64);
        git(
            fixture.path(),
            [
                "init",
                "--bare",
                "--object-format=sha256",
                remote.to_str().expect("remote path"),
            ],
        );
        git(
            &source,
            [
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path"),
            ],
        );
        git(&source, ["push", "origin", "main"]);
        git(&source, ["push", "origin", "HEAD:refs/pull/1/head"]);
        git(&source, ["push", "origin", "HEAD:refs/pull/1/merge"]);
        git(&remote, ["symbolic-ref", "HEAD", "refs/heads/main"]);

        let executor = Executor::new(ExecutorConfig {
            work_root: fixture.path().join("runs"),
            runner_name: "GitZero SHA-256 Test".to_owned(),
            keep_failed_workspaces: true,
            max_parallelism: 1,
            cache_max_bytes: 10 * 1024 * 1024,
            cache_max_entry_bytes: 1024 * 1024,
            artifact_max_bytes: 10 * 1024 * 1024,
            artifact_max_entry_bytes: 1024 * 1024,
        });
        let run = fixture_run(
            Uuid::new_v4(),
            commit.clone(),
            commit,
            remote.display().to_string(),
        );
        let (outbound, mut incoming) = mpsc::channel(128);
        let (_cancel_tx, cancel) = watch::channel(false);
        executor
            .execute(run, cancel, outbound)
            .await
            .expect("execute SHA-256 workflow");
        let mut events = Vec::new();
        while let Ok(message) = incoming.try_recv() {
            events.push(message);
        }
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. } if data == "sha256-checkout-parity\n"
        )));
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::JobFinished {
                conclusion: Conclusion::Success,
                ..
            }
        )));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn executes_a_supported_workflow_from_an_exact_pull_request_ref() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        let source = fixture.path().join("source");
        let remote = fixture.path().join("remote.git");
        std::fs::create_dir_all(&source).expect("source directory");
        git(&source, ["init", "--initial-branch=main"]);
        git(&source, ["config", "user.email", "gitzero@example.test"]);
        git(&source, ["config", "user.name", "GitZero Test"]);
        std::fs::write(source.join("README.md"), "base\n").expect("base file");
        git(&source, ["add", "."]);
        git(&source, ["commit", "-m", "base"]);
        let base_sha = git_output(&source, ["rev-parse", "HEAD"]);

        let parity = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/parity");
        for relative in [
            ".github/workflows/ci.yml",
            ".github/workflows/secondary.yml",
            ".github/workflows/reusable.yml",
            ".github/workflows/reusable-leaf.yml",
            ".github/actions/javascript/action.yml",
            ".github/actions/javascript/index.js",
            ".github/actions/javascript/post.js",
            ".github/actions/composite/action.yml",
        ] {
            let destination = source.join(relative);
            std::fs::create_dir_all(destination.parent().expect("fixture parent"))
                .expect("fixture directory");
            std::fs::copy(parity.join(relative), destination).expect("copy parity fixture");
        }
        git(&source, ["add", "."]);
        git(&source, ["commit", "-m", "add workflow"]);
        let head_sha = git_output(&source, ["rev-parse", "HEAD"]);
        git(
            fixture.path(),
            ["init", "--bare", remote.to_str().expect("remote path")],
        );
        git(
            &source,
            [
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path"),
            ],
        );
        git(&source, ["push", "origin", "main"]);
        git(&source, ["push", "origin", "HEAD:refs/pull/1/head"]);
        git(&source, ["push", "origin", "HEAD:refs/pull/1/merge"]);

        let work_root = fixture.path().join("runs");
        let executor = Executor::new(ExecutorConfig {
            work_root,
            runner_name: "GitZero Test".to_owned(),
            keep_failed_workspaces: false,
            max_parallelism: 4,
            cache_max_bytes: 10 * 1024 * 1024,
            cache_max_entry_bytes: 1024 * 1024,
            artifact_max_bytes: 10 * 1024 * 1024,
            artifact_max_entry_bytes: 1024 * 1024,
        })
        .with_runner_targeting(
            ["self-hosted", "macOS", "ARM64", "xcode-16"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            Some("parity-minis".to_owned()),
        );
        let (outbound, mut incoming) = mpsc::channel(128);
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let mut run = fixture_run(
            Uuid::new_v4(),
            head_sha,
            base_sha,
            remote.display().to_string(),
        );
        let workflow_token = "fixture-github-token-value";
        run.checkout_token = workflow_token.to_owned();
        run.checkout_token_expires_at_epoch_seconds = Some(4_102_444_800);
        run.changed_paths = Some(vec![".github/workflows/ci.yml".to_owned()]);
        run.variables = BTreeMap::from([
            ("ENABLED".to_owned(), "true".to_owned()),
            ("SHARDS".to_owned(), r#"["one","two"]"#.to_owned()),
            ("CHANNEL".to_owned(), "stable".to_owned()),
        ]);

        let mut events = Vec::new();
        let execution = executor.execute(run, cancel_rx, outbound);
        tokio::pin!(execution);
        loop {
            tokio::select! {
                result = &mut execution => {
                    result.expect("execute workflow");
                    break;
                }
                message = incoming.recv() => {
                    match message {
                        Some(message) => events.push(message),
                        None => {
                            execution.await.expect("execute workflow");
                            break;
                        }
                    }
                }
            }
        }
        while let Ok(message) = incoming.try_recv() {
            events.push(message);
        }
        for expected in [
            "workflow-alpha\n",
            "workflow-beta\n",
            "workflow-gamma\n",
            "matrix-parallel-alpha\n",
            "matrix-parallel-beta\n",
            "matrix-parallel-gamma\n",
            "hashfiles-alpha\n",
            "hashfiles-beta\n",
            "hashfiles-gamma\n",
            "javascript-alpha\n",
            "javascript-beta\n",
            "javascript-gamma\n",
            "javascript-token-available\n",
            "legacy-output-alpha\n",
            "legacy-output-beta\n",
            "legacy-output-gamma\n",
            "dynamic-secret=***\n",
            "Skip output 'dynamic-secret' because it may contain a masked value.\n",
            "Skip output 'legacy-secret' because it may contain a masked value.\n",
            "Skip output 'token' because it may contain a masked value.\n",
            "dynamic-output-rejected-alpha\n",
            "dynamic-output-rejected-beta\n",
            "dynamic-output-rejected-gamma\n",
            "action-output-alpha\n",
            "action-output-beta\n",
            "action-output-gamma\n",
            "composite-alpha\n",
            "composite-beta\n",
            "composite-gamma\n",
            "composite-tolerated-alpha\n",
            "composite-tolerated-beta\n",
            "composite-tolerated-gamma\n",
            "composite-failure-status-fatal-alpha\n",
            "composite-failure-status-fatal-beta\n",
            "composite-failure-status-fatal-gamma\n",
            "composite-failure-propagated-alpha\n",
            "composite-failure-propagated-beta\n",
            "composite-failure-propagated-gamma\n",
            "composite-output-composite-alpha\n",
            "composite-output-composite-beta\n",
            "composite-output-composite-gamma\n",
            "tolerated-alpha\n",
            "tolerated-beta\n",
            "tolerated-gamma\n",
            "default-bash-no-pipefail\n",
            "matrix-finished-alpha\n",
            "matrix-finished-beta\n",
            "matrix-finished-gamma\n",
            "post-alpha\n",
            "post-beta\n",
            "post-gamma\n",
            "legacy-post-alpha\n",
            "legacy-post-beta\n",
            "legacy-post-gamma\n",
            "independent-parallel-a\n",
            "independent-parallel-b\n",
            "builtin-token-available\n",
            "token-value=***\n",
            "workflow-parallel-ci\n",
            "workflow-parallel-secondary\n",
            "configured-var-one-stable\n",
            "configured-var-two-stable\n",
            "dynamic-color-red\n",
            "dynamic-color-blue\n",
            "dynamic-project-api-debug\n",
            "dynamic-project-app-release\n",
            "dependent-success\n",
            "needs-output-composite-gamma\n",
            "continued-job-success\n",
            "builtin-token-success\n",
            "dynamic-dependencies-success-success\n",
            "dynamic-skipped-skipped\n",
            "parallel-dependencies-success-success\n",
            "reusable-input-parity-true\n",
            "reusable-token-available\n",
            "reusable-token-alias-available\n",
            "reusable-inner-reusable-parity\n",
            "reusable-leaf-reusable-parity\n",
            "reusable-leaf-token-alias-available\n",
            "reusable-nested-output-leaf-reusable-parity\n",
            "reusable-output-leaf-reusable-parity\n",
            "reusable-input-matrix-alpha-true\n",
            "reusable-input-matrix-beta-true\n",
            "reusable-leaf-reusable-matrix-alpha\n",
            "reusable-leaf-reusable-matrix-beta\n",
            "reusable-matrix-output-leaf-reusable-matrix-beta\n",
            "reusable-input-dynamic-api-debug-true\n",
            "reusable-input-dynamic-app-release-true\n",
            "reusable-input-dynamic-color-red-true\n",
            "reusable-input-dynamic-color-blue-true\n",
            "reusable-dynamic-object-output-leaf-reusable-dynamic-app-release\n",
            "reusable-dynamic-dimension-output-leaf-reusable-dynamic-color-blue\n",
            "reusable-dynamic-skipped-skipped\n",
            "reusable-input-remote-true\n",
            "reusable-inner-reusable-remote\n",
            "reusable-leaf-reusable-remote\n",
            "reusable-nested-output-leaf-reusable-remote\n",
            "reusable-remote-output-leaf-reusable-remote\n",
        ] {
            assert!(
                events.iter().any(|message| matches!(
                    message,
                    AgentMessage::LogChunk { data, .. } if data == expected
                )),
                "missing log {expected:?}; received logs {:?}; finished summaries {:?}",
                events
                    .iter()
                    .filter_map(|message| match message {
                        AgentMessage::LogChunk { data, .. } => Some(data.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
                events
                    .iter()
                    .filter_map(|message| match message {
                        AgentMessage::JobFinished {
                            conclusion,
                            summary,
                            ..
                        } => Some((conclusion, summary)),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            );
        }
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::JobFinished {
                conclusion: Conclusion::Success,
                ..
            }
        )));
        let final_summary = events
            .iter()
            .find_map(|message| match message {
                AgentMessage::JobFinished {
                    conclusion: Conclusion::Success,
                    summary,
                    ..
                } => Some(summary),
                _ => None,
            })
            .expect("successful final summary");
        for expected in [
            "JavaScript summary alpha",
            "Composite summary beta",
            "Post summary gamma",
            "Shell summary available",
            "summary-token=***",
            "dynamic-summary-secret=***",
        ] {
            assert!(
                final_summary.contains(expected),
                "final summary did not contain {expected:?}: {final_summary}"
            );
        }
        assert!(!final_summary.contains("removed-summary-must-not-appear"));
        assert!(!events.iter().any(|message| match message {
            AgentMessage::LogChunk { data, .. } => data.contains(workflow_token),
            AgentMessage::JobFinished { summary, .. } => summary.contains(workflow_token),
            _ => false,
        }));
        for secret in [
            "generated-alpha-secret",
            "generated-beta-secret",
            "generated-gamma-secret",
        ] {
            assert!(!events.iter().any(|message| match message {
                AgentMessage::StepStarted { name, .. } => name.contains(secret),
                AgentMessage::LogChunk { data, .. } => data.contains(secret),
                AgentMessage::JobFinished { summary, .. } => summary.contains(secret),
                _ => false,
            }));
        }
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn jobs_start_empty_and_checkout_can_populate_and_clean_a_nested_path() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        let source = fixture.path().join("source");
        let remote = fixture.path().join("remote.git");
        std::fs::create_dir_all(source.join(".github/workflows"))
            .expect("source workflow directory");
        git(&source, ["init", "--initial-branch=main"]);
        git(&source, ["config", "user.email", "gitzero@example.test"]);
        git(&source, ["config", "user.name", "GitZero Test"]);
        std::fs::create_dir_all(source.join("src")).expect("source src directory");
        std::fs::create_dir_all(source.join("docs")).expect("source docs directory");
        std::fs::write(source.join("README.md"), "base\n").expect("base file");
        std::fs::write(source.join("src/keep.txt"), "keep\n").expect("kept source file");
        std::fs::write(source.join("docs/skip.txt"), "skip\n").expect("skipped source file");
        git(&source, ["add", "README.md", "src", "docs"]);
        git(&source, ["commit", "-m", "base"]);
        let base_sha = git_output(&source, ["rev-parse", "HEAD"]);
        std::fs::write(
            source.join(".github/workflows/checkout.yml"),
            r#"name: Checkout workspace
on: pull_request
jobs:
  checkout:
    runs-on: macos-latest
    steps:
      - name: Empty before checkout
        run: |
          test ! -e README.md
          test ! -e .git
          echo empty-before-checkout
      - name: Nested checkout
        uses: actions/checkout@v4
        with:
          path: nested/repository
          token: ${{ github.token }}
      - name: Verify and dirty checkout
        run: |
          test ! -e README.md
          test ! -e .git
          test "$(git -C nested/repository rev-parse HEAD)" = "$GITHUB_SHA"
          test -z "$(git -C nested/repository symbolic-ref --quiet --short HEAD || true)"
          touch nested/repository/untracked.tmp
          printf dirty >> nested/repository/README.md
          echo nested-exact-checkout
      - name: Repeat checkout
        uses: actions/checkout@v4
        with:
          path: nested/repository
          token: ${{ github.token }}
          clean: true
          persist-credentials: false
      - name: Verify clean checkout
        run: |
          test ! -e nested/repository/untracked.tmp
          test "$(cat nested/repository/README.md)" = base
          test "$(git -C nested/repository rev-parse HEAD)" = "$GITHUB_SHA"
          test -z "$(git -C nested/repository symbolic-ref --quiet --short HEAD || true)"
          test -z "${GIT_CONFIG_COUNT+x}"
          echo repeat-checkout-clean
      - name: Cone sparse checkout
        id: sparse
        uses: actions/checkout@v7
        with:
          path: sparse-cone
          ref: ${{ github.ref }}
          sparse-checkout: |
            .github
            src
          sparse-checkout-cone-mode: true
          show-progress: false
          allow-unsafe-pr-checkout: false
          github-server-url: https://github.com/
      - name: Verify cone sparse checkout
        run: |
          test -f sparse-cone/README.md
          test -f sparse-cone/src/keep.txt
          test -f sparse-cone/.github/workflows/checkout.yml
          test ! -e sparse-cone/docs/skip.txt
          test "$(git -C sparse-cone rev-parse HEAD)" = "$GITHUB_SHA"
          test -z "$(git -C sparse-cone symbolic-ref --quiet --short HEAD || true)"
          test "$(git -C sparse-cone config --get remote.origin.promisor)" = true
          test "$(git -C sparse-cone config --get remote.origin.partialclonefilter)" = blob:none
          test "$(git -C sparse-cone rev-list --objects --missing=print HEAD | grep -c '^?')" -gt 0
          test "${{ steps.sparse.outputs.commit }}" = "$GITHUB_SHA"
          test "${{ steps.sparse.outputs.ref }}" = refs/pull/1/merge
          echo sparse-cone-exact
      - name: Non-cone single-file checkout
        id: sparse_file
        uses: actions/checkout@v6
        with:
          path: sparse-file
          ref: ${{ github.head_ref }}
          sparse-checkout: README.md
          sparse-checkout-cone-mode: false
          filter: blob:none
          show-progress: false
      - name: Verify non-cone sparse checkout
        run: |
          test -f sparse-file/README.md
          test ! -e sparse-file/src
          test ! -e sparse-file/docs
          test ! -e sparse-file/.github
          test "$(git -C sparse-file config --bool --get core.sparseCheckoutCone)" = false
          test "$(git -C sparse-file rev-parse HEAD)" = "$GITHUB_SHA"
          test "$(git -C sparse-file symbolic-ref --quiet --short HEAD)" = feature
          test "$(git -C sparse-file rev-parse --abbrev-ref --symbolic-full-name '@{upstream}')" = origin/feature
          test "$(git -C sparse-file rev-parse refs/remotes/origin/feature)" = "$GITHUB_SHA"
          git -C sparse-file push --dry-run
          test "${{ steps.sparse_file.outputs.commit }}" = "$GITHUB_SHA"
          test "${{ steps.sparse_file.outputs.ref }}" = feature
          echo sparse-file-exact
      - name: Disable sparse checkout on repeat
        uses: actions/checkout@v6
        with:
          path: sparse-file
          ref: refs/pull/1/head
          show-progress: false
      - name: Verify full tree after repeat
        run: |
          test -f sparse-file/README.md
          test -f sparse-file/src/keep.txt
          test -f sparse-file/docs/skip.txt
          test -f sparse-file/.github/workflows/checkout.yml
          test "$(git -C sparse-file rev-parse HEAD)" = "$GITHUB_SHA"
          test -z "$(git -C sparse-file symbolic-ref --quiet --short HEAD || true)"
          test "$(git -C sparse-file config --bool --get core.sparseCheckout || true)" != true
          echo sparse-disabled-full-tree
      - name: Checkout authenticated base snapshot
        id: base
        uses: actions/checkout@v6
        with:
          path: base-snapshot
          ref: ${{ github.event.pull_request.base.sha }}
          show-progress: false
      - name: Verify base snapshot
        run: |
          test "$(git -C base-snapshot rev-parse HEAD)" = "${{ github.event.pull_request.base.sha }}"
          test -z "$(git -C base-snapshot symbolic-ref --quiet --short HEAD || true)"
          test ! -e base-snapshot/.github/workflows/checkout.yml
          test "${{ steps.base.outputs.commit }}" = "${{ github.event.pull_request.base.sha }}"
          test -z "${{ steps.base.outputs.ref }}"
          echo base-snapshot-exact
      - name: Checkout authenticated base branch
        id: base_branch
        uses: actions/checkout@v6
        with:
          path: base-branch
          ref: ${{ github.base_ref }}
          fetch-depth: 0
          show-progress: false
      - name: Verify base branch
        run: |
          test "$(git -C base-branch rev-parse HEAD)" = "${{ github.event.pull_request.base.sha }}"
          test "$(git -C base-branch symbolic-ref --quiet --short HEAD)" = main
          test "$(git -C base-branch rev-parse --abbrev-ref --symbolic-full-name '@{upstream}')" = origin/main
          test "$(git -C base-branch rev-parse refs/remotes/origin/main)" = "${{ github.event.pull_request.base.sha }}"
          test "${{ steps.base_branch.outputs.ref }}" = main
          echo base-branch-exact
"#,
        )
        .expect("workflow file");
        git(&source, ["add", ".github/workflows/checkout.yml"]);
        git(&source, ["commit", "-m", "add checkout workflow"]);
        let head_sha = git_output(&source, ["rev-parse", "HEAD"]);
        git(
            fixture.path(),
            ["init", "--bare", remote.to_str().expect("remote path")],
        );
        git(&remote, ["config", "uploadpack.allowFilter", "true"]);
        git(&remote, ["config", "uploadpack.allowAnySHA1InWant", "true"]);
        git(
            &source,
            [
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path"),
            ],
        );
        git(&source, ["push", "origin", "main"]);
        git(&source, ["push", "origin", "HEAD:refs/pull/1/head"]);
        git(&source, ["push", "origin", "HEAD:refs/pull/1/merge"]);

        let executor = Executor::new(ExecutorConfig {
            work_root: fixture.path().join("runs"),
            runner_name: "GitZero Test".to_owned(),
            keep_failed_workspaces: false,
            max_parallelism: 2,
            cache_max_bytes: 10 * 1024 * 1024,
            cache_max_entry_bytes: 1024 * 1024,
            artifact_max_bytes: 10 * 1024 * 1024,
            artifact_max_entry_bytes: 1024 * 1024,
        });
        let (outbound, mut incoming) = mpsc::channel(128);
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let mut run = fixture_run(
            Uuid::new_v4(),
            head_sha,
            base_sha,
            format!("file://{}", remote.display()),
        );
        run.checkout_token = "fixture-built-in-token".to_owned();
        run.checkout_token_expires_at_epoch_seconds = Some(4_102_444_800);

        let execution = executor.execute(run, cancel_rx, outbound);
        tokio::pin!(execution);
        let mut events = Vec::new();
        loop {
            tokio::select! {
                result = &mut execution => {
                    result.expect("execute nested checkout workflow");
                    break;
                }
                message = incoming.recv() => {
                    match message {
                        Some(message) => events.push(message),
                        None => {
                            execution.await.expect("execute nested checkout workflow");
                            break;
                        }
                    }
                }
            }
        }
        while let Ok(message) = incoming.try_recv() {
            events.push(message);
        }

        for expected in [
            "empty-before-checkout\n",
            "nested-exact-checkout\n",
            "repeat-checkout-clean\n",
            "sparse-cone-exact\n",
            "sparse-file-exact\n",
            "sparse-disabled-full-tree\n",
            "base-snapshot-exact\n",
            "base-branch-exact\n",
        ] {
            assert!(
                events.iter().any(|message| matches!(
                    message,
                    AgentMessage::LogChunk { data, .. } if data == expected
                )),
                "missing checkout log {expected:?}; events: {events:?}"
            );
        }
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::JobFinished {
                conclusion: Conclusion::Success,
                ..
            }
        )));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn unchanged_action_clients_restore_and_save_through_the_local_cache_service() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        let source = fixture.path().join("source");
        std::fs::create_dir_all(&source).expect("source directory");
        git(&source, ["init", "--initial-branch=main"]);
        git(&source, ["config", "user.email", "gitzero@example.test"]);
        git(&source, ["config", "user.name", "GitZero Test"]);
        std::fs::write(source.join("README.md"), "cache fixture\n").expect("fixture file");

        let action = source.join(".github/actions/cache-client");
        std::fs::create_dir_all(&action).expect("action directory");
        std::fs::write(
            action.join("action.yml"),
            r#"
name: Fixture cache client
inputs:
  key:
    required: true
outputs:
  cache-hit:
    description: Exact cache hit
runs:
  using: node24
  main: main.js
  post: post.js
  post-if: success()
"#,
        )
        .expect("action metadata");
        std::fs::write(
            action.join("main.js"),
            r#"
const fs = require("node:fs");
const path = require("node:path");

async function run() {
  const key = process.env.INPUT_KEY;
  const version = "fixture-v1";
  const headers = { Authorization: `Bearer ${process.env.ACTIONS_RUNTIME_TOKEN}` };
  const lookup = new URL("_apis/artifactcache/cache", process.env.ACTIONS_CACHE_URL);
  lookup.searchParams.set("keys", key);
  lookup.searchParams.set("version", version);
  const response = await fetch(lookup, { headers });
  fs.appendFileSync(process.env.GITHUB_STATE, `key=${key}\nversion=${version}\n`);
  if (response.status === 204) {
    fs.appendFileSync(process.env.GITHUB_OUTPUT, "cache-hit=\n");
    fs.appendFileSync(process.env.GITHUB_STATE, "cache_hit=false\n");
    console.log("fixture-cache-client-miss");
    return;
  }
  if (!response.ok) throw new Error(`lookup failed: ${response.status}`);
  const entry = await response.json();
  const archive = await fetch(entry.archiveLocation);
  if (!archive.ok) throw new Error(`download failed: ${archive.status}`);
  const bytes = Buffer.from(await archive.arrayBuffer());
  fs.mkdirSync(path.join(process.cwd(), ".fixture-cache"), { recursive: true });
  fs.writeFileSync(path.join(process.cwd(), ".fixture-cache/payload"), bytes);
  fs.appendFileSync(process.env.GITHUB_OUTPUT, `cache-hit=${entry.cacheKey === key}\n`);
  fs.appendFileSync(process.env.GITHUB_STATE, "cache_hit=true\n");
  console.log("fixture-cache-client-restored");
}

run().catch(error => { console.error(error); process.exitCode = 1; });
"#,
        )
        .expect("main action");
        std::fs::write(
            action.join("post.js"),
            r#"
const fs = require("node:fs");
const path = require("node:path");

async function run() {
  if (process.env.STATE_cache_hit === "true") {
    console.log("fixture-cache-client-hit-not-saved");
    return;
  }
  const bytes = fs.readFileSync(path.join(process.cwd(), ".fixture-cache/payload"));
  const headers = {
    Authorization: `Bearer ${process.env.ACTIONS_RUNTIME_TOKEN}`,
    "Content-Type": "application/json"
  };
  const base = new URL("_apis/artifactcache/caches", process.env.ACTIONS_CACHE_URL);
  const reserve = await fetch(base, {
    method: "POST",
    headers,
    body: JSON.stringify({
      key: process.env.STATE_key,
      version: process.env.STATE_version,
      cacheSize: bytes.length
    })
  });
  if (!reserve.ok) throw new Error(`reserve failed: ${reserve.status}`);
  const { cacheId } = await reserve.json();
  const entry = new URL(`_apis/artifactcache/caches/${cacheId}`, process.env.ACTIONS_CACHE_URL);
  const upload = await fetch(entry, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${process.env.ACTIONS_RUNTIME_TOKEN}`,
      "Content-Type": "application/octet-stream",
      "Content-Range": `bytes 0-${bytes.length - 1}/*`
    },
    body: bytes
  });
  if (!upload.ok) throw new Error(`upload failed: ${upload.status}`);
  const commit = await fetch(entry, {
    method: "POST",
    headers,
    body: JSON.stringify({ size: bytes.length })
  });
  if (!commit.ok) throw new Error(`commit failed: ${commit.status}`);
  console.log("fixture-cache-client-saved");
}

run().catch(error => { console.error(error); process.exitCode = 1; });
"#,
        )
        .expect("post action");

        let workflow = source.join(".github/workflows/cache.yml");
        std::fs::create_dir_all(workflow.parent().expect("workflow parent"))
            .expect("workflow directory");
        std::fs::write(
            &workflow,
            r#"
name: Cache compatibility
on: pull_request
jobs:
  cache:
    runs-on: macos-latest
    steps:
      - uses: actions/checkout@v6
      - id: cache
        uses: ./.github/actions/cache-client
        with:
          key: fixture-key
      - run: |
          if [[ "${{ steps.cache.outputs.cache-hit }}" == "true" ]]; then
            test "$(cat .fixture-cache/payload)" = "immutable cached payload"
            echo fixture-workflow-cache-hit
          else
            mkdir -p .fixture-cache
            printf 'immutable cached payload' > .fixture-cache/payload
            echo fixture-workflow-cache-miss
          fi
      - run: echo "fixture-cache-runtime-token=$ACTIONS_RUNTIME_TOKEN"
"#,
        )
        .expect("workflow");
        git(&source, ["add", "."]);
        git(&source, ["commit", "-m", "cache fixture"]);
        let head_sha = git_output(&source, ["rev-parse", "HEAD"]);
        let remote = fixture.path().join("remote.git");
        git(
            fixture.path(),
            ["init", "--bare", remote.to_str().expect("remote path")],
        );
        git(
            &source,
            [
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path"),
            ],
        );
        git(&source, ["push", "origin", "main"]);
        git(&source, ["push", "origin", "HEAD:refs/pull/1/head"]);
        git(&source, ["push", "origin", "HEAD:refs/pull/1/merge"]);

        let executor = Executor::new(ExecutorConfig {
            work_root: fixture.path().join("runs"),
            runner_name: "GitZero Test".to_owned(),
            keep_failed_workspaces: false,
            max_parallelism: 2,
            cache_max_bytes: 10 * 1024 * 1024,
            cache_max_entry_bytes: 1024 * 1024,
            artifact_max_bytes: 10 * 1024 * 1024,
            artifact_max_entry_bytes: 1024 * 1024,
        });
        let execute_once = |run_id| {
            let run = fixture_run(
                run_id,
                head_sha.clone(),
                head_sha.clone(),
                remote.display().to_string(),
            );
            async {
                let (_cancel_tx, cancel) = watch::channel(false);
                let (outbound, mut incoming) = mpsc::channel(256);
                let drain = tokio::spawn(async move {
                    let mut events = Vec::new();
                    while let Some(event) = incoming.recv().await {
                        events.push(event);
                    }
                    events
                });
                executor
                    .execute(run, cancel, outbound)
                    .await
                    .expect("execute cache workflow");
                drain.await.expect("join event drain")
            }
        };

        let first = execute_once(Uuid::new_v4()).await;
        assert!(first.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. } if data == "fixture-cache-client-miss\n"
        )));
        assert!(first.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. } if data == "fixture-cache-client-saved\n"
        )));
        assert!(first.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. } if data == "fixture-cache-runtime-token=***\n"
        )));
        assert!(first.iter().any(|message| matches!(
            message,
            AgentMessage::JobFinished {
                conclusion: Conclusion::Success,
                ..
            }
        )));

        let second = execute_once(Uuid::new_v4()).await;
        assert!(second.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. } if data == "fixture-cache-client-restored\n"
        )));
        assert!(second.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. } if data == "fixture-workflow-cache-hit\n"
        )));
        assert!(second.iter().any(|message| matches!(
            message,
            AgentMessage::LogChunk { data, .. } if data == "fixture-cache-client-hit-not-saved\n"
        )));
        assert!(second.iter().any(|message| matches!(
            message,
            AgentMessage::JobFinished {
                conclusion: Conclusion::Success,
                ..
            }
        )));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    #[ignore = "requires public GitHub action acquisition"]
    async fn official_artifact_actions_transfer_between_jobs() {
        let fixture = tempfile::tempdir().expect("fixture tempdir");
        let source = fixture.path().join("source");
        std::fs::create_dir_all(&source).expect("source directory");
        git(&source, ["init", "--initial-branch=main"]);
        git(&source, ["config", "user.email", "gitzero@example.test"]);
        git(&source, ["config", "user.name", "GitZero Test"]);
        std::fs::write(source.join("README.md"), "official artifact fixture\n")
            .expect("fixture file");
        let workflow = source.join(".github/workflows/artifact.yml");
        std::fs::create_dir_all(workflow.parent().expect("workflow parent"))
            .expect("workflow directory");
        std::fs::write(
            &workflow,
            r#"
name: Official artifact compatibility
on: pull_request
jobs:
  upload:
    runs-on: macos-latest
    steps:
      - run: printf 'official artifact payload' > payload.txt
      - id: upload
        uses: actions/upload-artifact@v7
        with:
          name: official-fixture
          path: payload.txt
      - run: |
          test -n "${{ steps.upload.outputs.artifact-id }}"
          test -n "${{ steps.upload.outputs.artifact-digest }}"
          echo official-artifact-uploaded
      - run: printf 'replacement artifact payload' > payload.txt
      - uses: actions/upload-artifact@v7
        with:
          name: official-fixture
          path: payload.txt
          overwrite: true
      - run: printf 'direct artifact payload' > direct.txt
      - uses: actions/upload-artifact@v7
        with:
          path: direct.txt
          archive: false
  download:
    needs: upload
    runs-on: macos-latest
    steps:
      - uses: actions/download-artifact@v8
        with:
          name: official-fixture
          path: restored
      - uses: actions/download-artifact@v8
        with:
          name: direct.txt
          path: direct-restored
      - run: |
          test "$(cat restored/payload.txt)" = "replacement artifact payload"
          test "$(cat direct-restored/direct.txt)" = "direct artifact payload"
          echo official-artifact-downloaded
"#,
        )
        .expect("workflow");
        git(&source, ["add", "."]);
        git(&source, ["commit", "-m", "official artifact fixture"]);
        let head_sha = git_output(&source, ["rev-parse", "HEAD"]);
        let remote = fixture.path().join("remote.git");
        git(
            fixture.path(),
            ["init", "--bare", remote.to_str().expect("remote path")],
        );
        git(
            &source,
            [
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path"),
            ],
        );
        git(&source, ["push", "origin", "main"]);
        git(&source, ["push", "origin", "HEAD:refs/pull/1/head"]);
        git(&source, ["push", "origin", "HEAD:refs/pull/1/merge"]);

        let executor = Executor::new(ExecutorConfig {
            work_root: fixture.path().join("runs"),
            runner_name: "GitZero Test".to_owned(),
            keep_failed_workspaces: false,
            max_parallelism: 2,
            cache_max_bytes: 10 * 1024 * 1024,
            cache_max_entry_bytes: 5 * 1024 * 1024,
            artifact_max_bytes: 10 * 1024 * 1024,
            artifact_max_entry_bytes: 5 * 1024 * 1024,
        });
        let run = fixture_run(
            Uuid::new_v4(),
            head_sha.clone(),
            head_sha,
            remote.display().to_string(),
        );
        let (_cancel_tx, cancel) = watch::channel(false);
        let (outbound, mut incoming) = mpsc::channel(512);
        let drain = tokio::spawn(async move {
            let mut events = Vec::new();
            while let Some(event) = incoming.recv().await {
                events.push(event);
            }
            events
        });
        executor
            .execute(run, cancel, outbound)
            .await
            .expect("execute official artifact workflow");
        let events = drain.await.expect("join event drain");
        let logs = events
            .iter()
            .filter_map(|message| match message {
                AgentMessage::LogChunk { data, .. } => Some(data.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            events.iter().any(|message| matches!(
                message,
                AgentMessage::LogChunk { data, .. } if data == "official-artifact-uploaded\n"
            )),
            "artifact workflow logs: {logs:?}"
        );
        assert!(
            events.iter().any(|message| matches!(
                message,
                AgentMessage::LogChunk { data, .. } if data == "official-artifact-downloaded\n"
            )),
            "artifact workflow logs: {logs:?}"
        );
        assert!(events.iter().any(|message| matches!(
            message,
            AgentMessage::JobFinished {
                conclusion: Conclusion::Success,
                ..
            }
        )));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    #[ignore = "requires public GitHub action acquisition"]
    async fn official_actions_cache_restores_an_entry_saved_by_a_prior_run() {
        let fixture_parent = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target");
        std::fs::create_dir_all(&fixture_parent).expect("fixture parent");
        let fixture = tempfile::tempdir_in(fixture_parent).expect("fixture tempdir");
        git(fixture.path(), ["init", "--initial-branch=main"]);
        git(
            fixture.path(),
            ["config", "user.email", "gitzero@example.test"],
        );
        git(fixture.path(), ["config", "user.name", "GitZero Test"]);
        let parent_readme = fixture.path().join("README.md");
        std::fs::write(&parent_readme, "parent repository sentinel\n").expect("parent sentinel");
        git(fixture.path(), ["add", "README.md"]);
        git(fixture.path(), ["commit", "-m", "parent sentinel"]);
        let source = fixture.path().join("source");
        std::fs::create_dir_all(&source).expect("source directory");
        git(&source, ["init", "--initial-branch=main"]);
        git(&source, ["config", "user.email", "gitzero@example.test"]);
        git(&source, ["config", "user.name", "GitZero Test"]);
        std::fs::write(source.join("README.md"), "official cache fixture\n").expect("fixture file");
        let workflow = source.join(".github/workflows/cache.yml");
        std::fs::create_dir_all(workflow.parent().expect("workflow parent"))
            .expect("workflow directory");
        std::fs::write(
            &workflow,
            r#"
name: Official cache compatibility
on: pull_request
jobs:
  cache:
    runs-on: macos-latest
    steps:
      - uses: actions/checkout@v6
      - id: cache
        uses: actions/cache@v5
        with:
          path: .official-cache
          key: official-fixture-key
      - run: |
          if [[ "${{ steps.cache.outputs.cache-hit }}" == "true" ]]; then
            test "$(cat .official-cache/payload)" = "official cached payload"
            echo official-workflow-cache-hit
          else
            mkdir -p .official-cache
            printf 'official cached payload' > .official-cache/payload
            echo official-workflow-cache-miss
          fi
"#,
        )
        .expect("workflow");
        git(&source, ["add", "."]);
        git(&source, ["commit", "-m", "official cache fixture"]);
        let head_sha = git_output(&source, ["rev-parse", "HEAD"]);
        let remote = fixture.path().join("remote.git");
        git(
            fixture.path(),
            ["init", "--bare", remote.to_str().expect("remote path")],
        );
        git(
            &source,
            [
                "remote",
                "add",
                "origin",
                remote.to_str().expect("remote path"),
            ],
        );
        git(&source, ["push", "origin", "main"]);
        git(&source, ["push", "origin", "HEAD:refs/pull/1/head"]);
        git(&source, ["push", "origin", "HEAD:refs/pull/1/merge"]);

        let executor = Executor::new(ExecutorConfig {
            work_root: fixture.path().join("runs"),
            runner_name: "GitZero Test".to_owned(),
            keep_failed_workspaces: false,
            max_parallelism: 2,
            cache_max_bytes: 10 * 1024 * 1024,
            cache_max_entry_bytes: 5 * 1024 * 1024,
            artifact_max_bytes: 10 * 1024 * 1024,
            artifact_max_entry_bytes: 5 * 1024 * 1024,
        });
        let parent_readme_before = std::fs::read(&parent_readme).expect("parent README");
        let execute_once = |run_id| {
            let run = fixture_run(
                run_id,
                head_sha.clone(),
                head_sha.clone(),
                remote.display().to_string(),
            );
            async {
                let (_cancel_tx, cancel) = watch::channel(false);
                let (outbound, mut incoming) = mpsc::channel(512);
                let drain = tokio::spawn(async move {
                    let mut events = Vec::new();
                    while let Some(event) = incoming.recv().await {
                        events.push(event);
                    }
                    events
                });
                executor
                    .execute(run, cancel, outbound)
                    .await
                    .expect("execute official cache workflow");
                drain.await.expect("join event drain")
            }
        };

        let first = execute_once(Uuid::new_v4()).await;
        let first_logs = first
            .iter()
            .filter_map(|message| match message {
                AgentMessage::LogChunk { data, .. } => Some(data.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            std::fs::read(&parent_readme).expect("parent README after first run"),
            parent_readme_before,
            "first cache run modified a parent repository"
        );
        assert!(
            first.iter().any(|message| matches!(
                message,
                AgentMessage::LogChunk { data, .. } if data == "official-workflow-cache-miss\n"
            )),
            "first-run logs: {first_logs:?}"
        );
        assert!(first.iter().any(|message| matches!(
            message,
            AgentMessage::JobFinished {
                conclusion: Conclusion::Success,
                ..
            }
        )));

        let second = execute_once(Uuid::new_v4()).await;
        assert_eq!(
            std::fs::read(&parent_readme).expect("parent README after second run"),
            parent_readme_before,
            "second cache run modified a parent repository"
        );
        let second_logs = second
            .iter()
            .filter_map(|message| match message {
                AgentMessage::LogChunk { data, .. } => Some(data.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            second.iter().any(|message| matches!(
                message,
                AgentMessage::LogChunk { data, .. } if data == "official-workflow-cache-hit\n"
            )),
            "second-run logs: {second_logs:?}"
        );
        assert!(second.iter().any(|message| matches!(
            message,
            AgentMessage::JobFinished {
                conclusion: Conclusion::Success,
                ..
            }
        )));
    }

    fn fixture_run(id: Uuid, head_sha: String, base_sha: String, clone_url: String) -> RunSpec {
        RunSpec {
            id,
            workspace_id: "test".to_owned(),
            installation_id: 0,
            run_number: 0,
            repository: RepositorySpec {
                owner: "local".to_owned(),
                name: "fixture".to_owned(),
                clone_url,
            },
            pull_request: PullRequestSpec {
                number: 1,
                action: "opened".to_owned(),
                merge_sha: head_sha.clone(),
                execution_ref: "refs/pull/1/merge".to_owned(),
                head_sha,
                base_sha,
                head_ref: "feature".to_owned(),
                base_ref: "main".to_owned(),
            },
            check_run_id: None,
            event: JsonValue::Object(Default::default()),
            checkout_token: String::new(),
            checkout_token_expires_at_epoch_seconds: None,
            github_api_version: "2026-03-10".to_owned(),
            managed_secrets: false,
            changed_paths: None,
            environment: BTreeMap::new(),
            variables: BTreeMap::new(),
        }
    }

    #[cfg(target_os = "macos")]
    fn git<const N: usize>(directory: &Path, arguments: [&str; N]) {
        let status = std::process::Command::new("git")
            .args(arguments)
            .current_dir(directory)
            .status()
            .expect("run git");
        assert!(status.success());
    }

    #[cfg(target_os = "macos")]
    fn git_output<const N: usize>(directory: &Path, arguments: [&str; N]) -> String {
        let output = std::process::Command::new("git")
            .args(arguments)
            .current_dir(directory)
            .output()
            .expect("run git");
        assert!(output.status.success());
        String::from_utf8(output.stdout)
            .expect("git UTF-8")
            .trim()
            .to_owned()
    }
}
