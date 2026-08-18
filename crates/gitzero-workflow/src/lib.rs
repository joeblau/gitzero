use gitzero_expression::analyze_template;
use indexmap::IndexMap;
use regex::Regex;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use serde_yaml_ng::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WorkflowError {
    #[error("invalid workflow YAML: {0}")]
    InvalidYaml(#[from] serde_yaml_ng::Error),
    #[error("workflow '{workflow}' job '{job}' is not compatible: {reason}")]
    Incompatible {
        workflow: String,
        job: String,
        reason: String,
    },
    #[error("workflow '{workflow}' is not compatible: {reason}")]
    UnsupportedWorkflow { workflow: String, reason: String },
}

#[derive(Clone, Debug, Deserialize)]
pub struct Workflow {
    #[serde(default = "default_workflow_name")]
    pub name: String,
    #[serde(rename = "on")]
    pub trigger: Value,
    #[serde(default)]
    pub env: BTreeMap<String, Scalar>,
    #[serde(default)]
    pub defaults: Defaults,
    #[serde(default)]
    pub permissions: Option<Value>,
    #[serde(rename = "run-name", default)]
    pub run_name: Option<Scalar>,
    #[serde(default)]
    pub concurrency: Option<Concurrency>,
    pub jobs: IndexMap<String, Job>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Job {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(rename = "runs-on")]
    #[serde(default)]
    pub runs_on: Option<Value>,
    #[serde(default)]
    pub needs: Needs,
    #[serde(default)]
    pub env: BTreeMap<String, Scalar>,
    #[serde(default)]
    pub outputs: BTreeMap<String, Scalar>,
    #[serde(default)]
    pub defaults: Defaults,
    #[serde(default)]
    pub permissions: Option<Value>,
    #[serde(rename = "continue-on-error", default)]
    pub continue_on_error: Option<Scalar>,
    #[serde(rename = "timeout-minutes", default)]
    pub timeout_minutes: Option<Scalar>,
    #[serde(default)]
    pub concurrency: Option<Concurrency>,
    #[serde(default)]
    pub environment: Option<JobEnvironment>,
    #[serde(default)]
    pub steps: Vec<Step>,
    #[serde(default)]
    pub strategy: Option<Value>,
    #[serde(default)]
    pub with: BTreeMap<String, Value>,
    #[serde(default)]
    pub secrets: Option<Value>,
    #[serde(rename = "if", default)]
    pub condition: Option<Scalar>,
    #[serde(default)]
    pub container: Option<Value>,
    #[serde(default)]
    pub services: Option<Value>,
    #[serde(default)]
    pub uses: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum JobEnvironment {
    Name(Scalar),
    Configuration(JobEnvironmentConfiguration),
}

#[derive(Clone, Debug, Deserialize)]
pub struct JobEnvironmentConfiguration {
    pub name: Scalar,
    #[serde(default)]
    pub url: Option<Scalar>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum Concurrency {
    Group(Scalar),
    Configuration(ConcurrencyConfiguration),
}

#[derive(Clone, Debug, Deserialize)]
pub struct ConcurrencyConfiguration {
    pub group: Scalar,
    #[serde(rename = "cancel-in-progress", default)]
    pub cancel_in_progress: Option<Scalar>,
    #[serde(default)]
    pub queue: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Step {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub run: Option<String>,
    #[serde(default)]
    pub uses: Option<String>,
    #[serde(default)]
    pub shell: Option<String>,
    #[serde(rename = "working-directory", default)]
    pub working_directory: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, Scalar>,
    #[serde(default)]
    pub with: BTreeMap<String, Scalar>,
    #[serde(rename = "if", default)]
    pub condition: Option<Scalar>,
    #[serde(rename = "continue-on-error", default)]
    pub continue_on_error: Option<Scalar>,
    #[serde(rename = "timeout-minutes", default)]
    pub timeout_minutes: Option<Scalar>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Needs(pub Vec<String>);

impl<'de> Deserialize<'de> for Needs {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        match value {
            Value::Null => Ok(Self::default()),
            Value::String(value) => Ok(Self(vec![value])),
            Value::Sequence(values) => values
                .into_iter()
                .map(|value| match value {
                    Value::String(value) => Ok(value),
                    _ => Err(serde::de::Error::custom("needs entries must be strings")),
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Self),
            _ => Err(serde::de::Error::custom("needs must be a string or list")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Scalar(String);

impl Scalar {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Defaults {
    #[serde(default)]
    pub run: RunDefaults,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct RunDefaults {
    #[serde(default)]
    pub shell: Option<String>,
    #[serde(rename = "working-directory", default)]
    pub working_directory: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl<'de> Deserialize<'de> for Scalar {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let string = match value {
            Value::String(value) => value,
            Value::Bool(value) => value.to_string(),
            Value::Number(value) => value.to_string(),
            Value::Null => String::new(),
            _ => return Err(serde::de::Error::custom("expected a scalar value")),
        };
        Ok(Self(string))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionPlan {
    pub workflow_name: String,
    pub workflow_path: String,
    pub concurrency: Option<PlannedConcurrency>,
    pub jobs: Vec<PlannedJob>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedJob {
    pub id: String,
    pub base_id: String,
    pub name: String,
    pub needs: Vec<String>,
    pub need_aliases: BTreeMap<String, String>,
    pub condition: Option<String>,
    pub runs_on: Value,
    pub reusable_input_scopes: Vec<PlannedReusableInputScope>,
    pub reusable_secret_scopes: Vec<PlannedReusableSecretScope>,
    pub virtual_job: Option<PlannedVirtualJob>,
    pub matrix: BTreeMap<String, JsonValue>,
    pub dynamic_matrix: Option<Value>,
    pub matrix_fail_fast: bool,
    pub matrix_max_parallel: Option<usize>,
    pub strategy_job_index: Option<usize>,
    pub strategy_job_total: Option<usize>,
    pub continue_on_error: Option<String>,
    pub timeout_minutes: Option<String>,
    pub permissions: PlannedPermissions,
    pub concurrency: Option<PlannedConcurrency>,
    pub concurrency_scope_ids: Vec<String>,
    pub concurrency_acquire: Vec<PlannedConcurrencyScope>,
    pub concurrency_release: Vec<String>,
    pub deployment_environment: Option<PlannedEnvironment>,
    pub environment: BTreeMap<String, String>,
    pub outputs: BTreeMap<String, String>,
    pub steps: Vec<PlannedStep>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PlannedPermissions {
    pub read: BTreeSet<String>,
    pub write: BTreeSet<String>,
}

impl Default for PlannedPermissions {
    fn default() -> Self {
        Self {
            read: BTreeSet::from(["contents".to_owned(), "pull-requests".to_owned()]),
            write: BTreeSet::new(),
        }
    }
}

impl PlannedPermissions {
    fn intersect(&self, maximum: &Self) -> Self {
        let available = self
            .read
            .union(&self.write)
            .filter(|permission| {
                maximum.read.contains(*permission) || maximum.write.contains(*permission)
            })
            .cloned()
            .collect::<BTreeSet<_>>();
        let write = self
            .write
            .intersection(&maximum.write)
            .cloned()
            .collect::<BTreeSet<_>>();
        let read = available.difference(&write).cloned().collect();
        Self { read, write }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedEnvironment {
    pub name: String,
    pub url: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PlannedConcurrencyQueue {
    #[default]
    Single,
    Max,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedConcurrency {
    pub group: String,
    pub cancel_in_progress: String,
    pub queue: PlannedConcurrencyQueue,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedConcurrencyScope {
    pub id: String,
    pub configuration: PlannedConcurrency,
    pub reusable_input_scopes: Vec<PlannedReusableInputScope>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReusableInputType {
    String,
    Boolean,
    Number,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedReusableInput {
    pub value: Value,
    pub input_type: ReusableInputType,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedReusableInputScope {
    pub inputs: BTreeMap<String, PlannedReusableInput>,
    pub matrix: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PlannedReusableSecretScope {
    pub inherit: bool,
    pub mappings: BTreeMap<String, String>,
    pub required: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlannedVirtualJob {
    ReusableGate,
    ReusableResult {
        jobs: BTreeMap<String, String>,
        outputs: BTreeMap<String, String>,
    },
    ReusableMatrixResult {
        invocations: Vec<String>,
        max_parallel: usize,
        fail_fast: bool,
    },
    ReusableDynamicCall(PlannedDynamicReusableCall),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedDynamicReusableCall {
    pub called_plan: Box<ExecutionPlan>,
    pub inputs: BTreeMap<String, PlannedReusableInput>,
    pub secrets: PlannedReusableSecretScope,
    pub outputs: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedStep {
    pub id: String,
    pub github_action: String,
    pub name: String,
    pub environment: BTreeMap<String, String>,
    pub working_directory: Option<String>,
    pub condition: Option<String>,
    pub continue_on_error: Option<String>,
    pub timeout_minutes: Option<String>,
    pub kind: StepKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StepKind {
    Checkout {
        inputs: BTreeMap<String, String>,
    },
    Run {
        shell: String,
        script: String,
    },
    Uses {
        action: String,
        inputs: BTreeMap<String, String>,
    },
}

pub fn parse(source: &str) -> Result<Workflow, WorkflowError> {
    serde_yaml_ng::from_str(source).map_err(WorkflowError::from)
}

pub fn matches_pull_request(
    workflow: &Workflow,
    action: &str,
    base_ref: &str,
    changed_paths: Option<&[String]>,
) -> Result<bool, WorkflowError> {
    matches_pull_request_trigger(
        &workflow.trigger,
        action,
        base_ref,
        changed_paths,
        &workflow.name,
    )
}

pub fn requires_pull_request_changed_paths(workflow: &Workflow) -> bool {
    trigger_has_pull_request_path_filter(&workflow.trigger)
}

pub fn compile(workflow: &Workflow, path: &Path) -> Result<ExecutionPlan, WorkflowError> {
    let workflow_path = path.to_string_lossy().replace('\\', "/");
    let workflow_name = if workflow.name == default_workflow_name() {
        workflow_path.clone()
    } else {
        workflow.name.clone()
    };

    if let Some(feature) = workflow.extra.keys().next() {
        return Err(unsupported_workflow(
            &workflow_name,
            format!("top-level key '{feature}' is not supported yet"),
        ));
    }
    validate_defaults(&workflow_name, "workflow", &workflow.defaults)?;
    let workflow_permissions = planned_permissions(
        &workflow_name,
        "workflow",
        workflow.permissions.as_ref(),
        &PlannedPermissions::default(),
    )?;
    let workflow_concurrency = planned_concurrency(
        &workflow_name,
        "workflow",
        workflow.concurrency.as_ref(),
        &["github", "inputs", "vars"],
    )?;
    let workflow_env = scalar_map(&workflow.env);
    let mut jobs = Vec::new();

    for (job_id, job) in &workflow.jobs {
        validate_job(&workflow_name, job_id, job)?;
        validate_defaults(&workflow_name, &format!("job '{job_id}'"), &job.defaults)?;
        let MatrixPlan {
            combinations,
            dynamic,
        } = expand_matrix(&workflow_name, job_id, job.strategy.as_ref())?;
        let matrix_total = combinations.len();
        let has_matrix = dynamic.is_some()
            || combinations.len() > 1
            || combinations.iter().any(|matrix| !matrix.is_empty());
        for (matrix_index, matrix) in combinations.into_iter().enumerate() {
            let mut environment = workflow_env.clone();
            environment.extend(scalar_map(&job.env));
            let default_shell = job.defaults.run.shell.as_ref().or(workflow
                .defaults
                .run
                .shell
                .as_ref());
            let default_working_directory = job
                .defaults
                .run
                .working_directory
                .as_ref()
                .or(workflow.defaults.run.working_directory.as_ref());
            let steps = compile_steps(
                &workflow_name,
                job_id,
                &job.steps,
                default_shell,
                default_working_directory,
            )?;
            let id = if has_matrix {
                format!("{job_id}[{}]", matrix_index + 1)
            } else {
                job_id.clone()
            };
            jobs.push(PlannedJob {
                id,
                base_id: job_id.clone(),
                name: job.name.clone().unwrap_or_else(|| job_id.clone()),
                needs: job.needs.0.clone(),
                need_aliases: job
                    .needs
                    .0
                    .iter()
                    .map(|dependency| (dependency.clone(), dependency.clone()))
                    .collect(),
                condition: job
                    .condition
                    .as_ref()
                    .map(|condition| condition.as_str().to_owned()),
                runs_on: job
                    .runs_on
                    .clone()
                    .expect("validated runnable job has runs-on"),
                reusable_input_scopes: Vec::new(),
                reusable_secret_scopes: Vec::new(),
                virtual_job: None,
                matrix,
                dynamic_matrix: dynamic.clone(),
                matrix_fail_fast: strategy_fail_fast(job.strategy.as_ref()),
                matrix_max_parallel: strategy_max_parallel(job.strategy.as_ref()),
                strategy_job_index: job.strategy.as_ref().map(|_| matrix_index),
                strategy_job_total: job.strategy.as_ref().map(|_| matrix_total),
                continue_on_error: job
                    .continue_on_error
                    .as_ref()
                    .map(|value| value.as_str().to_owned()),
                timeout_minutes: job
                    .timeout_minutes
                    .as_ref()
                    .map(|value| value.as_str().to_owned()),
                permissions: planned_permissions(
                    &workflow_name,
                    &format!("job '{job_id}'"),
                    job.permissions.as_ref(),
                    &workflow_permissions,
                )?,
                concurrency: planned_concurrency(
                    &workflow_name,
                    &format!("job '{job_id}'"),
                    job.concurrency.as_ref(),
                    &["github", "inputs", "vars", "needs", "strategy", "matrix"],
                )?,
                concurrency_scope_ids: Vec::new(),
                concurrency_acquire: Vec::new(),
                concurrency_release: Vec::new(),
                deployment_environment: planned_environment(
                    &workflow_name,
                    job_id,
                    job.environment.as_ref(),
                )?,
                environment,
                outputs: scalar_map(&job.outputs),
                steps,
            });
        }
    }

    validate_dependencies(&workflow_name, &jobs)?;

    Ok(ExecutionPlan {
        workflow_name,
        workflow_path,
        concurrency: workflow_concurrency,
        jobs,
    })
}

pub fn compile_with_local_reusables(
    workflow: &Workflow,
    path: &Path,
    reusable_workflows: &BTreeMap<String, Workflow>,
) -> Result<ExecutionPlan, WorkflowError> {
    compile_with_reusables(workflow, path, reusable_workflows)
}

pub fn compile_with_reusables(
    workflow: &Workflow,
    path: &Path,
    reusable_workflows: &BTreeMap<String, Workflow>,
) -> Result<ExecutionPlan, WorkflowError> {
    compile_linked_workflow(
        workflow,
        path,
        reusable_workflows,
        &mut Vec::new(),
        &mut BTreeSet::new(),
        1,
    )
}

const MAX_REUSABLE_WORKFLOW_LEVELS: usize = 10;
pub const MAX_UNIQUE_REUSABLE_WORKFLOWS: usize = 50;

fn compile_linked_workflow(
    workflow: &Workflow,
    path: &Path,
    reusable_workflows: &BTreeMap<String, Workflow>,
    call_stack: &mut Vec<String>,
    called_workflows: &mut BTreeSet<String>,
    workflow_level: usize,
) -> Result<ExecutionPlan, WorkflowError> {
    let calls = workflow
        .jobs
        .iter()
        .filter_map(|(job_id, job)| job.uses.as_ref().map(|_| (job_id.clone(), job.clone())))
        .collect::<BTreeMap<_, _>>();
    if calls.is_empty() {
        return compile(workflow, path);
    }

    let mut root = workflow.clone();
    for (job_id, call) in &calls {
        validate_reusable_call_job(&workflow.name, job_id, call)?;
        let placeholder = root.jobs.get_mut(job_id).expect("call job exists");
        placeholder.runs_on = Some(Value::String("macos-latest".to_owned()));
        placeholder.uses = None;
        placeholder.with.clear();
        placeholder.secrets = None;
        placeholder.steps.clear();
    }
    let mut plan = compile(&root, path)?;
    let placeholders = plan
        .jobs
        .iter()
        .filter(|job| calls.contains_key(&job.base_id))
        .fold(
            BTreeMap::<String, Vec<PlannedJob>>::new(),
            |mut jobs, job| {
                jobs.entry(job.base_id.clone())
                    .or_default()
                    .push(job.clone());
                jobs
            },
        );
    let mut expansions = BTreeMap::new();
    for (job_id, call) in &calls {
        let placeholder_instances = placeholders.get(job_id).expect("compiled call placeholder");
        let dynamic_matrix = placeholder_instances
            .iter()
            .any(|placeholder| placeholder.dynamic_matrix.is_some());
        let uses = call.uses.as_deref().expect("call has uses");
        let reusable_path = reusable_path(&workflow.name, job_id, uses, reusable_workflows)?;
        called_workflows.insert(reusable_path.clone());
        if called_workflows.len() > MAX_UNIQUE_REUSABLE_WORKFLOWS {
            return Err(incompatible(
                &workflow.name,
                job_id,
                format!(
                    "workflow tree calls more than {MAX_UNIQUE_REUSABLE_WORKFLOWS} unique reusable workflows"
                ),
            ));
        }
        if workflow_level >= MAX_REUSABLE_WORKFLOW_LEVELS {
            return Err(incompatible(
                &workflow.name,
                job_id,
                format!(
                    "reusable workflow nesting exceeds {MAX_REUSABLE_WORKFLOW_LEVELS} total workflow levels"
                ),
            ));
        }
        if call_stack.contains(&reusable_path) {
            let mut cycle = call_stack.clone();
            cycle.push(reusable_path.clone());
            return Err(incompatible(
                &workflow.name,
                job_id,
                format!("reusable workflow call cycle: {}", cycle.join(" -> ")),
            ));
        }
        let called = reusable_workflows.get(&reusable_path).ok_or_else(|| {
            incompatible(
                &workflow.name,
                job_id,
                format!("local reusable workflow '{reusable_path}' was not found"),
            )
        })?;
        let contract = reusable_contract(&workflow.name, job_id, called)?;
        let inputs = bind_reusable_inputs(&workflow.name, job_id, call, &contract)?;
        let secrets = bind_reusable_secrets(&workflow.name, job_id, call, &contract)?;
        call_stack.push(reusable_path.clone());
        let called_plan = compile_linked_workflow(
            called,
            Path::new(&reusable_path),
            reusable_workflows,
            call_stack,
            called_workflows,
            workflow_level + 1,
        )?;
        call_stack.pop();
        if called_plan.jobs.is_empty() {
            return Err(incompatible(
                &workflow.name,
                job_id,
                "reusable workflow must contain at least one job".to_owned(),
            ));
        }
        let expansion = if dynamic_matrix {
            if placeholder_instances.len() != 1 {
                return Err(incompatible(
                    &workflow.name,
                    job_id,
                    "dynamic reusable-call matrix produced multiple templates".to_owned(),
                ));
            }
            let mut template = placeholder_instances[0].clone();
            template.virtual_job = Some(PlannedVirtualJob::ReusableDynamicCall(
                PlannedDynamicReusableCall {
                    called_plan: Box::new(called_plan),
                    inputs,
                    secrets,
                    outputs: contract.outputs,
                },
            ));
            vec![template]
        } else if placeholder_instances.len() == 1 {
            expand_reusable_call(
                job_id,
                &placeholder_instances[0],
                called_plan,
                inputs,
                secrets,
                contract.outputs,
            )
        } else {
            let mut expanded = Vec::new();
            let mut invocations = Vec::with_capacity(placeholder_instances.len());
            for (index, placeholder) in placeholder_instances.iter().enumerate() {
                let invocation = format!("{job_id}::matrix-{}", index + 1);
                invocations.push(invocation.clone());
                expanded.extend(expand_reusable_call(
                    &invocation,
                    placeholder,
                    called_plan.clone(),
                    inputs.clone(),
                    secrets.clone(),
                    contract.outputs.clone(),
                ));
            }
            expanded.push(reusable_matrix_result(
                job_id,
                &placeholder_instances[0],
                invocations,
            ));
            expanded
        };
        expansions.insert(job_id.clone(), expansion);
    }

    let mut linked = Vec::new();
    let mut replaced_calls = BTreeSet::new();
    for job in plan.jobs.drain(..) {
        if calls.contains_key(&job.base_id) {
            if replaced_calls.insert(job.base_id.clone()) {
                linked.extend(
                    expansions
                        .remove(&job.base_id)
                        .expect("call expansion exists"),
                );
            }
        } else {
            linked.push(job);
        }
    }
    plan.jobs = linked;
    validate_dependencies(&plan.workflow_name, &plan.jobs)?;
    Ok(plan)
}

struct ReusableContract {
    inputs: BTreeMap<String, ReusableInputDefinition>,
    secrets: BTreeMap<String, ReusableSecretDefinition>,
    outputs: BTreeMap<String, String>,
}

struct ReusableInputDefinition {
    input_type: ReusableInputType,
    required: bool,
    default: Option<Value>,
}

struct ReusableSecretDefinition {
    required: bool,
}

fn validate_reusable_call_job(
    workflow: &str,
    job_id: &str,
    job: &Job,
) -> Result<(), WorkflowError> {
    if job.runs_on.is_some()
        || !job.steps.is_empty()
        || !job.env.is_empty()
        || !job.outputs.is_empty()
        || job.continue_on_error.is_some()
        || job.timeout_minutes.is_some()
        || job.environment.is_some()
        || job.container.is_some()
        || job.services.is_some()
        || !job.defaults.extra.is_empty()
        || !job.defaults.run.extra.is_empty()
        || job.defaults.run.shell.is_some()
        || job.defaults.run.working_directory.is_some()
    {
        return Err(incompatible(
            workflow,
            job_id,
            "reusable workflow call contains runner-only job fields".to_owned(),
        ));
    }
    if let Some(secrets) = &job.secrets {
        match secrets {
            Value::String(value) if value == "inherit" => {}
            Value::Mapping(_) => {}
            _ => {
                return Err(incompatible(
                    workflow,
                    job_id,
                    "reusable workflow secrets must be a named mapping or 'inherit'".to_owned(),
                ));
            }
        }
    }
    if let Some(feature) = job.extra.keys().next() {
        return Err(incompatible(
            workflow,
            job_id,
            format!("reusable workflow call key '{feature}' is not supported yet"),
        ));
    }
    Ok(())
}

fn reusable_path(
    workflow: &str,
    job_id: &str,
    uses: &str,
    reusable_workflows: &BTreeMap<String, Workflow>,
) -> Result<String, WorkflowError> {
    let Some(path) = uses.strip_prefix("./").or_else(|| uses.strip_prefix("$/")) else {
        if reusable_workflows.contains_key(uses) {
            return Ok(uses.to_owned());
        }
        return Err(incompatible(
            workflow,
            job_id,
            format!("remote reusable workflow reference '{uses}' was not resolved"),
        ));
    };
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
        return Err(incompatible(
            workflow,
            job_id,
            format!("reusable workflow path '{uses}' must name a file in .github/workflows"),
        ));
    }
    Ok(path.to_string_lossy().replace('\\', "/"))
}

fn reusable_contract(
    caller_workflow: &str,
    caller_job: &str,
    workflow: &Workflow,
) -> Result<ReusableContract, WorkflowError> {
    let configuration = workflow_call_configuration(&workflow.trigger)
        .map_err(|()| {
            incompatible(
                caller_workflow,
                caller_job,
                format!(
                    "called workflow '{}' has an invalid workflow_call configuration",
                    workflow.name
                ),
            )
        })?
        .ok_or_else(|| {
            incompatible(
                caller_workflow,
                caller_job,
                format!(
                    "called workflow '{}' does not declare workflow_call",
                    workflow.name
                ),
            )
        })?;
    let mut inputs = BTreeMap::new();
    let mut secrets = BTreeMap::new();
    let mut outputs = BTreeMap::new();
    let Some(configuration) = configuration else {
        return Ok(ReusableContract {
            inputs,
            secrets,
            outputs,
        });
    };
    for key in configuration.keys() {
        if !matches!(key.as_str(), Some("inputs" | "outputs" | "secrets")) {
            return Err(incompatible(
                caller_workflow,
                caller_job,
                format!(
                    "called workflow '{}' uses unsupported workflow_call key '{}'",
                    workflow.name,
                    key.as_str().unwrap_or("non-string")
                ),
            ));
        }
    }
    if let Some(secret_configuration) = configuration.get(Value::String("secrets".to_owned())) {
        let definitions = secret_configuration.as_mapping().ok_or_else(|| {
            incompatible(
                caller_workflow,
                caller_job,
                "workflow_call secrets must be a mapping".to_owned(),
            )
        })?;
        for (name, definition) in definitions {
            let name = name.as_str().ok_or_else(|| {
                incompatible(
                    caller_workflow,
                    caller_job,
                    "workflow_call secret names must be strings".to_owned(),
                )
            })?;
            if !valid_secret_identifier(name) {
                return Err(incompatible(
                    caller_workflow,
                    caller_job,
                    format!("workflow_call secret name '{name}' is invalid"),
                ));
            }
            let definition = definition.as_mapping().ok_or_else(|| {
                incompatible(
                    caller_workflow,
                    caller_job,
                    format!("workflow_call secret '{name}' must be a mapping"),
                )
            })?;
            for key in definition.keys() {
                if !matches!(key.as_str(), Some("description" | "required")) {
                    return Err(incompatible(
                        caller_workflow,
                        caller_job,
                        format!("workflow_call secret '{name}' key is not supported"),
                    ));
                }
            }
            let required = definition
                .get(Value::String("required".to_owned()))
                .map(|value| {
                    value.as_bool().ok_or_else(|| {
                        incompatible(
                            caller_workflow,
                            caller_job,
                            format!("workflow_call secret '{name}' required must be boolean"),
                        )
                    })
                })
                .transpose()?
                .unwrap_or(false);
            secrets.insert(name.to_owned(), ReusableSecretDefinition { required });
        }
    }
    if let Some(value) = configuration.get(Value::String("inputs".to_owned())) {
        let definitions = value.as_mapping().ok_or_else(|| {
            incompatible(
                caller_workflow,
                caller_job,
                "workflow_call inputs must be a mapping".to_owned(),
            )
        })?;
        for (name, definition) in definitions {
            let name = name.as_str().ok_or_else(|| {
                incompatible(
                    caller_workflow,
                    caller_job,
                    "workflow_call input names must be strings".to_owned(),
                )
            })?;
            let definition = definition.as_mapping().ok_or_else(|| {
                incompatible(
                    caller_workflow,
                    caller_job,
                    format!("workflow_call input '{name}' must be a mapping"),
                )
            })?;
            for key in definition.keys() {
                if !matches!(
                    key.as_str(),
                    Some("description" | "required" | "type" | "default")
                ) {
                    return Err(incompatible(
                        caller_workflow,
                        caller_job,
                        format!("workflow_call input '{name}' key is not supported"),
                    ));
                }
            }
            let input_type = match definition
                .get(Value::String("type".to_owned()))
                .and_then(Value::as_str)
            {
                Some("string") => ReusableInputType::String,
                Some("boolean") => ReusableInputType::Boolean,
                Some("number") => ReusableInputType::Number,
                _ => {
                    return Err(incompatible(
                        caller_workflow,
                        caller_job,
                        format!(
                            "workflow_call input '{name}' must declare string, boolean, or number type"
                        ),
                    ));
                }
            };
            let required = definition
                .get(Value::String("required".to_owned()))
                .map(|value| {
                    value.as_bool().ok_or_else(|| {
                        incompatible(
                            caller_workflow,
                            caller_job,
                            format!("workflow_call input '{name}' required must be boolean"),
                        )
                    })
                })
                .transpose()?
                .unwrap_or(false);
            let default = definition.get(Value::String("default".to_owned())).cloned();
            if let Some(default) = &default
                && (value_contains_expression(default)
                    || !reusable_input_matches(default, input_type))
            {
                return Err(incompatible(
                    caller_workflow,
                    caller_job,
                    format!("workflow_call input '{name}' default has the wrong type"),
                ));
            }
            inputs.insert(
                name.to_owned(),
                ReusableInputDefinition {
                    input_type,
                    required,
                    default,
                },
            );
        }
    }
    if let Some(value) = configuration.get(Value::String("outputs".to_owned())) {
        let definitions = value.as_mapping().ok_or_else(|| {
            incompatible(
                caller_workflow,
                caller_job,
                "workflow_call outputs must be a mapping".to_owned(),
            )
        })?;
        for (name, definition) in definitions {
            let name = name.as_str().ok_or_else(|| {
                incompatible(
                    caller_workflow,
                    caller_job,
                    "workflow_call output names must be strings".to_owned(),
                )
            })?;
            let definition = definition.as_mapping().ok_or_else(|| {
                incompatible(
                    caller_workflow,
                    caller_job,
                    format!("workflow_call output '{name}' must be a mapping"),
                )
            })?;
            for key in definition.keys() {
                if !matches!(key.as_str(), Some("description" | "value")) {
                    return Err(incompatible(
                        caller_workflow,
                        caller_job,
                        format!("workflow_call output '{name}' key is not supported"),
                    ));
                }
            }
            let output = definition
                .get(Value::String("value".to_owned()))
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    incompatible(
                        caller_workflow,
                        caller_job,
                        format!("workflow_call output '{name}' must define a scalar value"),
                    )
                })?;
            outputs.insert(name.to_owned(), output.to_owned());
        }
    }
    Ok(ReusableContract {
        inputs,
        secrets,
        outputs,
    })
}

fn workflow_call_configuration(
    trigger: &Value,
) -> Result<Option<Option<&serde_yaml_ng::Mapping>>, ()> {
    match trigger {
        Value::String(value) if value == "workflow_call" => Ok(Some(None)),
        Value::Sequence(values)
            if values
                .iter()
                .any(|value| value.as_str() == Some("workflow_call")) =>
        {
            Ok(Some(None))
        }
        Value::Mapping(events) => match events.get(Value::String("workflow_call".to_owned())) {
            None => Ok(None),
            Some(Value::Null) => Ok(Some(None)),
            Some(Value::Mapping(configuration)) => Ok(Some(Some(configuration))),
            Some(_) => Err(()),
        },
        _ => Ok(None),
    }
}

fn bind_reusable_inputs(
    workflow: &str,
    job_id: &str,
    call: &Job,
    contract: &ReusableContract,
) -> Result<BTreeMap<String, PlannedReusableInput>, WorkflowError> {
    if let Some(input) = call
        .with
        .keys()
        .find(|input| !contract.inputs.contains_key(*input))
    {
        return Err(incompatible(
            workflow,
            job_id,
            format!("reusable workflow does not declare input '{input}'"),
        ));
    }
    let mut inputs = BTreeMap::new();
    for (name, definition) in &contract.inputs {
        let value = match call
            .with
            .get(name)
            .cloned()
            .or_else(|| definition.default.clone())
        {
            Some(value) => value,
            None if definition.required => {
                return Err(incompatible(
                    workflow,
                    job_id,
                    format!("required reusable workflow input '{name}' is missing"),
                ));
            }
            None => match definition.input_type {
                ReusableInputType::String => Value::String(String::new()),
                ReusableInputType::Boolean => Value::Bool(false),
                ReusableInputType::Number => Value::Number(0.into()),
            },
        };
        if !value_contains_expression(&value)
            && !reusable_input_matches(&value, definition.input_type)
        {
            return Err(incompatible(
                workflow,
                job_id,
                format!("reusable workflow input '{name}' has the wrong type"),
            ));
        }
        inputs.insert(
            name.clone(),
            PlannedReusableInput {
                value,
                input_type: definition.input_type,
            },
        );
    }
    Ok(inputs)
}

fn reusable_input_matches(value: &Value, input_type: ReusableInputType) -> bool {
    match input_type {
        ReusableInputType::String => value.as_str().is_some(),
        ReusableInputType::Boolean => value.as_bool().is_some(),
        ReusableInputType::Number => {
            value.as_i64().is_some() || value.as_u64().is_some() || value.as_f64().is_some()
        }
    }
}

fn bind_reusable_secrets(
    workflow: &str,
    job_id: &str,
    call: &Job,
    contract: &ReusableContract,
) -> Result<PlannedReusableSecretScope, WorkflowError> {
    let required = contract
        .secrets
        .iter()
        .filter_map(|(name, definition)| definition.required.then_some(name.clone()))
        .collect::<BTreeSet<_>>();
    let mut scope = PlannedReusableSecretScope {
        required,
        ..Default::default()
    };
    match &call.secrets {
        None => {}
        Some(Value::String(value)) if value == "inherit" => scope.inherit = true,
        Some(Value::Mapping(bindings)) => {
            for (target, source) in bindings {
                let target = target.as_str().ok_or_else(|| {
                    incompatible(
                        workflow,
                        job_id,
                        "reusable workflow secret names must be strings".to_owned(),
                    )
                })?;
                if !contract.secrets.contains_key(target) {
                    return Err(incompatible(
                        workflow,
                        job_id,
                        format!("reusable workflow does not declare secret '{target}'"),
                    ));
                }
                let source = reusable_secret_reference(source).ok_or_else(|| {
                    incompatible(
                        workflow,
                        job_id,
                        format!(
                            "reusable workflow secret '{target}' must reference an available secret or github.token"
                        ),
                    )
                })?;
                scope.mappings.insert(target.to_owned(), source);
            }
        }
        Some(_) => {
            return Err(incompatible(
                workflow,
                job_id,
                "reusable workflow secrets must be a named mapping or 'inherit'".to_owned(),
            ));
        }
    }
    if !scope.inherit
        && let Some(missing) = scope.required.iter().find(|name| {
            !name.eq_ignore_ascii_case("GITHUB_TOKEN") && !scope.mappings.contains_key(*name)
        })
    {
        return Err(incompatible(
            workflow,
            job_id,
            format!("required reusable workflow secret '{missing}' is missing"),
        ));
    }
    Ok(scope)
}

fn reusable_secret_reference(value: &Value) -> Option<String> {
    let value = value.as_str()?.trim();
    let expression = value.strip_prefix("${{")?.strip_suffix("}}")?.trim();
    if expression == "github.token" {
        return Some("GITHUB_TOKEN".to_owned());
    }
    let name = expression.strip_prefix("secrets.").or_else(|| {
        expression
            .strip_prefix("secrets['")
            .and_then(|value| value.strip_suffix("']"))
    })?;
    valid_secret_identifier(name).then(|| name.to_owned())
}

fn valid_secret_identifier(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn expand_reusable_call(
    job_id: &str,
    placeholder: &PlannedJob,
    called_plan: ExecutionPlan,
    inputs: BTreeMap<String, PlannedReusableInput>,
    secrets: PlannedReusableSecretScope,
    outputs: BTreeMap<String, String>,
) -> Vec<PlannedJob> {
    let gate_id = format!("{job_id}::gate");
    let mut called_reusable_input_scopes = placeholder.reusable_input_scopes.clone();
    called_reusable_input_scopes.push(PlannedReusableInputScope {
        inputs: inputs.clone(),
        matrix: placeholder.matrix.clone(),
    });
    let mut invocation_scopes = Vec::new();
    if let Some(configuration) = &placeholder.concurrency {
        invocation_scopes.push(PlannedConcurrencyScope {
            id: format!("{job_id}::caller-concurrency"),
            configuration: configuration.clone(),
            reusable_input_scopes: placeholder.reusable_input_scopes.clone(),
        });
    }
    if let Some(configuration) = &called_plan.concurrency {
        invocation_scopes.push(PlannedConcurrencyScope {
            id: format!("{job_id}::workflow-concurrency"),
            configuration: configuration.clone(),
            reusable_input_scopes: called_reusable_input_scopes.clone(),
        });
    }
    let invocation_scope_ids = invocation_scopes
        .iter()
        .map(|scope| scope.id.clone())
        .collect::<Vec<_>>();
    let mut gate = placeholder.clone();
    gate.id = gate_id.clone();
    gate.base_id = gate_id.clone();
    gate.name = format!("{} / reusable workflow gate", placeholder.name);
    gate.outputs.clear();
    gate.concurrency = None;
    gate.concurrency_scope_ids = invocation_scope_ids.clone();
    gate.concurrency_acquire = invocation_scopes;
    gate.concurrency_release.clear();
    gate.virtual_job = Some(PlannedVirtualJob::ReusableGate);

    let base_ids = called_plan
        .jobs
        .iter()
        .map(|job| job.base_id.clone())
        .collect::<BTreeSet<_>>();
    let aliases = base_ids
        .iter()
        .map(|base_id| (base_id.clone(), format!("{job_id}::{base_id}")))
        .collect::<BTreeMap<_, _>>();
    let public_aliases = aliases
        .iter()
        .filter(|(base_id, _)| !base_id.contains("::"))
        .map(|(base_id, namespaced)| (base_id.clone(), namespaced.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut called_jobs = Vec::new();
    for mut job in called_plan.jobs {
        job.permissions = job.permissions.intersect(&placeholder.permissions);
        let old_base_id = job.base_id.clone();
        let new_base_id = aliases[&old_base_id].clone();
        job.id = format!("{new_base_id}{}", &job.id[old_base_id.len()..]);
        job.base_id = new_base_id;
        job.name = format!("{} / {}", placeholder.name, job.name);
        let original_needs = job.needs.clone();
        job.needs = original_needs
            .iter()
            .map(|dependency| aliases[dependency].clone())
            .collect();
        job.need_aliases = job
            .need_aliases
            .into_iter()
            .map(|(alias, dependency)| (alias, aliases[&dependency].clone()))
            .collect();
        for scope_id in &mut job.concurrency_scope_ids {
            *scope_id = format!("{job_id}::{scope_id}");
        }
        for scope in &mut job.concurrency_acquire {
            scope.id = format!("{job_id}::{}", scope.id);
            let mut reusable_input_scopes = called_reusable_input_scopes.clone();
            reusable_input_scopes.extend(scope.reusable_input_scopes.clone());
            scope.reusable_input_scopes = reusable_input_scopes;
        }
        for scope_id in &mut job.concurrency_release {
            *scope_id = format!("{job_id}::{scope_id}");
        }
        job.concurrency_scope_ids
            .splice(0..0, invocation_scope_ids.iter().cloned());
        for dependency in &placeholder.needs {
            job.need_aliases
                .entry(dependency.clone())
                .or_insert_with(|| dependency.clone());
        }
        job.needs.push(gate_id.clone());
        let mut scopes = called_reusable_input_scopes.clone();
        scopes.extend(job.reusable_input_scopes);
        job.reusable_input_scopes = scopes;
        let mut secret_scopes = placeholder.reusable_secret_scopes.clone();
        secret_scopes.push(secrets.clone());
        secret_scopes.extend(job.reusable_secret_scopes);
        job.reusable_secret_scopes = secret_scopes;
        match &mut job.virtual_job {
            Some(PlannedVirtualJob::ReusableResult { jobs, .. }) => {
                for dependency in jobs.values_mut() {
                    *dependency = aliases[dependency].clone();
                }
            }
            Some(PlannedVirtualJob::ReusableMatrixResult { invocations, .. }) => {
                for invocation in invocations {
                    *invocation = aliases[invocation].clone();
                }
            }
            Some(PlannedVirtualJob::ReusableGate | PlannedVirtualJob::ReusableDynamicCall(_))
            | None => {}
        }
        called_jobs.push(job);
    }

    let mut result = placeholder.clone();
    result.id = job_id.to_owned();
    result.base_id = job_id.to_owned();
    result.condition = Some("${{ always() }}".to_owned());
    result.needs = public_aliases.values().cloned().collect();
    result.need_aliases = public_aliases.clone();
    result.outputs.clear();
    result.concurrency = None;
    result.concurrency_scope_ids = invocation_scope_ids.clone();
    result.concurrency_acquire.clear();
    result.concurrency_release = invocation_scope_ids.into_iter().rev().collect();
    result.virtual_job = Some(PlannedVirtualJob::ReusableResult {
        jobs: public_aliases,
        outputs,
    });

    let mut expanded = Vec::with_capacity(called_jobs.len() + 2);
    expanded.push(gate);
    expanded.extend(called_jobs);
    expanded.push(result);
    expanded
}

fn reusable_matrix_result(
    job_id: &str,
    placeholder: &PlannedJob,
    invocations: Vec<String>,
) -> PlannedJob {
    let mut result = placeholder.clone();
    result.id = job_id.to_owned();
    result.base_id = job_id.to_owned();
    result.condition = Some("${{ always() }}".to_owned());
    result.needs = invocations.clone();
    result.need_aliases = invocations
        .iter()
        .map(|invocation| (invocation.clone(), invocation.clone()))
        .collect();
    result.matrix.clear();
    result.dynamic_matrix = None;
    result.matrix_max_parallel = None;
    result.strategy_job_index = None;
    result.strategy_job_total = None;
    result.outputs.clear();
    result.steps.clear();
    result.concurrency = None;
    result.concurrency_scope_ids.clear();
    result.concurrency_acquire.clear();
    result.concurrency_release.clear();
    result.virtual_job = Some(PlannedVirtualJob::ReusableMatrixResult {
        max_parallel: placeholder
            .matrix_max_parallel
            .unwrap_or(invocations.len())
            .min(invocations.len())
            .max(1),
        fail_fast: placeholder.matrix_fail_fast,
        invocations,
    });
    result
}

pub fn expand_dynamic_reusable_call(
    template: &PlannedJob,
    instances: Vec<PlannedJob>,
    call: &PlannedDynamicReusableCall,
) -> Vec<PlannedJob> {
    let mut expanded = Vec::new();
    let mut invocations = Vec::with_capacity(instances.len());
    for (index, instance) in instances.iter().enumerate() {
        let invocation = format!("{}::matrix-{}", template.base_id, index + 1);
        invocations.push(invocation.clone());
        expanded.extend(expand_reusable_call(
            &invocation,
            instance,
            (*call.called_plan).clone(),
            call.inputs.clone(),
            call.secrets.clone(),
            call.outputs.clone(),
        ));
    }
    expanded.push(reusable_matrix_result(
        &template.base_id,
        template,
        invocations,
    ));
    expanded
}

fn compile_steps(
    workflow: &str,
    job_id: &str,
    source_steps: &[Step],
    default_shell: Option<&String>,
    default_working_directory: Option<&String>,
) -> Result<Vec<PlannedStep>, WorkflowError> {
    let mut steps = Vec::with_capacity(source_steps.len());
    let mut action_occurrences = BTreeMap::<String, usize>::new();
    for (index, step) in source_steps.iter().enumerate() {
        let step_id = step
            .id
            .clone()
            .unwrap_or_else(|| format!("step-{}", index + 1));
        let step_name = step.name.clone().unwrap_or_else(|| {
            step.uses
                .clone()
                .or_else(|| step.run.as_ref().map(|_| "Run".to_owned()))
                .unwrap_or_else(|| step_id.clone())
        });
        if let Some(feature) = step.extra.keys().next() {
            return Err(incompatible(
                workflow,
                job_id,
                format!("step '{step_name}' uses unsupported key '{feature}'"),
            ));
        }
        if step.uses.is_some() && (step.shell.is_some() || step.working_directory.is_some()) {
            return Err(incompatible(
                workflow,
                job_id,
                format!("step '{step_name}' assigns run-only shell or working-directory fields"),
            ));
        }

        let kind = match (&step.run, &step.uses) {
            (Some(script), None) if step.with.is_empty() => StepKind::Run {
                shell: step
                    .shell
                    .clone()
                    .or_else(|| default_shell.cloned())
                    .unwrap_or_else(|| "bash -e {0}".to_owned()),
                script: script.clone(),
            },
            (Some(_), None) => {
                return Err(incompatible(
                    workflow,
                    job_id,
                    format!("step '{step_name}' supplies with inputs to a run step"),
                ));
            }
            (None, Some(action)) if is_checkout_action(action) => StepKind::Checkout {
                inputs: scalar_map(&step.with),
            },
            (None, Some(action)) => StepKind::Uses {
                action: action.clone(),
                inputs: scalar_map(&step.with),
            },
            _ => {
                return Err(incompatible(
                    workflow,
                    job_id,
                    format!("step '{step_name}' must define exactly one of run or uses"),
                ));
            }
        };
        let action_base = step.id.clone().unwrap_or_else(|| match &kind {
            StepKind::Run { .. } => "__run".to_owned(),
            StepKind::Checkout { .. } => "actionscheckout".to_owned(),
            StepKind::Uses { action, .. } => action
                .split('@')
                .next()
                .unwrap_or(action)
                .chars()
                .filter(char::is_ascii_alphanumeric)
                .collect::<String>(),
        });
        let occurrence = action_occurrences.entry(action_base.clone()).or_default();
        *occurrence += 1;
        let github_action = match (&kind, *occurrence) {
            (_, 1) => action_base,
            (StepKind::Run { .. }, count) => format!("{action_base}_{count}"),
            (_, count) => format!("{action_base}{count}"),
        };

        steps.push(PlannedStep {
            id: step_id,
            github_action,
            name: step_name,
            environment: scalar_map(&step.env),
            working_directory: if matches!(&kind, StepKind::Run { .. }) {
                step.working_directory
                    .clone()
                    .or_else(|| default_working_directory.cloned())
            } else {
                None
            },
            condition: step
                .condition
                .as_ref()
                .map(|condition| condition.as_str().to_owned()),
            continue_on_error: step
                .continue_on_error
                .as_ref()
                .map(|value| value.as_str().to_owned()),
            timeout_minutes: step
                .timeout_minutes
                .as_ref()
                .map(|value| value.as_str().to_owned()),
            kind,
        });
    }
    Ok(steps)
}

fn validate_job(workflow: &str, job_id: &str, job: &Job) -> Result<(), WorkflowError> {
    if job.uses.is_some() {
        return Err(incompatible(
            workflow,
            job_id,
            "reusable workflow call was not linked".to_owned(),
        ));
    }
    job.runs_on.as_ref().ok_or_else(|| {
        incompatible(
            workflow,
            job_id,
            "runnable job must define runs-on".to_owned(),
        )
    })?;
    for (present, feature) in [
        (job.container.is_some(), "container"),
        (job.services.is_some(), "services"),
    ] {
        if present {
            return Err(incompatible(
                workflow,
                job_id,
                format!("{feature} is not supported yet"),
            ));
        }
    }
    if !job.with.is_empty() || job.secrets.is_some() {
        return Err(incompatible(
            workflow,
            job_id,
            "with and secrets are only valid on reusable workflow calls".to_owned(),
        ));
    }
    if let Some(feature) = job.extra.keys().next() {
        return Err(incompatible(
            workflow,
            job_id,
            format!("job key '{feature}' is not supported yet"),
        ));
    }
    Ok(())
}

const READ_WRITE_TOKEN_PERMISSIONS: [&str; 14] = [
    "actions",
    "artifact-metadata",
    "attestations",
    "checks",
    "code-quality",
    "contents",
    "deployments",
    "discussions",
    "issues",
    "packages",
    "pages",
    "pull-requests",
    "security-events",
    "statuses",
];
const READ_ONLY_TOKEN_PERMISSIONS: [&str; 1] = ["vulnerability-alerts"];
const TOKEN_PERMISSION_KEYS: [&str; 16] = [
    "actions",
    "artifact-metadata",
    "attestations",
    "checks",
    "code-quality",
    "contents",
    "deployments",
    "discussions",
    "id-token",
    "issues",
    "packages",
    "pages",
    "pull-requests",
    "security-events",
    "statuses",
    "vulnerability-alerts",
];

fn planned_permissions(
    workflow: &str,
    location: &str,
    value: Option<&Value>,
    inherited: &PlannedPermissions,
) -> Result<PlannedPermissions, WorkflowError> {
    let Some(value) = value else {
        return Ok(inherited.clone());
    };
    match value {
        Value::String(value) if value == "read-all" => Ok(PlannedPermissions {
            read: READ_WRITE_TOKEN_PERMISSIONS
                .into_iter()
                .chain(READ_ONLY_TOKEN_PERMISSIONS)
                .map(str::to_owned)
                .collect(),
            write: BTreeSet::new(),
        }),
        Value::String(value) if value == "write-all" => Err(unsupported_workflow(
            workflow,
            format!(
                "{location} permissions request write-all, which includes unsupported id-token: write access"
            ),
        )),
        Value::Mapping(values) => {
            let mut read = BTreeSet::new();
            let mut write = BTreeSet::new();
            for (name, access) in values {
                let Some(name) = name.as_str() else {
                    return Err(unsupported_workflow(
                        workflow,
                        format!("{location} permission names must be strings"),
                    ));
                };
                if !TOKEN_PERMISSION_KEYS.contains(&name) {
                    return Err(unsupported_workflow(
                        workflow,
                        format!("{location} permission '{name}' is not recognized"),
                    ));
                }
                let Some(access) = access.as_str() else {
                    return Err(unsupported_workflow(
                        workflow,
                        format!("{location} permission '{name}' must be read, write, or none"),
                    ));
                };
                match access {
                    "none" => {}
                    "read" if name != "id-token" => {
                        read.insert(name.to_owned());
                    }
                    "read" => {
                        return Err(unsupported_workflow(
                            workflow,
                            format!(
                                "{location} permission 'id-token' does not support read access"
                            ),
                        ));
                    }
                    "write" => {
                        if name == "id-token" {
                            return Err(unsupported_workflow(
                                workflow,
                                format!(
                                    "{location} permission 'id-token: write' is not supported because GitZero does not issue GitHub OIDC tokens"
                                ),
                            ));
                        }
                        if READ_ONLY_TOKEN_PERMISSIONS.contains(&name) {
                            return Err(unsupported_workflow(
                                workflow,
                                format!(
                                    "{location} permission '{name}' supports only read or none access"
                                ),
                            ));
                        }
                        write.insert(name.to_owned());
                    }
                    _ => {
                        return Err(unsupported_workflow(
                            workflow,
                            format!("{location} permission '{name}' must be read, write, or none"),
                        ));
                    }
                }
            }
            Ok(PlannedPermissions { read, write })
        }
        _ => Err(unsupported_workflow(
            workflow,
            format!("{location} permissions must be read-all, write-all, or a permission mapping"),
        )),
    }
}

fn planned_concurrency(
    workflow: &str,
    location: &str,
    concurrency: Option<&Concurrency>,
    allowed_contexts: &[&str],
) -> Result<Option<PlannedConcurrency>, WorkflowError> {
    let Some(concurrency) = concurrency else {
        return Ok(None);
    };
    let (group, cancel_in_progress, queue) = match concurrency {
        Concurrency::Group(group) => (group.as_str(), "false", PlannedConcurrencyQueue::Single),
        Concurrency::Configuration(configuration) => {
            if let Some(feature) = configuration.extra.keys().next() {
                return Err(unsupported_workflow(
                    workflow,
                    format!("{location} concurrency key '{feature}' is not supported"),
                ));
            }
            let queue = match configuration.queue.as_deref().unwrap_or("single") {
                "single" => PlannedConcurrencyQueue::Single,
                "max" => PlannedConcurrencyQueue::Max,
                value => {
                    return Err(unsupported_workflow(
                        workflow,
                        format!(
                            "{location} concurrency queue must be 'single' or 'max', not '{value}'"
                        ),
                    ));
                }
            };
            (
                configuration.group.as_str(),
                configuration
                    .cancel_in_progress
                    .as_ref()
                    .map_or("false", Scalar::as_str),
                queue,
            )
        }
    };
    if group.trim().is_empty()
        || group.contains(['\0', '\n', '\r'])
        || (!group.contains("${{") && group.len() > 256)
        || group.len() > 4_096
    {
        return Err(unsupported_workflow(
            workflow,
            format!("{location} concurrency group is empty or exceeds its safe bounds"),
        ));
    }
    for (field, value) in [("group", group), ("cancel-in-progress", cancel_in_progress)] {
        let analysis = analyze_template(value).map_err(|error| {
            unsupported_workflow(
                workflow,
                format!("{location} concurrency {field} is invalid: {error}"),
            )
        })?;
        let unsupported = analysis
            .context_roots
            .iter()
            .filter(|context| !allowed_contexts.contains(&context.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if !unsupported.is_empty() || analysis.uses_hash_files || analysis.uses_status_function {
            return Err(unsupported_workflow(
                workflow,
                format!(
                    "{location} concurrency {field} uses a context or function unavailable to GitHub concurrency"
                ),
            ));
        }
    }
    if !cancel_in_progress.contains("${{") && !matches!(cancel_in_progress, "true" | "false") {
        return Err(unsupported_workflow(
            workflow,
            format!("{location} concurrency cancel-in-progress must be a boolean or expression"),
        ));
    }
    if queue == PlannedConcurrencyQueue::Max && cancel_in_progress == "true" {
        return Err(unsupported_workflow(
            workflow,
            format!(
                "{location} concurrency cannot combine queue: max with cancel-in-progress: true"
            ),
        ));
    }
    Ok(Some(PlannedConcurrency {
        group: group.to_owned(),
        cancel_in_progress: cancel_in_progress.to_owned(),
        queue,
    }))
}

fn planned_environment(
    workflow: &str,
    job_id: &str,
    environment: Option<&JobEnvironment>,
) -> Result<Option<PlannedEnvironment>, WorkflowError> {
    let Some(environment) = environment else {
        return Ok(None);
    };
    let (name, url) = match environment {
        JobEnvironment::Name(name) => (name.as_str(), None),
        JobEnvironment::Configuration(configuration) => {
            if let Some(feature) = configuration.extra.keys().next() {
                return Err(incompatible(
                    workflow,
                    job_id,
                    format!("environment key '{feature}' is not supported"),
                ));
            }
            (
                configuration.name.as_str(),
                configuration.url.as_ref().map(Scalar::as_str),
            )
        }
    };
    if name.trim().is_empty() {
        return Err(incompatible(
            workflow,
            job_id,
            "environment name must not be empty".to_owned(),
        ));
    }
    Ok(Some(PlannedEnvironment {
        name: name.to_owned(),
        url: url.map(str::to_owned),
    }))
}

fn validate_defaults(
    workflow: &str,
    location: &str,
    defaults: &Defaults,
) -> Result<(), WorkflowError> {
    if let Some(feature) = defaults.extra.keys().next() {
        return Err(unsupported_workflow(
            workflow,
            format!("{location} defaults key '{feature}' is not supported"),
        ));
    }
    if let Some(feature) = defaults.run.extra.keys().next() {
        return Err(unsupported_workflow(
            workflow,
            format!("{location} defaults.run key '{feature}' is not supported"),
        ));
    }
    if defaults
        .run
        .shell
        .iter()
        .chain(defaults.run.working_directory.iter())
        .any(|value| value.contains("${{"))
    {
        return Err(unsupported_workflow(
            workflow,
            format!("{location} defaults.run cannot contain expressions"),
        ));
    }
    Ok(())
}

struct MatrixPlan {
    combinations: Vec<BTreeMap<String, JsonValue>>,
    dynamic: Option<Value>,
}

fn expand_matrix(
    workflow: &str,
    job_id: &str,
    strategy: Option<&Value>,
) -> Result<MatrixPlan, WorkflowError> {
    let Some(strategy) = strategy else {
        return Ok(MatrixPlan {
            combinations: vec![BTreeMap::new()],
            dynamic: None,
        });
    };
    let Value::Mapping(strategy) = strategy else {
        return Err(incompatible(
            workflow,
            job_id,
            "strategy must be a mapping".to_owned(),
        ));
    };

    for (key, value) in strategy {
        let Some(key) = key.as_str() else {
            return Err(incompatible(
                workflow,
                job_id,
                "strategy keys must be strings".to_owned(),
            ));
        };
        match key {
            "matrix" => {}
            "fail-fast" if value.as_bool().is_some() => {}
            "max-parallel" if value.as_u64().is_some_and(|value| value > 0) => {}
            "fail-fast" => {
                return Err(incompatible(
                    workflow,
                    job_id,
                    "strategy fail-fast must be a static boolean".to_owned(),
                ));
            }
            "max-parallel" => {
                return Err(incompatible(
                    workflow,
                    job_id,
                    "strategy max-parallel must be a positive static integer".to_owned(),
                ));
            }
            _ => {
                return Err(incompatible(
                    workflow,
                    job_id,
                    format!("strategy key '{key}' is not supported yet"),
                ));
            }
        }
    }

    let Some(matrix) = strategy.get(Value::String("matrix".to_owned())) else {
        return Ok(MatrixPlan {
            combinations: vec![BTreeMap::new()],
            dynamic: None,
        });
    };
    if contains_expression(matrix) {
        return Ok(MatrixPlan {
            combinations: vec![BTreeMap::new()],
            dynamic: Some(matrix.clone()),
        });
    }
    Ok(MatrixPlan {
        combinations: expand_matrix_definition(workflow, job_id, matrix)?,
        dynamic: None,
    })
}

pub fn expand_matrix_definition(
    workflow: &str,
    job_id: &str,
    matrix: &Value,
) -> Result<Vec<BTreeMap<String, JsonValue>>, WorkflowError> {
    let Value::Mapping(matrix) = matrix else {
        return Err(incompatible(
            workflow,
            job_id,
            "matrix must resolve to a mapping".to_owned(),
        ));
    };

    let mut dimensions = Vec::new();
    let mut includes = Vec::new();
    let mut excludes = Vec::new();
    for (key, value) in matrix {
        let Some(key) = key.as_str() else {
            return Err(incompatible(
                workflow,
                job_id,
                "matrix keys must be strings".to_owned(),
            ));
        };
        match key {
            "include" => includes = matrix_objects(workflow, job_id, "include", value)?,
            "exclude" => excludes = matrix_objects(workflow, job_id, "exclude", value)?,
            dimension => {
                let Value::Sequence(values) = value else {
                    return Err(incompatible(
                        workflow,
                        job_id,
                        format!("matrix dimension '{dimension}' must be a static list"),
                    ));
                };
                if values.is_empty() {
                    return Err(incompatible(
                        workflow,
                        job_id,
                        format!("matrix dimension '{dimension}' cannot be empty"),
                    ));
                }
                let values = values
                    .iter()
                    .map(|value| {
                        yaml_to_json(value).map_err(|reason| incompatible(workflow, job_id, reason))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                dimensions.push((dimension.to_owned(), values));
            }
        }
    }

    let mut combinations = if dimensions.is_empty() && !includes.is_empty() {
        Vec::new()
    } else {
        vec![BTreeMap::new()]
    };
    for (key, values) in &dimensions {
        let mut product = Vec::new();
        for combination in &combinations {
            for value in values {
                let mut next = combination.clone();
                next.insert(key.clone(), value.clone());
                product.push(next);
                if product.len() > 256 {
                    return Err(incompatible(
                        workflow,
                        job_id,
                        "matrix expands beyond GitHub's 256-job limit".to_owned(),
                    ));
                }
            }
        }
        combinations = product;
    }

    combinations.retain(|combination| {
        !excludes
            .iter()
            .any(|exclude| matrix_object_matches(combination, exclude))
    });

    // GitHub evaluates include entries against the original Cartesian product. Entries may
    // augment compatible combinations; otherwise they create an additional combination.
    let original = combinations.clone();
    for include in includes {
        let mut applied = false;
        for (index, original_combination) in original.iter().enumerate() {
            let compatible = include.iter().all(|(key, value)| {
                original_combination
                    .get(key)
                    .is_none_or(|original| original == value)
            });
            if compatible {
                combinations[index].extend(include.clone());
                applied = true;
            }
        }
        if !applied {
            combinations.push(include);
        }
        if combinations.len() > 256 {
            return Err(incompatible(
                workflow,
                job_id,
                "matrix expands beyond GitHub's 256-job limit".to_owned(),
            ));
        }
    }

    Ok(combinations)
}

fn contains_expression(value: &Value) -> bool {
    match value {
        Value::String(value) => value.contains("${{"),
        Value::Sequence(values) => values.iter().any(contains_expression),
        Value::Mapping(entries) => entries
            .iter()
            .any(|(key, value)| contains_expression(key) || contains_expression(value)),
        Value::Tagged(value) => contains_expression(&value.value),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn matrix_objects(
    workflow: &str,
    job_id: &str,
    name: &str,
    value: &Value,
) -> Result<Vec<BTreeMap<String, JsonValue>>, WorkflowError> {
    let Value::Sequence(values) = value else {
        return Err(incompatible(
            workflow,
            job_id,
            format!("matrix {name} must be a static list"),
        ));
    };
    values
        .iter()
        .map(|value| {
            let Value::Mapping(entries) = value else {
                return Err(incompatible(
                    workflow,
                    job_id,
                    format!("matrix {name} entries must be mappings"),
                ));
            };
            entries
                .iter()
                .map(|(key, value)| {
                    let key = key.as_str().ok_or_else(|| {
                        incompatible(
                            workflow,
                            job_id,
                            format!("matrix {name} keys must be strings"),
                        )
                    })?;
                    let value = yaml_to_json(value)
                        .map_err(|reason| incompatible(workflow, job_id, reason))?;
                    Ok((key.to_owned(), value))
                })
                .collect()
        })
        .collect()
}

fn yaml_to_json(value: &Value) -> Result<JsonValue, String> {
    serde_json::to_value(value)
        .map_err(|error| format!("matrix value is not JSON-compatible: {error}"))
}

fn matrix_object_matches(
    combination: &BTreeMap<String, JsonValue>,
    filter: &BTreeMap<String, JsonValue>,
) -> bool {
    filter
        .iter()
        .all(|(key, value)| combination.get(key) == Some(value))
}

fn strategy_fail_fast(strategy: Option<&Value>) -> bool {
    strategy
        .and_then(Value::as_mapping)
        .and_then(|strategy| strategy.get(Value::String("fail-fast".to_owned())))
        .and_then(Value::as_bool)
        .unwrap_or(true)
}

fn strategy_max_parallel(strategy: Option<&Value>) -> Option<usize> {
    strategy
        .and_then(Value::as_mapping)
        .and_then(|strategy| strategy.get(Value::String("max-parallel".to_owned())))
        .and_then(Value::as_u64)
        .map(|value| usize::try_from(value).unwrap_or(usize::MAX))
}

fn validate_dependencies(workflow: &str, jobs: &[PlannedJob]) -> Result<(), WorkflowError> {
    let mut graph = BTreeMap::<String, Vec<String>>::new();
    for job in jobs {
        graph
            .entry(job.base_id.clone())
            .or_insert_with(|| job.needs.clone());
    }
    for (job, needs) in &graph {
        for dependency in needs {
            if !graph.contains_key(dependency) {
                return Err(incompatible(
                    workflow,
                    job,
                    format!("needs unknown job '{dependency}'"),
                ));
            }
            if dependency == job {
                return Err(incompatible(
                    workflow,
                    job,
                    "job cannot need itself".to_owned(),
                ));
            }
        }
    }

    fn visit(
        workflow: &str,
        job: &str,
        graph: &BTreeMap<String, Vec<String>>,
        states: &mut BTreeMap<String, u8>,
    ) -> Result<(), WorkflowError> {
        match states.get(job) {
            Some(1) => {
                return Err(incompatible(
                    workflow,
                    job,
                    "job dependency graph contains a cycle".to_owned(),
                ));
            }
            Some(2) => return Ok(()),
            _ => {}
        }
        states.insert(job.to_owned(), 1);
        for dependency in &graph[job] {
            visit(workflow, dependency, graph, states)?;
        }
        states.insert(job.to_owned(), 2);
        Ok(())
    }

    let mut states = BTreeMap::new();
    for job in graph.keys() {
        visit(workflow, job, &graph, &mut states)?;
    }
    Ok(())
}

fn incompatible(workflow: &str, job: &str, reason: String) -> WorkflowError {
    WorkflowError::Incompatible {
        workflow: workflow.to_owned(),
        job: job.to_owned(),
        reason,
    }
}

fn unsupported_workflow(workflow: &str, reason: String) -> WorkflowError {
    WorkflowError::UnsupportedWorkflow {
        workflow: workflow.to_owned(),
        reason,
    }
}

fn value_contains_expression(value: &Value) -> bool {
    match value {
        Value::String(value) => value.contains("${{"),
        Value::Sequence(values) => values.iter().any(value_contains_expression),
        Value::Mapping(values) => values
            .iter()
            .any(|(key, value)| value_contains_expression(key) || value_contains_expression(value)),
        Value::Tagged(value) => value_contains_expression(&value.value),
        _ => false,
    }
}

fn is_checkout_action(action: &str) -> bool {
    action
        .split_once('@')
        .is_some_and(|(name, version)| name == "actions/checkout" && !version.is_empty())
}

fn matches_pull_request_trigger(
    value: &Value,
    action: &str,
    base_ref: &str,
    changed_paths: Option<&[String]>,
    workflow: &str,
) -> Result<bool, WorkflowError> {
    match value {
        Value::String(value) => Ok(value == "pull_request" && is_default_action(action)),
        Value::Sequence(values) => {
            for value in values {
                if matches_pull_request_trigger(value, action, base_ref, changed_paths, workflow)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Value::Mapping(values) => {
            let Some(configuration) = values.get(Value::String("pull_request".to_owned())) else {
                return Ok(false);
            };
            match configuration {
                Value::Null => Ok(is_default_action(action)),
                Value::Mapping(options) => {
                    for key in options.keys() {
                        if !matches!(
                            key.as_str(),
                            Some(
                                "types" | "branches" | "branches-ignore" | "paths" | "paths-ignore"
                            )
                        ) {
                            return Err(unsupported_workflow(
                                workflow,
                                format!(
                                    "pull_request filter '{}' is not supported yet",
                                    key.as_str().unwrap_or("non-string")
                                ),
                            ));
                        }
                    }
                    if options.contains_key(Value::String("branches".to_owned()))
                        && options.contains_key(Value::String("branches-ignore".to_owned()))
                    {
                        return Err(unsupported_workflow(
                            workflow,
                            "pull_request cannot define both branches and branches-ignore"
                                .to_owned(),
                        ));
                    }
                    if options.contains_key(Value::String("paths".to_owned()))
                        && options.contains_key(Value::String("paths-ignore".to_owned()))
                    {
                        return Err(unsupported_workflow(
                            workflow,
                            "pull_request cannot define both paths and paths-ignore".to_owned(),
                        ));
                    }
                    let action_matches =
                        options.get(Value::String("types".to_owned())).map_or_else(
                            || is_default_action(action),
                            |types| value_contains_string(types, action),
                        );
                    if !action_matches {
                        return Ok(false);
                    }
                    if let Some(branches) = options.get(Value::String("branches".to_owned()))
                        && !matches_ordered_patterns(branches, base_ref, workflow, "branches")?
                    {
                        return Ok(false);
                    }
                    if let Some(ignored) = options.get(Value::String("branches-ignore".to_owned()))
                        && matches_ignored_pattern(ignored, base_ref, workflow, "branches-ignore")?
                    {
                        return Ok(false);
                    }
                    if options.contains_key(Value::String("paths".to_owned()))
                        || options.contains_key(Value::String("paths-ignore".to_owned()))
                    {
                        let changed_paths = changed_paths.ok_or_else(|| {
                            unsupported_workflow(
                                workflow,
                                "pull_request path filters require the changed-file list"
                                    .to_owned(),
                            )
                        })?;
                        if changed_paths.is_empty() {
                            return Ok(false);
                        }
                        if let Some(paths) = options.get(Value::String("paths".to_owned()))
                            && !matches_any_ordered_path(paths, changed_paths, workflow, "paths")?
                        {
                            return Ok(false);
                        }
                        if let Some(ignored) = options.get(Value::String("paths-ignore".to_owned()))
                        {
                            let patterns =
                                compile_ignored_patterns(ignored, workflow, "paths-ignore")?;
                            if changed_paths
                                .iter()
                                .all(|path| patterns.iter().any(|pattern| pattern.is_match(path)))
                            {
                                return Ok(false);
                            }
                        }
                    }
                    Ok(true)
                }
                _ => Err(unsupported_workflow(
                    workflow,
                    "pull_request trigger configuration must be a mapping".to_owned(),
                )),
            }
        }
        _ => Ok(false),
    }
}

fn trigger_has_pull_request_path_filter(value: &Value) -> bool {
    match value {
        Value::Sequence(values) => values.iter().any(trigger_has_pull_request_path_filter),
        Value::Mapping(values) => values
            .get(Value::String("pull_request".to_owned()))
            .and_then(Value::as_mapping)
            .is_some_and(|options| {
                options.contains_key(Value::String("paths".to_owned()))
                    || options.contains_key(Value::String("paths-ignore".to_owned()))
            }),
        _ => false,
    }
}

fn matches_any_ordered_path(
    value: &Value,
    changed_paths: &[String],
    workflow: &str,
    filter: &str,
) -> Result<bool, WorkflowError> {
    let patterns = compile_ordered_patterns(value, workflow, filter)?;
    Ok(changed_paths
        .iter()
        .any(|path| matches_compiled_ordered_patterns(&patterns, path)))
}

fn matches_ordered_patterns(
    value: &Value,
    candidate: &str,
    workflow: &str,
    filter: &str,
) -> Result<bool, WorkflowError> {
    let patterns = compile_ordered_patterns(value, workflow, filter)?;
    Ok(matches_compiled_ordered_patterns(&patterns, candidate))
}

fn compile_ordered_patterns(
    value: &Value,
    workflow: &str,
    filter: &str,
) -> Result<Vec<(bool, Regex)>, WorkflowError> {
    let patterns = string_values(value, workflow, filter)?;
    if !patterns.iter().any(|pattern| !pattern.starts_with('!')) {
        return Err(unsupported_workflow(
            workflow,
            format!("pull_request {filter} must contain a positive pattern"),
        ));
    }
    patterns
        .into_iter()
        .map(|pattern| {
            let (negative, pattern) = pattern
                .strip_prefix('!')
                .map_or((false, pattern), |pattern| (true, pattern));
            if pattern.is_empty() {
                return Err(unsupported_workflow(
                    workflow,
                    format!("pull_request {filter} contains an empty pattern"),
                ));
            }
            Ok((negative, compile_glob(pattern, workflow, filter)?))
        })
        .collect()
}

fn matches_compiled_ordered_patterns(patterns: &[(bool, Regex)], candidate: &str) -> bool {
    let mut included = false;
    for (negative, pattern) in patterns {
        if pattern.is_match(candidate) {
            included = !negative;
        }
    }
    included
}

fn matches_ignored_pattern(
    value: &Value,
    candidate: &str,
    workflow: &str,
    filter: &str,
) -> Result<bool, WorkflowError> {
    Ok(compile_ignored_patterns(value, workflow, filter)?
        .iter()
        .any(|pattern| pattern.is_match(candidate)))
}

fn compile_ignored_patterns(
    value: &Value,
    workflow: &str,
    filter: &str,
) -> Result<Vec<Regex>, WorkflowError> {
    string_values(value, workflow, filter)?
        .into_iter()
        .map(|pattern| {
            if pattern.starts_with('!') {
                return Err(unsupported_workflow(
                    workflow,
                    format!("pull_request {filter} does not accept negative patterns"),
                ));
            }
            compile_glob(pattern, workflow, filter)
        })
        .collect()
}

fn string_values<'a>(
    value: &'a Value,
    workflow: &str,
    filter: &str,
) -> Result<Vec<&'a str>, WorkflowError> {
    let values = match value {
        Value::String(value) => vec![value.as_str()],
        Value::Sequence(values) => values
            .iter()
            .map(|value| {
                value.as_str().ok_or_else(|| {
                    unsupported_workflow(
                        workflow,
                        format!("pull_request {filter} entries must be strings"),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => {
            return Err(unsupported_workflow(
                workflow,
                format!("pull_request {filter} must be a string or list"),
            ));
        }
    };
    if values.is_empty() {
        return Err(unsupported_workflow(
            workflow,
            format!("pull_request {filter} cannot be empty"),
        ));
    }
    Ok(values)
}

fn compile_glob(pattern: &str, workflow: &str, filter: &str) -> Result<Regex, WorkflowError> {
    struct Atom {
        source: String,
        quantifiable: bool,
    }

    let characters = pattern.chars().collect::<Vec<_>>();
    let mut atoms = Vec::<Atom>::new();
    let mut index = 0;
    while index < characters.len() {
        match characters[index] {
            '\\' => {
                index += 1;
                let Some(character) = characters.get(index) else {
                    return Err(unsupported_workflow(
                        workflow,
                        format!("pull_request {filter} pattern '{pattern}' ends with an escape"),
                    ));
                };
                atoms.push(Atom {
                    source: regex::escape(&character.to_string()),
                    quantifiable: true,
                });
            }
            '*' if characters.get(index + 1) == Some(&'*') => {
                atoms.push(Atom {
                    source: ".*".to_owned(),
                    quantifiable: false,
                });
                index += 1;
            }
            '*' => atoms.push(Atom {
                source: "[^/]*".to_owned(),
                quantifiable: false,
            }),
            '?' | '+' => {
                let quantifier = characters[index];
                let Some(previous) = atoms.last_mut() else {
                    return Err(unsupported_workflow(
                        workflow,
                        format!(
                            "pull_request {filter} pattern '{pattern}' starts with quantifier '{quantifier}'"
                        ),
                    ));
                };
                if !previous.quantifiable {
                    return Err(unsupported_workflow(
                        workflow,
                        format!(
                            "pull_request {filter} pattern '{pattern}' has repeated or misplaced quantifier '{quantifier}'"
                        ),
                    ));
                }
                previous.source = format!("(?:{}){quantifier}", previous.source);
                previous.quantifiable = false;
            }
            '[' => {
                let start = index + 1;
                let Some(relative_end) = characters[start..]
                    .iter()
                    .position(|character| *character == ']')
                else {
                    return Err(unsupported_workflow(
                        workflow,
                        format!("pull_request {filter} pattern '{pattern}' has an unclosed range"),
                    ));
                };
                let end = start + relative_end;
                let range = characters[start..end].iter().collect::<String>();
                if range.is_empty()
                    || !range
                        .chars()
                        .all(|character| character.is_ascii_alphanumeric() || character == '-')
                {
                    return Err(unsupported_workflow(
                        workflow,
                        format!("pull_request {filter} pattern '{pattern}' has an invalid range"),
                    ));
                }
                atoms.push(Atom {
                    source: format!("[{range}]"),
                    quantifiable: true,
                });
                index = end;
            }
            character => atoms.push(Atom {
                source: regex::escape(&character.to_string()),
                quantifiable: true,
            }),
        }
        index += 1;
    }

    let expression = format!(
        "^(?:{})$",
        atoms
            .into_iter()
            .map(|atom| atom.source)
            .collect::<String>()
    );
    Regex::new(&expression).map_err(|error| {
        unsupported_workflow(
            workflow,
            format!("pull_request {filter} pattern '{pattern}' is invalid: {error}"),
        )
    })
}

fn value_contains_string(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(value) => value == expected,
        Value::Sequence(values) => values.iter().any(|value| value.as_str() == Some(expected)),
        _ => false,
    }
}

fn is_default_action(action: &str) -> bool {
    matches!(action, "opened" | "synchronize" | "reopened")
}

fn scalar_map(values: &BTreeMap<String, Scalar>) -> BTreeMap<String, String> {
    values
        .iter()
        .map(|(key, value)| (key.clone(), value.as_str().to_owned()))
        .collect()
}

fn default_workflow_name() -> String {
    "GitZero workflow".to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    const WORKFLOW: &str = r#"
name: CI
on:
  pull_request:
jobs:
  test:
    runs-on: macos-latest
    env:
      RUST_BACKTRACE: 1
    steps:
      - uses: actions/checkout@v6
      - name: Test
        run: cargo test --workspace
"#;

    #[test]
    fn compiles_checkout_and_shell_steps() {
        let workflow = parse(WORKFLOW).expect("parse");
        assert!(matches_pull_request(&workflow, "opened", "main", None).expect("match trigger"));
        let plan = compile(&workflow, Path::new(".github/workflows/ci.yml")).expect("compile");
        assert_eq!(plan.workflow_name, "CI");
        assert_eq!(plan.workflow_path, ".github/workflows/ci.yml");
        assert_eq!(plan.jobs[0].steps[0].github_action, "actionscheckout");
        assert_eq!(plan.jobs[0].steps[1].github_action, "__run");
        assert_eq!(plan.jobs[0].environment["RUST_BACKTRACE"], "1");
        assert!(matches!(
            &plan.jobs[0].steps[0].kind,
            StepKind::Checkout { inputs } if inputs.is_empty()
        ));
        assert!(matches!(
            &plan.jobs[0].steps[1].kind,
            StepKind::Run { shell, script }
                if shell == "bash -e {0}" && script == "cargo test --workspace"
        ));
    }

    #[test]
    fn preserves_workflow_and_job_concurrency_contracts() {
        let workflow = parse(
            r#"
name: Concurrent
on: pull_request
concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: ${{ !contains(github.ref, 'release/') }}
jobs:
  deploy:
    runs-on: macos-latest
    strategy:
      matrix:
        region: [west, east]
    concurrency:
      group: deploy-${{ matrix.region }}-${{ vars.channel }}
      queue: max
    steps:
      - run: echo deploy
"#,
        )
        .expect("parse concurrency");
        let plan = compile(&workflow, Path::new(".github/workflows/concurrency.yml"))
            .expect("compile concurrency");
        assert_eq!(
            plan.concurrency,
            Some(PlannedConcurrency {
                group: "${{ github.workflow }}-${{ github.ref }}".to_owned(),
                cancel_in_progress: "${{ !contains(github.ref, 'release/') }}".to_owned(),
                queue: PlannedConcurrencyQueue::Single,
            })
        );
        assert_eq!(plan.jobs.len(), 2);
        assert!(plan.jobs.iter().all(|job| {
            job.concurrency
                == Some(PlannedConcurrency {
                    group: "deploy-${{ matrix.region }}-${{ vars.channel }}".to_owned(),
                    cancel_in_progress: "false".to_owned(),
                    queue: PlannedConcurrencyQueue::Max,
                })
        }));
    }

    #[test]
    fn rejects_invalid_concurrency_contexts_and_queue_combinations() {
        let unsupported_context = parse(
            r#"
on: pull_request
concurrency: ${{ matrix.os }}
jobs:
  test:
    runs-on: macos-latest
    steps:
      - run: true
"#,
        )
        .expect("parse unsupported context");
        assert!(
            compile(
                &unsupported_context,
                Path::new(".github/workflows/invalid.yml")
            )
            .expect_err("workflow-level matrix context must fail")
            .to_string()
            .contains("unavailable")
        );

        let invalid_queue = parse(
            r#"
on: pull_request
jobs:
  test:
    runs-on: macos-latest
    concurrency:
      group: deploy
      queue: max
      cancel-in-progress: true
    steps:
      - run: true
"#,
        )
        .expect("parse invalid queue");
        assert!(
            compile(&invalid_queue, Path::new(".github/workflows/invalid.yml"))
                .expect_err("queue max cancellation must fail")
                .to_string()
                .contains("cannot combine")
        );
    }

    #[test]
    fn preserves_string_and_object_deployment_environments() {
        let workflow = parse(
            r#"
name: Environments
on: pull_request
jobs:
  staging:
    runs-on: macos-latest
    environment: staging
    steps:
      - run: echo staging
  production:
    runs-on: macos-latest
    strategy:
      matrix:
        region: [us]
    environment:
      name: production-${{ matrix.region }}
      url: ${{ steps.deploy.outputs.url }}
    steps:
      - id: deploy
        run: echo url=https://example.test >> "$GITHUB_OUTPUT"
"#,
        )
        .expect("parse workflow");
        let plan = compile(&workflow, Path::new(".github/workflows/environments.yml"))
            .expect("compile workflow");

        assert_eq!(
            plan.jobs[0].deployment_environment,
            Some(PlannedEnvironment {
                name: "staging".to_owned(),
                url: None,
            })
        );
        assert_eq!(
            plan.jobs[1].deployment_environment,
            Some(PlannedEnvironment {
                name: "production-${{ matrix.region }}".to_owned(),
                url: Some("${{ steps.deploy.outputs.url }}".to_owned()),
            })
        );
    }

    #[test]
    fn derives_unnamed_workflow_and_repeated_action_identity_like_github() {
        let workflow = parse(
            r#"
on: pull_request
jobs:
  metadata:
    runs-on: macos-latest
    steps:
      - run: echo first
      - run: echo second
      - uses: owner/action@v1
      - uses: owner/action@v1
"#,
        )
        .expect("parse workflow");
        let plan = compile(&workflow, Path::new(".github/workflows/metadata.yml"))
            .expect("compile workflow");

        assert_eq!(plan.workflow_name, ".github/workflows/metadata.yml");
        assert_eq!(
            plan.jobs[0]
                .steps
                .iter()
                .map(|step| step.github_action.as_str())
                .collect::<Vec<_>>(),
            vec!["__run", "__run_2", "owneraction", "owneraction2"]
        );
    }

    #[test]
    fn preserves_actions_for_runtime_resolution() {
        let source = WORKFLOW.replace("actions/checkout@v6", "actions/setup-node@v6");
        let workflow = parse(&source).expect("parse");
        let plan = compile(&workflow, Path::new("ci.yml")).expect("compile");
        assert!(matches!(
            &plan.jobs[0].steps[0].kind,
            StepKind::Uses { action, inputs }
                if action == "actions/setup-node@v6" && inputs.is_empty()
        ));
    }

    #[test]
    fn ignores_non_pull_request_triggers() {
        let source = WORKFLOW.replace("pull_request:", "push:");
        let workflow = parse(&source).expect("parse");
        assert!(!matches_pull_request(&workflow, "opened", "main", None).expect("match trigger"));
    }

    #[test]
    fn honors_pull_request_activity_types() {
        let activity_types = [
            "assigned",
            "unassigned",
            "labeled",
            "unlabeled",
            "opened",
            "edited",
            "closed",
            "reopened",
            "synchronize",
            "converted_to_draft",
            "locked",
            "unlocked",
            "enqueued",
            "dequeued",
            "milestoned",
            "demilestoned",
            "ready_for_review",
            "review_requested",
            "review_request_removed",
            "auto_merge_enabled",
            "auto_merge_disabled",
        ];
        let source = WORKFLOW.replace(
            "pull_request:",
            &format!("pull_request:\n    types: [{}]", activity_types.join(", ")),
        );
        let workflow = parse(&source).expect("parse");
        for action in activity_types {
            assert!(
                matches_pull_request(&workflow, action, "main", None).expect("activity type"),
                "did not match {action}"
            );
        }
        assert!(!matches_pull_request(&workflow, "unknown", "main", None).expect("unknown"));
    }

    #[test]
    fn defaults_pull_request_to_opened_synchronize_and_reopened() {
        let workflow = parse(WORKFLOW).expect("parse");
        for action in ["opened", "synchronize", "reopened"] {
            assert!(matches_pull_request(&workflow, action, "main", None).expect("default type"));
        }
        for action in ["labeled", "ready_for_review", "closed"] {
            assert!(!matches_pull_request(&workflow, action, "main", None).expect("explicit type"));
        }
    }

    #[test]
    fn honors_pull_request_base_branch_patterns_in_order() {
        let source = WORKFLOW.replace(
            "pull_request:",
            "pull_request:\n    branches: [main, 'release/**', '!release/**-alpha', release/special-alpha]",
        );
        let workflow = parse(&source).expect("parse");

        assert!(matches_pull_request(&workflow, "opened", "main", None).expect("main"));
        assert!(matches_pull_request(&workflow, "opened", "release/2.0", None).expect("release"));
        assert!(
            !matches_pull_request(&workflow, "opened", "release/2.0-alpha", None)
                .expect("excluded release")
        );
        assert!(
            matches_pull_request(&workflow, "opened", "release/special-alpha", None)
                .expect("re-included release")
        );
        assert!(!matches_pull_request(&workflow, "opened", "develop", None).expect("develop"));
    }

    #[test]
    fn honors_pull_request_ignored_base_branches() {
        let source = WORKFLOW.replace(
            "pull_request:",
            "pull_request:\n    branches-ignore: ['automation/**']",
        );
        let workflow = parse(&source).expect("parse");

        assert!(
            !matches_pull_request(&workflow, "opened", "automation/dependencies", None)
                .expect("ignored")
        );
        assert!(matches_pull_request(&workflow, "opened", "main", None).expect("main"));
    }

    #[test]
    fn implements_github_branch_glob_quantifiers_and_ranges() {
        let source = WORKFLOW.replace(
            "pull_request:",
            "pull_request:\n    branches: ['release/v[0-9]+.[0-9]+', 'hotfi?x']",
        );
        let workflow = parse(&source).expect("parse");

        assert!(matches_pull_request(&workflow, "opened", "release/v12.3", None).expect("version"));
        assert!(
            !matches_pull_request(&workflow, "opened", "release/v.3", None).expect("missing major")
        );
        assert!(
            matches_pull_request(&workflow, "opened", "hotfix", None).expect("optional present")
        );
        assert!(matches_pull_request(&workflow, "opened", "hotfx", None).expect("optional absent"));
    }

    #[test]
    fn rejects_conflicting_pull_request_branch_filters() {
        let source = WORKFLOW.replace(
            "pull_request:",
            "pull_request:\n    branches: [main]\n    branches-ignore: [develop]",
        );
        let workflow = parse(&source).expect("parse");

        assert!(matches_pull_request(&workflow, "opened", "main", None).is_err());
    }

    #[test]
    fn honors_ordered_pull_request_path_filters() {
        let source = WORKFLOW.replace(
            "pull_request:",
            "pull_request:\n    paths: ['src/**', '!src/generated/**', 'src/generated/keep.rs']",
        );
        let workflow = parse(&source).expect("parse");
        assert!(requires_pull_request_changed_paths(&workflow));
        assert!(matches_pull_request(&workflow, "opened", "main", None).is_err());

        let docs = vec!["docs/readme.md".to_owned()];
        let source = vec!["src/lib.rs".to_owned()];
        let generated = vec!["src/generated/client.rs".to_owned()];
        let re_included = vec!["src/generated/keep.rs".to_owned()];
        assert!(!matches_pull_request(&workflow, "opened", "main", Some(&docs)).expect("docs"));
        assert!(matches_pull_request(&workflow, "opened", "main", Some(&source)).expect("source"));
        assert!(
            !matches_pull_request(&workflow, "opened", "main", Some(&generated))
                .expect("generated")
        );
        assert!(
            matches_pull_request(&workflow, "opened", "main", Some(&re_included))
                .expect("re-included")
        );
    }

    #[test]
    fn honors_pull_request_paths_ignore_only_when_every_file_matches() {
        let source = WORKFLOW.replace(
            "pull_request:",
            "pull_request:\n    paths-ignore: ['docs/**', '**.md']",
        );
        let workflow = parse(&source).expect("parse");
        let ignored = vec!["docs/guide.txt".to_owned(), "README.md".to_owned()];
        let mixed = vec!["docs/guide.txt".to_owned(), "src/lib.rs".to_owned()];
        let empty = Vec::<String>::new();

        assert!(
            !matches_pull_request(&workflow, "opened", "main", Some(&ignored))
                .expect("all ignored")
        );
        assert!(matches_pull_request(&workflow, "opened", "main", Some(&mixed)).expect("mixed"));
        assert!(
            !matches_pull_request(&workflow, "opened", "main", Some(&empty)).expect("empty diff")
        );
    }

    #[test]
    fn rejects_conflicting_pull_request_path_filters() {
        let source = WORKFLOW.replace(
            "pull_request:",
            "pull_request:\n    paths: ['src/**']\n    paths-ignore: ['docs/**']",
        );
        let workflow = parse(&source).expect("parse");
        let paths = vec!["src/lib.rs".to_owned()];
        assert!(matches_pull_request(&workflow, "opened", "main", Some(&paths)).is_err());
    }

    #[test]
    fn preserves_expressions_for_runtime_evaluation() {
        let source = WORKFLOW.replace("cargo test --workspace", "echo '${{ github.sha }}'");
        let workflow = parse(&source).expect("parse");
        let plan = compile(&workflow, Path::new("ci.yml")).expect("compile");
        assert!(matches!(
            &plan.jobs[0].steps[1].kind,
            StepKind::Run { script, .. } if script == "echo '${{ github.sha }}'"
        ));
    }

    #[test]
    fn preserves_checkout_inputs_for_runtime_application() {
        let source = WORKFLOW.replace(
            "- uses: actions/checkout@v6",
            "- uses: actions/checkout@v6\n        with:\n          fetch-depth: 0",
        );
        let workflow = parse(&source).expect("parse");
        let plan = compile(&workflow, Path::new("ci.yml")).expect("compile");
        assert!(matches!(
            &plan.jobs[0].steps[0].kind,
            StepKind::Checkout { inputs } if inputs["fetch-depth"] == "0"
        ));
    }

    #[test]
    fn expands_static_matrix_with_include_and_exclude() {
        let source = r#"
name: Matrix
on: pull_request
jobs:
  test:
    runs-on: ${{ matrix.os }}
    strategy:
      fail-fast: false
      max-parallel: 2
      matrix:
        os: [macos-14, macos-15]
        version: [1, 2]
        exclude:
          - os: macos-14
            version: 1
        include:
          - os: macos-15
            color: green
          - os: macos-13
            version: 3
    steps:
      - run: echo ${{ matrix.version }}
"#;
        let workflow = parse(source).expect("parse");
        let plan = compile(&workflow, Path::new("matrix.yml")).expect("compile");
        assert_eq!(plan.jobs.len(), 4);
        assert!(plan.jobs.iter().all(|job| !job.matrix_fail_fast));
        assert!(
            plan.jobs
                .iter()
                .all(|job| job.matrix_max_parallel == Some(2))
        );
        assert_eq!(
            plan.jobs
                .iter()
                .map(|job| (job.strategy_job_index, job.strategy_job_total))
                .collect::<Vec<_>>(),
            vec![
                (Some(0), Some(4)),
                (Some(1), Some(4)),
                (Some(2), Some(4)),
                (Some(3), Some(4)),
            ]
        );
        assert_eq!(plan.jobs[0].id, "test[1]");
        assert_eq!(
            plan.jobs[1].matrix["color"],
            JsonValue::String("green".to_owned())
        );
        assert_eq!(
            plan.jobs[3].matrix["os"],
            JsonValue::String("macos-13".to_owned())
        );
        assert_eq!(plan.jobs[3].matrix["version"], JsonValue::from(3));
    }

    #[test]
    fn preserves_full_and_dimension_dynamic_matrices_for_runtime() {
        let source = r#"
name: Dynamic matrix
on: pull_request
jobs:
  define:
    runs-on: macos-latest
    outputs:
      colors: ${{ steps.values.outputs.colors }}
      plan: ${{ steps.values.outputs.plan }}
    steps:
      - id: values
        run: echo values
  dimensions:
    needs: define
    runs-on: macos-latest
    strategy:
      matrix:
        color: ${{ fromJSON(needs.define.outputs.colors) }}
    steps:
      - run: echo ${{ matrix.color }}
  full-object:
    needs: define
    runs-on: macos-latest
    strategy:
      matrix: ${{ fromJSON(needs.define.outputs.plan) }}
    steps:
      - run: echo ${{ matrix.project }}
"#;
        let workflow = parse(source).expect("parse");
        let plan = compile(&workflow, Path::new("dynamic.yml")).expect("compile");

        assert_eq!(plan.jobs.len(), 3);
        assert!(plan.jobs[0].dynamic_matrix.is_none());
        assert_eq!(plan.jobs[1].id, "dimensions[1]");
        assert!(matches!(
            &plan.jobs[1].dynamic_matrix,
            Some(Value::Mapping(matrix))
                if matrix.contains_key(Value::String("color".to_owned()))
        ));
        assert_eq!(
            plan.jobs[2].dynamic_matrix,
            Some(Value::String(
                "${{ fromJSON(needs.define.outputs.plan) }}".to_owned()
            ))
        );
    }

    #[test]
    fn include_only_matrix_creates_one_job_per_entry() {
        let matrix: Value = serde_yaml_ng::from_str(
            r#"
include:
  - project: api
    config: debug
  - project: app
    config: release
"#,
        )
        .expect("parse matrix");
        let combinations =
            expand_matrix_definition("Matrix", "build", &matrix).expect("expand matrix");

        assert_eq!(combinations.len(), 2);
        assert_eq!(combinations[0]["project"], JsonValue::String("api".into()));
        assert_eq!(
            combinations[1]["config"],
            JsonValue::String("release".into())
        );
    }

    #[test]
    fn accepts_multiple_jobs_and_validates_dependencies() {
        let source = r#"
name: Jobs
on: pull_request
jobs:
  build:
    runs-on: macos-latest
    timeout-minutes: 15
    outputs:
      artifact: ${{ steps.build.outputs.artifact }}
    steps:
      - id: build
        run: echo build
  test:
    needs: build
    if: ${{ success() }}
    runs-on: macos-latest
    steps:
      - run: echo test
"#;
        let workflow = parse(source).expect("parse");
        let plan = compile(&workflow, Path::new("jobs.yml")).expect("compile");
        assert_eq!(plan.jobs.len(), 2);
        assert_eq!(plan.jobs[0].timeout_minutes.as_deref(), Some("15"));
        assert_eq!(
            plan.jobs[0].outputs["artifact"],
            "${{ steps.build.outputs.artifact }}"
        );
        assert_eq!(plan.jobs[1].needs, ["build"]);
        assert_eq!(plan.jobs[1].condition.as_deref(), Some("${{ success() }}"));

        let cyclic = source.replace("build:\n", "build:\n    needs: test\n");
        let workflow = parse(&cyclic).expect("parse cyclic");
        let error = compile(&workflow, Path::new("jobs.yml")).expect_err("reject cycle");
        assert!(error.to_string().contains("cycle"));
    }

    #[test]
    fn links_same_repository_reusable_workflows() {
        let caller = parse(
            r#"
name: Caller
on: pull_request
jobs:
  prepare:
    runs-on: macos-latest
    outputs:
      target: ${{ steps.target.outputs.value }}
    steps:
      - id: target
        run: echo target
  reusable:
    needs: prepare
    uses: ./.github/workflows/reusable.yml
    concurrency: caller-${{ needs.prepare.outputs.target }}
    with:
      target: ${{ needs.prepare.outputs.target }}
      release: true
    secrets:
      workflow_token: ${{ secrets.GITHUB_TOKEN }}
  finish:
    needs: reusable
    runs-on: macos-latest
    steps:
      - run: echo ${{ needs.reusable.outputs.artifact }}
"#,
        )
        .expect("parse caller");
        let reusable = parse(
            r#"
name: Reusable
on:
  workflow_call:
    inputs:
      target:
        required: true
        type: string
      release:
        type: boolean
        default: false
    secrets:
      workflow_token:
        required: true
    outputs:
      artifact:
        description: Built artifact
        value: ${{ jobs.build.outputs.artifact }}
concurrency: reusable-${{ inputs.target }}
jobs:
  build:
    runs-on: macos-latest
    timeout-minutes: 15
    outputs:
      artifact: ${{ steps.build.outputs.artifact }}
    steps:
      - id: build
        run: echo ${{ inputs.target }}-${{ inputs.release }}
  verify:
    needs: build
    runs-on: macos-latest
    steps:
      - run: echo ${{ needs.build.outputs.artifact }}
"#,
        )
        .expect("parse reusable");
        let reusable_workflows =
            BTreeMap::from([(".github/workflows/reusable.yml".to_owned(), reusable)]);

        let plan = compile_with_local_reusables(
            &caller,
            Path::new(".github/workflows/ci.yml"),
            &reusable_workflows,
        )
        .expect("link reusable workflow");

        let base_ids = plan
            .jobs
            .iter()
            .map(|job| job.base_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            base_ids,
            [
                "prepare",
                "reusable::gate",
                "reusable::build",
                "reusable::verify",
                "reusable",
                "finish",
            ]
        );
        let gate = plan
            .jobs
            .iter()
            .find(|job| job.base_id == "reusable::gate")
            .expect("reusable gate");
        assert_eq!(gate.concurrency_acquire.len(), 2);
        assert_eq!(
            gate.concurrency_acquire[0].configuration.group,
            "caller-${{ needs.prepare.outputs.target }}"
        );
        assert_eq!(
            gate.concurrency_acquire[1].configuration.group,
            "reusable-${{ inputs.target }}"
        );
        assert_eq!(gate.concurrency_acquire[1].reusable_input_scopes.len(), 1);
        let build = plan
            .jobs
            .iter()
            .find(|job| job.base_id == "reusable::build")
            .expect("reusable build");
        assert_eq!(build.concurrency_scope_ids.len(), 2);
        let result = plan
            .jobs
            .iter()
            .find(|job| job.base_id == "reusable")
            .expect("reusable result");
        assert_eq!(
            result.concurrency_release,
            [
                "reusable::workflow-concurrency",
                "reusable::caller-concurrency"
            ]
        );
        assert!(matches!(
            plan.jobs[1].virtual_job,
            Some(PlannedVirtualJob::ReusableGate)
        ));
        assert_eq!(plan.jobs[2].needs, ["reusable::gate"]);
        assert_eq!(plan.jobs[2].timeout_minutes.as_deref(), Some("15"));
        assert_eq!(plan.jobs[2].need_aliases["prepare"], "prepare");
        assert_eq!(
            plan.jobs[2].reusable_input_scopes[0].inputs["release"].input_type,
            ReusableInputType::Boolean
        );
        assert_eq!(
            plan.jobs[2].reusable_secret_scopes[0].mappings["workflow_token"],
            "GITHUB_TOKEN"
        );
        assert!(
            plan.jobs[2].reusable_secret_scopes[0]
                .required
                .contains("workflow_token")
        );
        assert_eq!(plan.jobs[3].need_aliases["build"], "reusable::build");
        assert!(matches!(
            &plan.jobs[4].virtual_job,
            Some(PlannedVirtualJob::ReusableResult { jobs, outputs })
                if jobs["build"] == "reusable::build"
                    && outputs["artifact"] == "${{ jobs.build.outputs.artifact }}"
        ));
        assert_eq!(plan.jobs[5].needs, ["reusable"]);
    }

    #[test]
    fn validates_named_reusable_workflow_secret_contracts_without_exposing_values() {
        let called = parse(
            r#"
name: Called
on:
  workflow_call:
    secrets:
      api_token:
        description: Repository API token
        required: true
jobs:
  use-token:
    runs-on: macos-latest
    steps:
      - run: test -n "${{ secrets.api_token }}"
"#,
        )
        .expect("parse called workflow");
        let workflows = BTreeMap::from([(".github/workflows/called.yml".to_owned(), called)]);
        let compile_caller = |secrets: &str| {
            let caller = parse(&format!(
                "name: Caller\non: pull_request\njobs:\n  call:\n    uses: ./.github/workflows/called.yml\n{secrets}"
            ))
            .expect("parse caller");
            compile_with_local_reusables(&caller, Path::new(".github/workflows/ci.yml"), &workflows)
        };

        let plan = compile_caller("    secrets:\n      api_token: ${{ secrets.GITHUB_TOKEN }}\n")
            .expect("bind built-in token alias");
        assert_eq!(
            plan.jobs[1].reusable_secret_scopes[0].mappings["api_token"],
            "GITHUB_TOKEN"
        );

        let missing = compile_caller("").expect_err("reject missing required secret");
        assert!(
            missing
                .to_string()
                .contains("required reusable workflow secret 'api_token'")
        );

        let literal = compile_caller("    secrets:\n      api_token: literal-sensitive-value\n")
            .expect_err("reject literal secret");
        let message = literal.to_string();
        assert!(message.contains("must reference an available secret"));
        assert!(!message.contains("literal-sensitive-value"));

        let undeclared =
            compile_caller("    secrets:\n      other_token: ${{ secrets.GITHUB_TOKEN }}\n")
                .expect_err("reject undeclared secret");
        assert!(
            undeclared
                .to_string()
                .contains("does not declare secret 'other_token'")
        );
    }

    #[test]
    fn links_preloaded_remote_reusable_workflows() {
        let remote = parse(
            r#"
name: Caller
on: pull_request
jobs:
  reusable:
    uses: owner/repository/.github/workflows/ci.yml@main
"#,
        )
        .expect("parse remote caller");
        let called = parse(
            r#"
name: Remote reusable
on: workflow_call
jobs:
  build:
    runs-on: macos-latest
    steps:
      - run: echo remote
"#,
        )
        .expect("parse remote reusable");
        let remote_reference = "owner/repository/.github/workflows/ci.yml@main";
        let workflows = BTreeMap::from([(remote_reference.to_owned(), called)]);
        let plan =
            compile_with_reusables(&remote, Path::new(".github/workflows/ci.yml"), &workflows)
                .expect("link preloaded remote call");
        assert_eq!(
            plan.jobs
                .iter()
                .map(|job| job.base_id.as_str())
                .collect::<Vec<_>>(),
            ["reusable::gate", "reusable::build", "reusable"]
        );

        let error = compile_with_reusables(
            &remote,
            Path::new(".github/workflows/ci.yml"),
            &BTreeMap::new(),
        )
        .expect_err("reject unresolved remote call");
        assert!(error.to_string().contains("was not resolved"));
    }

    #[test]
    fn rejects_unsupported_reusable_workflow_contracts() {
        let caller = parse(
            r#"
name: Caller
on: pull_request
jobs:
  reusable:
    uses: ./.github/workflows/reusable.yml
"#,
        )
        .expect("parse caller");
        let malformed = parse(
            r#"
name: Reusable
on:
  workflow_call: invalid
jobs:
  build:
    runs-on: macos-latest
    steps:
      - run: echo build
"#,
        )
        .expect("parse malformed reusable");
        let workflows = BTreeMap::from([(".github/workflows/reusable.yml".to_owned(), malformed)]);
        let error = compile_with_local_reusables(
            &caller,
            Path::new(".github/workflows/ci.yml"),
            &workflows,
        )
        .expect_err("reject malformed call contract");
        assert!(error.to_string().contains("invalid workflow_call"));
    }

    #[test]
    fn links_nested_reusable_workflows_without_losing_scopes() {
        let caller = parse(
            r#"
name: Caller
on: pull_request
jobs:
  outer:
    uses: ./.github/workflows/outer.yml
    with:
      enabled: true
    secrets:
      outer_token: ${{ github.token }}
"#,
        )
        .expect("parse caller");
        let outer = parse(
            r#"
name: Outer
on:
  workflow_call:
    inputs:
      enabled:
        type: boolean
    secrets:
      outer_token:
        required: true
    outputs:
      result:
        value: ${{ jobs.inner.outputs.result }}
jobs:
  prepare:
    runs-on: macos-latest
    outputs:
      label: ${{ steps.label.outputs.value }}
    steps:
      - id: label
        run: echo label
  inner:
    needs: prepare
    uses: ./.github/workflows/inner.yml
    concurrency: outer-${{ inputs.enabled }}
    with:
      label: ${{ needs.prepare.outputs.label }}
      enabled: ${{ inputs.enabled }}
    secrets:
      inner_token: ${{ secrets.outer_token }}
"#,
        )
        .expect("parse outer reusable");
        let inner = parse(
            r#"
name: Inner
on:
  workflow_call:
    inputs:
      label:
        required: true
        type: string
      enabled:
        required: true
        type: boolean
    secrets:
      inner_token:
        required: true
    outputs:
      result:
        value: ${{ jobs.build.outputs.result }}
concurrency: inner-${{ inputs.label }}
jobs:
  build:
    runs-on: macos-latest
    outputs:
      result: ${{ steps.build.outputs.result }}
    steps:
      - id: build
        run: echo ${{ inputs.label }}-${{ inputs.enabled }}
"#,
        )
        .expect("parse inner reusable");
        let workflows = BTreeMap::from([
            (".github/workflows/outer.yml".to_owned(), outer),
            (".github/workflows/inner.yml".to_owned(), inner),
        ]);

        let plan = compile_with_local_reusables(
            &caller,
            Path::new(".github/workflows/ci.yml"),
            &workflows,
        )
        .expect("link nested reusable workflows");
        let inner_gate = plan
            .jobs
            .iter()
            .find(|job| job.base_id == "outer::inner::gate")
            .expect("nested reusable gate");
        assert_eq!(inner_gate.concurrency_acquire.len(), 2);
        assert_eq!(
            inner_gate.concurrency_acquire[0]
                .reusable_input_scopes
                .len(),
            1
        );
        assert_eq!(
            inner_gate.concurrency_acquire[1]
                .reusable_input_scopes
                .len(),
            2
        );
        assert_eq!(
            plan.jobs
                .iter()
                .map(|job| job.base_id.as_str())
                .collect::<Vec<_>>(),
            [
                "outer::gate",
                "outer::prepare",
                "outer::inner::gate",
                "outer::inner::build",
                "outer::inner",
                "outer",
            ]
        );
        let nested = &plan.jobs[3];
        assert_eq!(nested.reusable_input_scopes.len(), 2);
        assert_eq!(
            nested.reusable_input_scopes[0].inputs["enabled"].value,
            Value::Bool(true)
        );
        assert_eq!(
            nested.reusable_input_scopes[1].inputs["enabled"].value,
            Value::String("${{ inputs.enabled }}".to_owned())
        );
        assert_eq!(nested.reusable_secret_scopes.len(), 2);
        assert_eq!(
            nested.reusable_secret_scopes[0].mappings["outer_token"],
            "GITHUB_TOKEN"
        );
        assert_eq!(
            nested.reusable_secret_scopes[1].mappings["inner_token"],
            "outer_token"
        );
        assert_eq!(nested.need_aliases["prepare"], "outer::prepare");
        assert!(matches!(
            &plan.jobs[4].virtual_job,
            Some(PlannedVirtualJob::ReusableResult { jobs, .. })
                if jobs["build"] == "outer::inner::build"
        ));
        assert!(matches!(
            &plan.jobs[5].virtual_job,
            Some(PlannedVirtualJob::ReusableResult { jobs, outputs })
                if jobs.len() == 2
                    && jobs["prepare"] == "outer::prepare"
                    && jobs["inner"] == "outer::inner"
                    && outputs["result"] == "${{ jobs.inner.outputs.result }}"
        ));
    }

    #[test]
    fn expands_static_matrix_reusable_workflow_calls() {
        let caller = parse(
            r#"
name: Caller matrix
on: pull_request
jobs:
  reusable:
    strategy:
      fail-fast: false
      max-parallel: 1
      matrix:
        flavor: [alpha, beta]
    uses: ./.github/workflows/reusable.yml
    with:
      flavor: ${{ matrix.flavor }}
"#,
        )
        .expect("parse caller");
        let reusable = parse(
            r#"
name: Reusable
on:
  workflow_call:
    inputs:
      flavor:
        required: true
        type: string
    outputs:
      result:
        value: ${{ jobs.build.outputs.result }}
jobs:
  build:
    runs-on: macos-latest
    outputs:
      result: ${{ steps.build.outputs.result }}
    steps:
      - id: build
        run: echo ${{ inputs.flavor }}
"#,
        )
        .expect("parse reusable");
        let workflows = BTreeMap::from([(".github/workflows/reusable.yml".to_owned(), reusable)]);

        let plan = compile_with_local_reusables(
            &caller,
            Path::new(".github/workflows/ci.yml"),
            &workflows,
        )
        .expect("link matrix reusable workflow");
        assert_eq!(
            plan.jobs
                .iter()
                .map(|job| job.base_id.as_str())
                .collect::<Vec<_>>(),
            [
                "reusable::matrix-1::gate",
                "reusable::matrix-1::build",
                "reusable::matrix-1",
                "reusable::matrix-2::gate",
                "reusable::matrix-2::build",
                "reusable::matrix-2",
                "reusable",
            ]
        );
        assert_eq!(
            plan.jobs[1].reusable_input_scopes[0].matrix["flavor"],
            JsonValue::String("alpha".to_owned())
        );
        assert_eq!(
            plan.jobs[4].reusable_input_scopes[0].matrix["flavor"],
            JsonValue::String("beta".to_owned())
        );
        assert!(matches!(
            &plan.jobs[6].virtual_job,
            Some(PlannedVirtualJob::ReusableMatrixResult {
                invocations,
                max_parallel: 1,
                fail_fast: false,
            }) if invocations == &["reusable::matrix-1", "reusable::matrix-2"]
        ));
    }

    #[test]
    fn preserves_dynamic_matrix_reusable_workflow_calls_for_runtime() {
        let caller = parse(
            r#"
name: Caller matrix
on: pull_request
jobs:
  reusable:
    strategy:
      matrix:
        flavor: ${{ fromJSON('["alpha"]') }}
    uses: ./.github/workflows/reusable.yml
    secrets:
      workflow_token: ${{ secrets.GITHUB_TOKEN }}
"#,
        )
        .expect("parse caller");
        let reusable = parse(
            r#"
name: Reusable
on:
  workflow_call:
    secrets:
      workflow_token:
        required: true
jobs:
  build:
    runs-on: macos-latest
    steps:
      - run: echo build
"#,
        )
        .expect("parse reusable");
        let workflows = BTreeMap::from([(".github/workflows/reusable.yml".to_owned(), reusable)]);
        let plan = compile_with_local_reusables(
            &caller,
            Path::new(".github/workflows/ci.yml"),
            &workflows,
        )
        .expect("compile dynamic call matrix");
        assert_eq!(plan.jobs.len(), 1);
        assert!(plan.jobs[0].dynamic_matrix.is_some());
        assert!(matches!(
            &plan.jobs[0].virtual_job,
            Some(PlannedVirtualJob::ReusableDynamicCall(call))
                if call.called_plan.jobs.len() == 1
        ));

        let template = plan.jobs[0].clone();
        let call = match template.virtual_job.clone() {
            Some(PlannedVirtualJob::ReusableDynamicCall(call)) => call,
            _ => panic!("expected dynamic reusable-call template"),
        };
        assert_eq!(call.secrets.mappings["workflow_token"], "GITHUB_TOKEN");
        assert!(call.secrets.required.contains("workflow_token"));
        let instances = ["alpha", "beta"]
            .into_iter()
            .enumerate()
            .map(|(index, flavor)| {
                let mut instance = template.clone();
                instance.id = format!("reusable[{}]", index + 1);
                instance.dynamic_matrix = None;
                instance
                    .matrix
                    .insert("flavor".to_owned(), JsonValue::String(flavor.to_owned()));
                instance
            })
            .collect();
        let expanded = expand_dynamic_reusable_call(&template, instances, &call);
        assert_eq!(
            expanded
                .iter()
                .map(|job| job.base_id.as_str())
                .collect::<Vec<_>>(),
            [
                "reusable::matrix-1::gate",
                "reusable::matrix-1::build",
                "reusable::matrix-1",
                "reusable::matrix-2::gate",
                "reusable::matrix-2::build",
                "reusable::matrix-2",
                "reusable",
            ]
        );
        assert_eq!(
            expanded[1].reusable_input_scopes[0].matrix["flavor"],
            JsonValue::String("alpha".to_owned())
        );
        assert_eq!(
            expanded[4].reusable_input_scopes[0].matrix["flavor"],
            JsonValue::String("beta".to_owned())
        );
        for index in [1, 4] {
            assert_eq!(
                expanded[index].reusable_secret_scopes[0].mappings["workflow_token"],
                "GITHUB_TOKEN"
            );
        }
        assert!(matches!(
            &expanded[6].virtual_job,
            Some(PlannedVirtualJob::ReusableMatrixResult { invocations, .. })
                if invocations == &["reusable::matrix-1", "reusable::matrix-2"]
        ));
    }

    #[test]
    fn rejects_reusable_workflow_cycles_and_excessive_depth() {
        let caller = parse(
            r#"
name: Caller
on: pull_request
jobs:
  call:
    uses: ./.github/workflows/one.yml
"#,
        )
        .expect("parse caller");
        let recursive = |name: &str, target: &str| {
            parse(&format!(
                "name: {name}\non: workflow_call\njobs:\n  call:\n    uses: ./.github/workflows/{target}.yml\n"
            ))
            .expect("parse recursive workflow")
        };
        let cycle = BTreeMap::from([
            (
                ".github/workflows/one.yml".to_owned(),
                recursive("One", "two"),
            ),
            (
                ".github/workflows/two.yml".to_owned(),
                recursive("Two", "one"),
            ),
        ]);
        let error =
            compile_with_local_reusables(&caller, Path::new(".github/workflows/ci.yml"), &cycle)
                .expect_err("reject reusable cycle");
        assert!(error.to_string().contains("call cycle"));

        let mut too_deep = BTreeMap::new();
        for level in 1..=10 {
            let workflow = if level == 10 {
                parse(
                    "name: Ten\non: workflow_call\njobs:\n  build:\n    runs-on: macos-latest\n    steps:\n      - run: echo ten\n",
                )
                .expect("parse terminal workflow")
            } else {
                recursive(&format!("Level {level}"), &format!("level-{}", level + 1))
            };
            let filename = if level == 1 {
                "one".to_owned()
            } else {
                format!("level-{level}")
            };
            too_deep.insert(format!(".github/workflows/{filename}.yml"), workflow);
        }
        let error =
            compile_with_local_reusables(&caller, Path::new(".github/workflows/ci.yml"), &too_deep)
                .expect_err("reject eleventh workflow level");
        assert!(error.to_string().contains("exceeds 10"));
    }

    #[test]
    fn rejects_more_than_fifty_unique_reusable_workflows() {
        let mut caller = "name: Caller\non: pull_request\njobs:\n".to_owned();
        let called = parse(
            "name: Called\non: workflow_call\njobs:\n  build:\n    runs-on: macos-latest\n    steps:\n      - run: echo build\n",
        )
        .expect("parse called workflow");
        let mut workflows = BTreeMap::new();
        for index in 1..=MAX_UNIQUE_REUSABLE_WORKFLOWS + 1 {
            let reference = format!("owner/repository/.github/workflows/called-{index}.yml@main");
            caller.push_str(&format!("  call-{index}:\n    uses: {reference}\n"));
            workflows.insert(reference, called.clone());
        }
        let caller = parse(&caller).expect("parse caller");
        let error =
            compile_with_reusables(&caller, Path::new(".github/workflows/ci.yml"), &workflows)
                .expect_err("reject too many unique reusable workflows");
        assert!(error.to_string().contains("more than 50 unique"));
    }

    #[test]
    fn applies_run_defaults_and_preserves_continue_on_error() {
        let source = r#"
name: Defaults
run-name: PR checks
on: pull_request
permissions: read-all
defaults:
  run:
    shell: bash
    working-directory: scripts
jobs:
  test:
    continue-on-error: ${{ matrix.experimental }}
    timeout-minutes: ${{ matrix.timeout }}
    runs-on: macos-latest
    strategy:
      matrix:
        experimental: [true]
        timeout: [10]
    steps:
      - run: false
        continue-on-error: true
        timeout-minutes: 5
"#;
        let workflow = parse(source).expect("parse");
        let plan = compile(&workflow, Path::new("defaults.yml")).expect("compile");
        let job = &plan.jobs[0];
        assert_eq!(
            job.continue_on_error.as_deref(),
            Some("${{ matrix.experimental }}")
        );
        assert_eq!(
            job.timeout_minutes.as_deref(),
            Some("${{ matrix.timeout }}")
        );
        assert_eq!(job.steps[0].working_directory.as_deref(), Some("scripts"));
        assert_eq!(job.steps[0].continue_on_error.as_deref(), Some("true"));
        assert_eq!(job.steps[0].timeout_minutes.as_deref(), Some("5"));
        assert!(matches!(
            &job.steps[0].kind,
            StepKind::Run { shell, .. } if shell == "bash"
        ));
        assert_eq!(job.permissions.read.len(), 15);
        assert!(job.permissions.write.is_empty());
        assert!(job.permissions.read.contains("vulnerability-alerts"));
    }

    #[test]
    fn applies_exact_read_and_write_permissions_and_rejects_oidc() {
        let workflow = parse(
            r#"
name: Permissions
on: pull_request
permissions:
  contents: read
  pull-requests: read
jobs:
  inherited:
    runs-on: macos-latest
    steps:
      - run: echo inherited
  reduced:
    permissions:
      checks: write
      contents: none
    runs-on: macos-latest
    steps:
      - run: echo reduced
  none:
    permissions: {}
    runs-on: macos-latest
    steps:
      - run: echo none
"#,
        )
        .expect("parse permissions");
        let plan = compile(&workflow, Path::new("permissions.yml")).expect("compile permissions");
        assert_eq!(
            plan.jobs[0].permissions.read,
            BTreeSet::from(["contents".to_owned(), "pull-requests".to_owned()])
        );
        assert!(plan.jobs[0].permissions.write.is_empty());
        assert!(plan.jobs[1].permissions.read.is_empty());
        assert_eq!(
            plan.jobs[1].permissions.write,
            BTreeSet::from(["checks".to_owned()])
        );
        assert!(plan.jobs[2].permissions.read.is_empty());
        assert!(plan.jobs[2].permissions.write.is_empty());

        let oidc = parse(
            "name: OIDC\non: pull_request\npermissions:\n  id-token: write\njobs:\n  build:\n    runs-on: macos-latest\n    steps:\n      - run: echo build\n",
        )
        .expect("parse OIDC permissions");
        let error = compile(&oidc, Path::new("oidc.yml")).expect_err("reject OIDC permission");
        assert!(error.to_string().contains("id-token: write"));
        assert!(error.to_string().contains("OIDC"));

        let write_all = parse(
            "name: Write all\non: pull_request\npermissions: write-all\njobs:\n  build:\n    runs-on: macos-latest\n    steps:\n      - run: echo build\n",
        )
        .expect("parse write-all permissions");
        let error = compile(&write_all, Path::new("write-all.yml")).expect_err("reject write-all");
        assert!(error.to_string().contains("write-all"));
        assert!(error.to_string().contains("id-token: write"));

        let vulnerability_write = parse(
            "name: Dependabot\non: pull_request\npermissions:\n  vulnerability-alerts: write\njobs:\n  build:\n    runs-on: macos-latest\n    steps:\n      - run: echo build\n",
        )
        .expect("parse Dependabot permission");
        let error = compile(&vulnerability_write, Path::new("vulnerability-write.yml"))
            .expect_err("reject read-only permission write");
        assert!(error.to_string().contains("vulnerability-alerts"));
        assert!(error.to_string().contains("only read or none"));
    }

    #[test]
    fn reusable_workflows_cannot_elevate_caller_permissions() {
        let caller = parse(
            r#"
name: Caller
on: pull_request
jobs:
  call:
    permissions:
      contents: read
      issues: write
    uses: ./.github/workflows/called.yml
"#,
        )
        .expect("parse caller");
        let called = parse(
            r#"
name: Called
on: workflow_call
permissions:
  checks: read
  contents: write
  issues: write
jobs:
  build:
    runs-on: macos-latest
    steps:
      - run: echo build
"#,
        )
        .expect("parse called workflow");
        let plan = compile_with_local_reusables(
            &caller,
            Path::new(".github/workflows/ci.yml"),
            &BTreeMap::from([(".github/workflows/called.yml".to_owned(), called)]),
        )
        .expect("compile reusable workflow");
        let build = plan
            .jobs
            .iter()
            .find(|job| job.base_id == "call::build")
            .expect("called build job");
        assert_eq!(
            build.permissions.read,
            BTreeSet::from(["contents".to_owned()])
        );
        assert_eq!(
            build.permissions.write,
            BTreeSet::from(["issues".to_owned()])
        );
    }
}
