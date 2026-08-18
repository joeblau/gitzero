use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use uuid::Uuid;

pub const PROTOCOL_VERSION: u16 = 8;
pub const MAX_RUNNER_LABELS: usize = 32;
pub const MAX_RUNNER_REQUIREMENTS: usize = 512;
pub const MAX_RUNNER_SELECTOR_BYTES: usize = 256;
pub const MAX_CHECK_ANNOTATIONS: usize = 50;
pub const MAX_CHECK_ANNOTATION_PATH_BYTES: usize = 4_096;
pub const MAX_CHECK_ANNOTATION_MESSAGE_BYTES: usize = 65_536;
pub const MAX_CHECK_ANNOTATION_TITLE_BYTES: usize = 255;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHello {
    pub protocol_version: u16,
    pub agent_id: String,
    pub name: String,
    pub version: String,
    pub labels: Vec<String>,
    #[serde(default)]
    pub runner_group: Option<String>,
    pub max_parallelism: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RunnerRequirement {
    pub labels: Vec<String>,
    #[serde(default)]
    pub runner_group: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositorySpec {
    pub owner: String,
    pub name: String,
    pub clone_url: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequestSpec {
    pub number: u64,
    pub action: String,
    pub head_sha: String,
    pub base_sha: String,
    pub head_ref: String,
    pub base_ref: String,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RepositoryToken(String);

impl RepositoryToken {
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl From<String> for RepositoryToken {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl std::fmt::Debug for RepositoryToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConcurrencyQueue {
    #[default]
    Single,
    Max,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSpec {
    pub id: Uuid,
    pub workspace_id: String,
    pub installation_id: u64,
    #[serde(default)]
    pub run_number: u64,
    pub repository: RepositorySpec,
    pub pull_request: PullRequestSpec,
    pub check_run_id: Option<u64>,
    #[serde(default = "default_event")]
    pub event: JsonValue,
    pub checkout_token: String,
    #[serde(default)]
    pub environment_token: String,
    #[serde(default = "default_github_api_version")]
    pub github_api_version: String,
    #[serde(default)]
    pub changed_paths: Option<Vec<String>>,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub variables: BTreeMap<String, String>,
}

impl std::fmt::Debug for RunSpec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RunSpec")
            .field("id", &self.id)
            .field("workspace_id", &self.workspace_id)
            .field("installation_id", &self.installation_id)
            .field("run_number", &self.run_number)
            .field("repository", &self.repository)
            .field("pull_request", &self.pull_request)
            .field("check_run_id", &self.check_run_id)
            .field(
                "event_action",
                &self.event.get("action").and_then(JsonValue::as_str),
            )
            .field("checkout_token", &"[REDACTED]")
            .field("environment_token", &"[REDACTED]")
            .field("github_api_version", &self.github_api_version)
            .field(
                "changed_path_count",
                &self.changed_paths.as_ref().map(Vec::len),
            )
            .field("environment", &self.environment)
            .field("variable_count", &self.variables.len())
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    Welcome {
        protocol_version: u16,
        heartbeat_interval_seconds: u16,
    },
    RunJob {
        job: Box<RunSpec>,
    },
    CancelJob {
        job_id: Uuid,
        reason: String,
    },
    ConcurrencyGranted {
        request_id: Uuid,
    },
    ConcurrencyCancelled {
        request_id: Uuid,
        reason: String,
    },
    RepositoryTokenGranted {
        request_id: Uuid,
        token: RepositoryToken,
    },
    RepositoryTokenDenied {
        request_id: Uuid,
        reason: String,
    },
    WorkflowTokenGranted {
        request_id: Uuid,
        token: RepositoryToken,
    },
    WorkflowTokenDenied {
        request_id: Uuid,
        reason: String,
    },
    Ack {
        message_id: Uuid,
    },
    Error {
        code: String,
        message: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentMessage {
    Hello {
        hello: AgentHello,
    },
    Heartbeat {
        message_id: Uuid,
        running_job_ids: Vec<Uuid>,
    },
    JobStarted {
        message_id: Uuid,
        job_id: Uuid,
    },
    JobRejected {
        message_id: Uuid,
        job_id: Uuid,
        requirements: Vec<RunnerRequirement>,
        reason: String,
    },
    ConcurrencyAcquire {
        message_id: Uuid,
        job_id: Uuid,
        request_id: Uuid,
        unit_id: String,
        group: String,
        cancel_in_progress: bool,
        queue: ConcurrencyQueue,
    },
    ConcurrencyRelease {
        message_id: Uuid,
        job_id: Uuid,
        request_id: Uuid,
    },
    RepositoryTokenRequest {
        message_id: Uuid,
        job_id: Uuid,
        request_id: Uuid,
        owner: String,
        repository: String,
    },
    WorkflowTokenRequest {
        message_id: Uuid,
        job_id: Uuid,
        request_id: Uuid,
        read_permissions: Vec<String>,
        write_permissions: Vec<String>,
    },
    StepStarted {
        message_id: Uuid,
        job_id: Uuid,
        step_id: String,
        name: String,
    },
    LogChunk {
        message_id: Uuid,
        job_id: Uuid,
        step_id: String,
        sequence: u64,
        stream: LogStream,
        data: String,
    },
    StepFinished {
        message_id: Uuid,
        job_id: Uuid,
        step_id: String,
        conclusion: Conclusion,
        exit_code: Option<i32>,
    },
    JobFinished {
        message_id: Uuid,
        job_id: Uuid,
        conclusion: Conclusion,
        summary: String,
        annotations: Vec<CheckAnnotation>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogStream {
    Stdout,
    Stderr,
    System,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Conclusion {
    Success,
    Failure,
    Cancelled,
    TimedOut,
    Neutral,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckAnnotationLevel {
    Notice,
    Warning,
    Failure,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckAnnotation {
    pub path: String,
    pub start_line: u32,
    pub end_line: u32,
    pub start_column: Option<u32>,
    pub end_column: Option<u32>,
    pub annotation_level: CheckAnnotationLevel,
    pub message: String,
    pub title: Option<String>,
}

fn default_github_api_version() -> String {
    "2026-03-10".to_owned()
}

fn default_event() -> JsonValue {
    JsonValue::Object(Default::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_spec_debug_redacts_checkout_token() {
        let spec = fixture_run();
        let rendered = format!("{spec:?}");
        assert!(rendered.contains("[REDACTED]"));
        assert!(!rendered.contains("secret-token"));
        assert!(!rendered.contains("environment-secret-token"));
        assert!(!rendered.contains("configured-variable-value"));
    }

    #[test]
    fn messages_round_trip_with_stable_discriminator() {
        let message = ServerMessage::RunJob {
            job: Box::new(fixture_run()),
        };
        let json = serde_json::to_string(&message).expect("serialize");
        assert!(json.contains(r#""type":"run_job""#));
        let decoded: ServerMessage = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, message);

        let rejected = AgentMessage::JobRejected {
            message_id: Uuid::nil(),
            job_id: Uuid::nil(),
            requirements: vec![RunnerRequirement {
                labels: vec!["self-hosted".into(), "macOS".into(), "xcode-16".into()],
                runner_group: Some("release-minis".into()),
            }],
            reason: "runner selectors do not match this Mac".into(),
        };
        let json = serde_json::to_string(&rejected).expect("serialize rejection");
        assert!(json.contains(r#""type":"job_rejected""#));
        assert_eq!(
            serde_json::from_str::<AgentMessage>(&json).expect("deserialize rejection"),
            rejected
        );

        let request_id = Uuid::new_v4();
        let acquire = AgentMessage::ConcurrencyAcquire {
            message_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            request_id,
            unit_id: "workflow:build / test".into(),
            group: "pull-request-17".into(),
            cancel_in_progress: true,
            queue: ConcurrencyQueue::Single,
        };
        let json = serde_json::to_string(&acquire).expect("serialize concurrency request");
        assert!(json.contains(r#""type":"concurrency_acquire""#));
        assert!(json.contains(r#""queue":"single""#));
        assert_eq!(
            serde_json::from_str::<AgentMessage>(&json).expect("deserialize concurrency request"),
            acquire
        );

        let cancelled = ServerMessage::ConcurrencyCancelled {
            request_id,
            reason: "superseded".into(),
        };
        let json = serde_json::to_string(&cancelled).expect("serialize concurrency response");
        assert!(json.contains(r#""type":"concurrency_cancelled""#));
        assert_eq!(
            serde_json::from_str::<ServerMessage>(&json).expect("deserialize concurrency response"),
            cancelled
        );

        let token_request = AgentMessage::RepositoryTokenRequest {
            message_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            request_id,
            owner: "acme".into(),
            repository: "shared-actions".into(),
        };
        let json = serde_json::to_string(&token_request).expect("serialize token request");
        assert!(json.contains(r#""type":"repository_token_request""#));
        assert_eq!(
            serde_json::from_str::<AgentMessage>(&json).expect("deserialize token request"),
            token_request
        );

        let granted = ServerMessage::RepositoryTokenGranted {
            request_id,
            token: "target-secret-token".to_owned().into(),
        };
        let json = serde_json::to_string(&granted).expect("serialize token response");
        assert!(json.contains(r#""token":"target-secret-token""#));
        assert!(!format!("{granted:?}").contains("target-secret-token"));
        assert_eq!(
            serde_json::from_str::<ServerMessage>(&json).expect("deserialize token response"),
            granted
        );

        let workflow_request = AgentMessage::WorkflowTokenRequest {
            message_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            request_id,
            read_permissions: vec!["contents".into()],
            write_permissions: vec!["checks".into()],
        };
        let json = serde_json::to_string(&workflow_request).expect("serialize workflow token");
        assert!(json.contains(r#""type":"workflow_token_request""#));
        assert_eq!(
            serde_json::from_str::<AgentMessage>(&json).expect("deserialize workflow token"),
            workflow_request
        );

        let annotated = AgentMessage::JobFinished {
            message_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            conclusion: Conclusion::Failure,
            summary: "lint failed".into(),
            annotations: vec![CheckAnnotation {
                path: "src/lib.rs".into(),
                start_line: 7,
                end_line: 7,
                start_column: Some(2),
                end_column: Some(5),
                annotation_level: CheckAnnotationLevel::Failure,
                message: "invalid syntax".into(),
                title: Some("Compiler".into()),
            }],
        };
        let json = serde_json::to_string(&annotated).expect("serialize annotations");
        assert!(json.contains(r#""annotation_level":"failure""#));
        assert_eq!(
            serde_json::from_str::<AgentMessage>(&json).expect("deserialize annotations"),
            annotated
        );
    }

    #[test]
    fn older_run_specs_default_to_an_empty_variables_context() {
        let mut value = serde_json::to_value(fixture_run()).expect("serialize");
        let object = value.as_object_mut().expect("run object");
        object.remove("variables");
        object.remove("environment_token");
        let decoded: RunSpec = serde_json::from_value(value).expect("deserialize");
        assert!(decoded.variables.is_empty());
        assert!(decoded.environment_token.is_empty());
    }

    fn fixture_run() -> RunSpec {
        RunSpec {
            id: Uuid::nil(),
            workspace_id: "42".into(),
            installation_id: 42,
            run_number: 7,
            repository: RepositorySpec {
                owner: "acme".into(),
                name: "widget".into(),
                clone_url: "https://github.com/acme/widget.git".into(),
            },
            pull_request: PullRequestSpec {
                number: 7,
                action: "opened".into(),
                head_sha: "0123456789012345678901234567890123456789".into(),
                base_sha: "abcdefabcdefabcdefabcdefabcdefabcdefabcd".into(),
                head_ref: "feature".into(),
                base_ref: "main".into(),
            },
            check_run_id: Some(99),
            event: serde_json::json!({
                "action": "opened",
                "sender": {"id": 1, "login": "octocat"}
            }),
            checkout_token: "secret-token".into(),
            environment_token: "environment-secret-token".into(),
            github_api_version: default_github_api_version(),
            changed_paths: None,
            environment: BTreeMap::new(),
            variables: BTreeMap::from([("RUNTIME".into(), "configured-variable-value".into())]),
        }
    }
}
