use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_yaml_ng::Value;
use std::{collections::BTreeMap, path::Path};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActionReference {
    Local {
        path: String,
    },
    SelfRepository {
        path: String,
    },
    Remote {
        owner: String,
        repository: String,
        path: String,
        git_ref: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteReusableWorkflowReference {
    pub owner: String,
    pub repository: String,
    pub path: String,
    pub git_ref: String,
}

impl RemoteReusableWorkflowReference {
    pub fn parse(source: &str) -> Result<Self> {
        let ActionReference::Remote {
            owner,
            repository,
            path,
            git_ref,
        } = ActionReference::parse(source)?
        else {
            bail!("reusable workflow reference '{source}' must include owner/repository and @ref");
        };
        if git_ref.starts_with("refs/") {
            bail!("reusable workflow reference '{source}' must not use a refs/ prefix");
        }
        let path = Path::new(&path);
        if path.parent() != Some(Path::new(".github/workflows"))
            || !path
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| matches!(extension, "yml" | "yaml"))
        {
            bail!(
                "reusable workflow reference '{source}' must name a file directly in .github/workflows"
            );
        }
        Ok(Self {
            owner,
            repository,
            path: path.to_string_lossy().replace('\\', "/"),
            git_ref,
        })
    }
}

impl ActionReference {
    pub fn parse(source: &str) -> Result<Self> {
        if let Some(path) = source.strip_prefix("./") {
            validate_relative_path(path)?;
            return Ok(Self::Local {
                path: path.to_owned(),
            });
        }
        if let Some(path) = source.strip_prefix("$/") {
            let path = path.trim_start_matches('/');
            validate_relative_path(path)?;
            return Ok(Self::SelfRepository {
                path: path.to_owned(),
            });
        }
        if source.starts_with("docker://") {
            bail!("Docker actions are not supported on the native macOS executor");
        }
        let (location, git_ref) = source
            .rsplit_once('@')
            .with_context(|| format!("action reference '{source}' must include @ref"))?;
        if git_ref.is_empty() || git_ref.starts_with('-') || git_ref.contains(['\0', '\n', '\r']) {
            bail!("action reference '{source}' has an invalid git ref");
        }
        let mut components = location.split('/');
        let owner = components.next().unwrap_or_default();
        let repository = components.next().unwrap_or_default();
        validate_repository_component(owner, "owner")?;
        validate_repository_component(repository, "repository")?;
        let path = components.collect::<Vec<_>>().join("/");
        if !path.is_empty() {
            validate_relative_path(&path)?;
        }
        Ok(Self::Remote {
            owner: owner.to_owned(),
            repository: repository.to_owned(),
            path,
            git_ref: git_ref.to_owned(),
        })
    }
}

fn validate_repository_component(value: &str, name: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("action {name} '{value}' is invalid");
    }
    Ok(())
}

fn validate_relative_path(value: &str) -> Result<()> {
    let path = Path::new(value);
    if value.is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir | std::path::Component::RootDir
            )
        })
    {
        bail!("action path '{value}' must stay within its repository");
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize)]
pub struct ActionDefinition {
    pub name: String,
    #[serde(default)]
    pub inputs: BTreeMap<String, ActionInput>,
    #[serde(default)]
    pub outputs: BTreeMap<String, ActionOutput>,
    pub runs: ActionRuns,
    #[serde(rename = "description", default)]
    pub _description: Option<String>,
    #[serde(rename = "author", default)]
    pub _author: Option<String>,
    #[serde(rename = "branding", default)]
    pub _branding: Option<Value>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ActionInput {
    #[serde(rename = "description", default)]
    pub _description: Option<String>,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub default: Option<ActionScalar>,
    #[serde(rename = "deprecationMessage", default)]
    pub _deprecation_message: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ActionOutput {
    #[serde(rename = "description", default)]
    pub _description: Option<String>,
    #[serde(default)]
    pub value: Option<ActionScalar>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ActionRuns {
    pub using: String,
    #[serde(default)]
    pub main: Option<String>,
    #[serde(default)]
    pub pre: Option<String>,
    #[serde(default)]
    pub post: Option<String>,
    #[serde(rename = "pre-if", default)]
    pub pre_if: Option<ActionScalar>,
    #[serde(rename = "post-if", default)]
    pub post_if: Option<ActionScalar>,
    #[serde(default)]
    pub steps: Vec<ActionStep>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ActionStep {
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
    pub env: BTreeMap<String, ActionScalar>,
    #[serde(default)]
    pub with: BTreeMap<String, ActionScalar>,
    #[serde(rename = "if", default)]
    pub condition: Option<ActionScalar>,
    #[serde(rename = "continue-on-error", default)]
    pub continue_on_error: Option<ActionScalar>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionScalar(String);

impl ActionScalar {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ActionScalar {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let string = match value {
            Value::String(value) => value,
            Value::Bool(value) => value.to_string(),
            Value::Number(value) => value.to_string(),
            Value::Null => String::new(),
            _ => return Err(serde::de::Error::custom("expected an action scalar value")),
        };
        Ok(Self(string))
    }
}

pub async fn load_definition(directory: &Path) -> Result<ActionDefinition> {
    for name in ["action.yml", "action.yaml"] {
        let path = directory.join(name);
        match tokio::fs::read_to_string(&path).await {
            Ok(source) => {
                return serde_yaml_ng::from_str(&source)
                    .with_context(|| format!("parse action metadata {}", path.display()));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("read action metadata {}", path.display()));
            }
        }
    }
    bail!(
        "action directory {} has no action.yml or action.yaml",
        directory.display()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_remote_and_local_references() {
        assert_eq!(
            ActionReference::parse("owner/repo/path/to/action@v2").expect("remote"),
            ActionReference::Remote {
                owner: "owner".to_owned(),
                repository: "repo".to_owned(),
                path: "path/to/action".to_owned(),
                git_ref: "v2".to_owned(),
            }
        );
        assert_eq!(
            ActionReference::parse("./.github/actions/test").expect("local"),
            ActionReference::Local {
                path: ".github/actions/test".to_owned(),
            }
        );
        assert_eq!(
            ActionReference::parse("$//.github/actions/test").expect("self repository"),
            ActionReference::SelfRepository {
                path: ".github/actions/test".to_owned(),
            }
        );
        assert!(ActionReference::parse("$/").is_err());
        assert!(ActionReference::parse("$/../escape").is_err());
        assert!(ActionReference::parse("../escape").is_err());
    }

    #[test]
    fn parses_remote_reusable_workflow_references() {
        assert_eq!(
            RemoteReusableWorkflowReference::parse("owner/repo/.github/workflows/reusable.yml@v2")
                .expect("remote reusable workflow"),
            RemoteReusableWorkflowReference {
                owner: "owner".to_owned(),
                repository: "repo".to_owned(),
                path: ".github/workflows/reusable.yml".to_owned(),
                git_ref: "v2".to_owned(),
            }
        );
        assert!(
            RemoteReusableWorkflowReference::parse(
                "owner/repo/.github/workflows/nested/reusable.yml@v2"
            )
            .is_err()
        );
        assert!(
            RemoteReusableWorkflowReference::parse("./.github/workflows/reusable.yml").is_err()
        );
        assert!(
            RemoteReusableWorkflowReference::parse(
                "owner/repo/.github/workflows/reusable.yml@refs/heads/main"
            )
            .is_err()
        );
    }

    #[test]
    fn parses_javascript_and_composite_metadata() {
        let javascript: ActionDefinition = serde_yaml_ng::from_str(
            "name: JS\ninputs:\n  count:\n    required: true\n    default: 2\nruns:\n  using: node20\n  main: dist/index.js\n",
        )
        .expect("javascript");
        assert_eq!(
            javascript.inputs["count"]
                .default
                .as_ref()
                .unwrap()
                .as_str(),
            "2"
        );
        assert_eq!(javascript.runs.main.as_deref(), Some("dist/index.js"));

        let composite: ActionDefinition = serde_yaml_ng::from_str(
            "name: Composite\nruns:\n  using: composite\n  steps:\n    - run: echo ok\n      shell: bash\n      continue-on-error: ${{ fromJSON(inputs.tolerate) }}\n",
        )
        .expect("composite");
        assert_eq!(composite.runs.steps.len(), 1);
        assert_eq!(
            composite.runs.steps[0]
                .continue_on_error
                .as_ref()
                .expect("continue-on-error")
                .as_str(),
            "${{ fromJSON(inputs.tolerate) }}"
        );
    }
}
