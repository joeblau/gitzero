use anyhow::{Context, Result, bail};
use gitzero_protocol::{CheckAnnotation, CheckAnnotationLevel};
use regex::{Regex, RegexBuilder};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::OnceLock,
};

const MAX_MATCHER_CONFIG_BYTES: u64 = 256 * 1024;
const MAX_MATCHERS_PER_CONFIG: usize = 32;
const MAX_MATCHERS_PER_JOB: usize = 128;
const MAX_PATTERNS_PER_MATCHER: usize = 10;
const MAX_MATCHER_OWNER_BYTES: usize = 256;
const MAX_MATCHER_REGEX_BYTES: usize = 16 * 1024;
const MAX_MATCHER_MESSAGE_UTF16_UNITS: usize = 4_096;
const MAX_MATCHER_TITLE_BYTES: usize = 255;
const MAX_REGEX_COMPILED_BYTES: usize = 10 * 1024 * 1024;
const MAX_REGEX_DFA_BYTES: usize = 2 * 1024 * 1024;

#[derive(Default)]
pub(crate) struct ProblemMatcherRegistry {
    fallback_workspace: Option<PathBuf>,
    workspaces: BTreeMap<String, PathBuf>,
    matcher_root: Option<PathBuf>,
    scopes: BTreeMap<String, Vec<ProblemMatcher>>,
}

impl ProblemMatcherRegistry {
    pub(crate) fn new(workspace: PathBuf, matcher_root: PathBuf) -> Self {
        Self {
            fallback_workspace: Some(workspace),
            workspaces: BTreeMap::new(),
            matcher_root: Some(matcher_root),
            scopes: BTreeMap::new(),
        }
    }

    pub(crate) fn register_workspace(&mut self, scope: &str, workspace: PathBuf) {
        self.workspaces.insert(scope.to_owned(), workspace);
    }

    pub(crate) fn workspace(&self, scope: &str) -> Option<&Path> {
        self.workspaces
            .get(scope)
            .map(PathBuf::as_path)
            .or(self.fallback_workspace.as_deref())
    }

    pub(crate) fn add_from_file(&mut self, scope: &str, path: &str) -> Result<()> {
        let config = self.read_config(scope, path)?;
        if config.problem_matchers.len() > MAX_MATCHERS_PER_CONFIG {
            bail!("problem matcher file contains too many matchers");
        }
        let mut owners = BTreeSet::new();
        let mut matchers = Vec::with_capacity(config.problem_matchers.len());
        for matcher in config.problem_matchers {
            let compiled = ProblemMatcher::compile(matcher)?;
            let owner = compiled.owner.to_ascii_lowercase();
            if !owners.insert(owner) {
                bail!("problem matcher file contains a duplicate owner");
            }
            matchers.push(compiled);
        }
        if matchers.is_empty() {
            return Ok(());
        }

        let active = self.scopes.entry(scope.to_owned()).or_default();
        let new_owners = matchers
            .iter()
            .map(|matcher| matcher.owner.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        let retained = active
            .drain(..)
            .filter(|matcher| !new_owners.contains(&matcher.owner.to_ascii_lowercase()))
            .collect::<Vec<_>>();
        if matchers.len().saturating_add(retained.len()) > MAX_MATCHERS_PER_JOB {
            bail!("job registered too many problem matchers");
        }
        matchers.extend(retained);
        *active = matchers;
        Ok(())
    }

    pub(crate) fn remove_owner(&mut self, scope: &str, owner: &str) -> Result<()> {
        validate_owner(owner)?;
        if let Some(matchers) = self.scopes.get_mut(scope) {
            matchers.retain(|matcher| !matcher.owner.eq_ignore_ascii_case(owner));
        }
        Ok(())
    }

    pub(crate) fn remove_from_file(&mut self, scope: &str, path: &str) -> Result<()> {
        let config = self.read_config(scope, path)?;
        if config.problem_matchers.len() > MAX_MATCHERS_PER_CONFIG {
            bail!("problem matcher file contains too many matchers");
        }
        let mut owners = BTreeSet::new();
        for matcher in config.problem_matchers {
            validate_owner(&matcher.owner)?;
            owners.insert(matcher.owner.to_ascii_lowercase());
        }
        if let Some(matchers) = self.scopes.get_mut(scope) {
            matchers.retain(|matcher| !owners.contains(&matcher.owner.to_ascii_lowercase()));
        }
        Ok(())
    }

    pub(crate) fn scan(&mut self, scope: &str, line: &str) -> Option<CheckAnnotation> {
        let workspace = self.workspace(scope).map(Path::to_path_buf);
        let matchers = self.scopes.get_mut(scope)?;
        let stripped = strip_ansi_color(line);
        for index in 0..matchers.len() {
            let Some(issue) = matchers[index].matches(&stripped) else {
                continue;
            };
            for (other_index, matcher) in matchers.iter_mut().enumerate() {
                if other_index != index {
                    matcher.reset();
                }
            }
            if let Some(annotation) = issue.into_annotation(workspace.as_deref()) {
                return Some(annotation);
            }
        }
        None
    }

    pub(crate) fn reset_scope(&mut self, scope: &str) {
        if let Some(matchers) = self.scopes.get_mut(scope) {
            for matcher in matchers {
                matcher.reset();
            }
        }
    }

    fn read_config(&self, scope: &str, path: &str) -> Result<ProblemMatcherFile> {
        if path.is_empty() || path.len() > 4_096 || path.contains(['\0', '\r', '\n']) {
            bail!("problem matcher file path is invalid");
        }
        let workspace = self
            .workspace(scope)
            .context("problem matcher workspace is unavailable")?;
        let matcher_root = self
            .matcher_root
            .as_deref()
            .context("problem matcher root is unavailable")?;
        let candidate = if Path::new(path).is_absolute() {
            PathBuf::from(path)
        } else {
            workspace.join(path)
        };
        let matcher_root =
            std::fs::canonicalize(matcher_root).context("resolve isolated problem matcher root")?;
        let candidate =
            std::fs::canonicalize(&candidate).context("resolve problem matcher configuration")?;
        if !candidate.starts_with(&matcher_root) {
            bail!("problem matcher configuration is outside the isolated run directory");
        }
        let metadata = std::fs::metadata(&candidate).context("inspect problem matcher file")?;
        if !metadata.is_file() || metadata.len() > MAX_MATCHER_CONFIG_BYTES {
            bail!("problem matcher configuration is not a bounded regular file");
        }
        let mut source = Vec::with_capacity(metadata.len() as usize);
        File::open(&candidate)
            .context("open problem matcher configuration")?
            .take(MAX_MATCHER_CONFIG_BYTES + 1)
            .read_to_end(&mut source)
            .context("read problem matcher configuration")?;
        if source.len() as u64 > MAX_MATCHER_CONFIG_BYTES {
            bail!("problem matcher configuration exceeds its size limit");
        }
        serde_json::from_slice(&source).context("parse problem matcher configuration")
    }
}

#[derive(Deserialize)]
struct ProblemMatcherFile {
    #[serde(rename = "problemMatcher", default)]
    problem_matchers: Vec<ProblemMatcherConfig>,
}

#[derive(Deserialize)]
struct ProblemMatcherConfig {
    owner: String,
    #[serde(default)]
    severity: String,
    #[serde(rename = "fromPath", default)]
    from_path: String,
    pattern: Vec<ProblemPatternConfig>,
}

#[derive(Deserialize)]
struct ProblemPatternConfig {
    #[serde(default)]
    regexp: String,
    file: Option<usize>,
    line: Option<usize>,
    column: Option<usize>,
    severity: Option<usize>,
    code: Option<usize>,
    message: Option<usize>,
    #[serde(rename = "fromPath")]
    from_path: Option<usize>,
    #[serde(default)]
    r#loop: bool,
}

struct ProblemMatcher {
    owner: String,
    default_severity: String,
    default_from_path: String,
    patterns: Vec<ProblemPattern>,
    state: Vec<Option<CapturedIssue>>,
}

impl ProblemMatcher {
    fn compile(config: ProblemMatcherConfig) -> Result<Self> {
        validate_owner(&config.owner)?;
        validate_severity(&config.severity)?;
        if config.pattern.is_empty() || config.pattern.len() > MAX_PATTERNS_PER_MATCHER {
            bail!("problem matcher must contain a bounded non-empty pattern list");
        }
        if config.from_path.len() > 4_096 || config.from_path.contains('\0') {
            bail!("problem matcher default fromPath is invalid");
        }

        let pattern_count = config.pattern.len();
        let mut assigned = BTreeSet::new();
        let mut patterns = Vec::with_capacity(pattern_count);
        for (index, pattern) in config.pattern.into_iter().enumerate() {
            if pattern.r#loop && (index == 0 || index + 1 != pattern_count) {
                bail!("only the last pattern in a multiline matcher may loop");
            }
            if pattern.r#loop && pattern.message.is_none() {
                bail!("a looping problem matcher pattern must capture a message");
            }
            if pattern.regexp.len() > MAX_MATCHER_REGEX_BYTES || pattern.regexp.contains('\0') {
                bail!("problem matcher regular expression is invalid or too large");
            }
            let regex = RegexBuilder::new(&pattern.regexp)
                .size_limit(MAX_REGEX_COMPILED_BYTES)
                .dfa_size_limit(MAX_REGEX_DFA_BYTES)
                .build()
                .context("compile problem matcher regular expression")?;
            for (name, capture) in [
                ("file", pattern.file),
                ("line", pattern.line),
                ("column", pattern.column),
                ("severity", pattern.severity),
                ("code", pattern.code),
                ("message", pattern.message),
                ("fromPath", pattern.from_path),
            ] {
                if let Some(capture) = capture {
                    if capture >= regex.captures_len() {
                        bail!("problem matcher capture index for {name} is out of range");
                    }
                    if !assigned.insert(name) {
                        bail!("problem matcher assigns {name} more than once");
                    }
                }
            }
            patterns.push(ProblemPattern {
                regex,
                file: pattern.file,
                line: pattern.line,
                column: pattern.column,
                severity: pattern.severity,
                code: pattern.code,
                message: pattern.message,
                from_path: pattern.from_path,
                r#loop: pattern.r#loop,
            });
        }
        if !assigned.contains("message") {
            bail!("problem matcher must capture a message");
        }

        let state_length = patterns.len().saturating_sub(1);
        Ok(Self {
            owner: config.owner,
            default_severity: config.severity,
            default_from_path: config.from_path,
            patterns,
            state: vec![None; state_length],
        })
    }

    fn matches(&mut self, line: &str) -> Option<MatchedIssue> {
        if self.patterns.len() == 1 {
            let pattern = &self.patterns[0];
            let captures = pattern.regex.captures(line)?;
            return Some(CapturedIssue::from_captures(pattern, &captures).finish(
                &self.owner,
                &self.default_severity,
                &self.default_from_path,
            ));
        }

        for index in (0..self.patterns.len()).rev() {
            let running = index
                .checked_sub(1)
                .and_then(|state_index| self.state[state_index].clone());
            if index != 0 && running.is_none() {
                continue;
            }
            let pattern = &self.patterns[index];
            let pattern_loops = pattern.r#loop;
            if let Some(captures) = pattern.regex.captures(line) {
                let captured = CapturedIssue::from_captures(pattern, &captures);
                if index + 1 == self.patterns.len() {
                    let issue = running.clone().unwrap_or_default().merge(captured).finish(
                        &self.owner,
                        &self.default_severity,
                        &self.default_from_path,
                    );
                    self.reset();
                    if pattern_loops {
                        self.state[index - 1] = running;
                    }
                    return Some(issue);
                }
                self.state[index] = Some(running.unwrap_or_default().merge(captured));
            } else if index + 1 == self.patterns.len() {
                self.state[index - 1] = None;
            } else {
                self.state[index] = None;
            }
        }
        None
    }

    fn reset(&mut self) {
        self.state.fill(None);
    }
}

struct ProblemPattern {
    regex: Regex,
    file: Option<usize>,
    line: Option<usize>,
    column: Option<usize>,
    severity: Option<usize>,
    code: Option<usize>,
    message: Option<usize>,
    from_path: Option<usize>,
    r#loop: bool,
}

#[derive(Clone, Default)]
struct CapturedIssue {
    file: Option<String>,
    line: Option<String>,
    column: Option<String>,
    severity: Option<String>,
    code: Option<String>,
    message: Option<String>,
    from_path: Option<String>,
}

impl CapturedIssue {
    fn from_captures(pattern: &ProblemPattern, captures: &regex::Captures<'_>) -> Self {
        Self {
            file: capture(captures, pattern.file),
            line: capture(captures, pattern.line),
            column: capture(captures, pattern.column),
            severity: capture(captures, pattern.severity),
            code: capture(captures, pattern.code),
            message: capture(captures, pattern.message),
            from_path: capture(captures, pattern.from_path),
        }
    }

    fn merge(self, newer: Self) -> Self {
        Self {
            file: self.file.or(newer.file),
            line: self.line.or(newer.line),
            column: self.column.or(newer.column),
            severity: self.severity.or(newer.severity),
            code: self.code.or(newer.code),
            message: self.message.or(newer.message),
            from_path: self.from_path.or(newer.from_path),
        }
    }

    fn finish(
        mut self,
        owner: &str,
        default_severity: &str,
        default_from_path: &str,
    ) -> MatchedIssue {
        if self
            .severity
            .as_ref()
            .is_none_or(|severity| severity.is_empty())
            && !default_severity.is_empty()
        {
            self.severity = Some(default_severity.to_owned());
        }
        if self
            .from_path
            .as_ref()
            .is_none_or(|from_path| from_path.is_empty())
            && !default_from_path.is_empty()
        {
            self.from_path = Some(default_from_path.to_owned());
        }
        MatchedIssue {
            owner: owner.to_owned(),
            captured: self,
        }
    }
}

struct MatchedIssue {
    owner: String,
    captured: CapturedIssue,
}

impl MatchedIssue {
    fn into_annotation(self, workspace: Option<&Path>) -> Option<CheckAnnotation> {
        let annotation_level = match self
            .captured
            .severity
            .as_deref()
            .unwrap_or("error")
            .to_ascii_lowercase()
            .as_str()
        {
            "error" => CheckAnnotationLevel::Failure,
            "warning" => CheckAnnotationLevel::Warning,
            "notice" => CheckAnnotationLevel::Notice,
            _ => return None,
        };
        let mut message = self.captured.message?;
        if message.trim().is_empty() {
            return None;
        }
        truncate_utf16(&mut message, MAX_MATCHER_MESSAGE_UTF16_UNITS);
        let title = self
            .captured
            .code
            .map(|code| code.trim().to_owned())
            .filter(|code| !code.is_empty())
            .or(Some(self.owner))
            .map(|mut title| {
                truncate_utf8(&mut title, MAX_MATCHER_TITLE_BYTES);
                title
            });
        let start_line = coordinate(self.captured.line.as_deref()).unwrap_or(1);
        let start_column = coordinate(self.captured.column.as_deref());
        Some(CheckAnnotation {
            path: matcher_path(
                self.captured.file.as_deref(),
                self.captured.from_path.as_deref(),
                workspace,
            )
            .unwrap_or_else(|| ".github".to_owned()),
            start_line,
            end_line: start_line,
            start_column,
            end_column: start_column,
            annotation_level,
            message,
            title,
        })
    }
}

fn capture(captures: &regex::Captures<'_>, index: Option<usize>) -> Option<String> {
    index.map(|index| {
        captures
            .get(index)
            .map_or("", |capture| capture.as_str())
            .to_owned()
    })
}

fn validate_owner(owner: &str) -> Result<()> {
    if owner.is_empty()
        || owner.len() > MAX_MATCHER_OWNER_BYTES
        || owner.contains(['\0', '\r', '\n'])
    {
        bail!("problem matcher owner is invalid");
    }
    Ok(())
}

fn validate_severity(severity: &str) -> Result<()> {
    if matches!(
        severity.to_ascii_lowercase().as_str(),
        "" | "error" | "warning" | "notice"
    ) {
        Ok(())
    } else {
        bail!("problem matcher default severity is unsupported");
    }
}

fn coordinate(value: Option<&str>) -> Option<u32> {
    let value = value?.parse::<u32>().ok()?;
    (value > 0 && value <= i32::MAX as u32).then_some(value)
}

fn matcher_path(
    file: Option<&str>,
    from_path: Option<&str>,
    workspace: Option<&Path>,
) -> Option<String> {
    let workspace = workspace?;
    let file = file?.trim();
    if file.is_empty() || file.contains(['\0', '\r', '\n']) {
        return None;
    }
    let mut candidate = PathBuf::from(file.replace('\\', "/"));
    if !candidate.is_absolute() {
        if let Some(from_path) = from_path.filter(|from_path| !from_path.trim().is_empty()) {
            let from_path = PathBuf::from(from_path.replace('\\', "/"));
            if let Some(parent) = from_path.parent()
                && !parent.as_os_str().is_empty()
            {
                candidate = parent.join(candidate);
            }
        }
        candidate = workspace.join(candidate);
    }
    let workspace = std::fs::canonicalize(workspace).ok()?;
    let candidate = std::fs::canonicalize(candidate).ok()?;
    let relative = candidate.strip_prefix(workspace).ok()?;
    if !candidate.is_file() {
        return None;
    }
    let path = relative.to_str()?.replace('\\', "/");
    (!path.is_empty() && path.len() <= 4_096).then_some(path)
}

fn strip_ansi_color(line: &str) -> String {
    static ANSI_COLOR: OnceLock<Regex> = OnceLock::new();
    ANSI_COLOR
        .get_or_init(|| Regex::new(r"\x1b\[[0-9;]*m?").expect("ANSI color regex is valid"))
        .replace_all(line, "")
        .into_owned()
}

fn truncate_utf16(value: &mut String, maximum_units: usize) {
    if value.encode_utf16().count() <= maximum_units {
        return;
    }
    let mut units = 0;
    let mut boundary = 0;
    for (index, character) in value.char_indices() {
        let next = units + character.len_utf16();
        if next > maximum_units {
            break;
        }
        units = next;
        boundary = index + character.len_utf8();
    }
    value.truncate(boundary);
}

fn truncate_utf8(value: &mut String, maximum_bytes: usize) {
    if value.len() <= maximum_bytes {
        return;
    }
    let mut boundary = maximum_bytes;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.truncate(boundary);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_single_and_looping_multiline_diagnostics() {
        let directory = tempfile::tempdir().expect("temporary matcher root");
        let discovery = directory.path().join("discovery");
        let workspace = directory.path().join("jobs/workflow-job");
        std::fs::create_dir_all(&discovery).expect("discovery workspace");
        std::fs::create_dir_all(workspace.join("src")).expect("workspace");
        std::fs::write(workspace.join("src/main.ts"), "export {};\n").expect("source");
        std::fs::write(workspace.join("lint.js"), "test\n").expect("lint source");
        std::fs::write(
            workspace.join("matcher.json"),
            r#"{
              "problemMatcher": [
                {
                  "owner": "tsc",
                  "pattern": [{
                    "regexp": "^([^\\s].*)[\\(:](\\d+)[,:](\\d+)(?:\\):\\s+|\\s+-\\s+)(error|warning|info)\\s+TS(\\d+)\\s*:\\s*(.*)$",
                    "file": 1, "line": 2, "column": 3, "severity": 4,
                    "code": 5, "message": 6
                  }]
                },
                {
                  "owner": "eslint",
                  "pattern": [
                    {"regexp": "^([^\\s].*)$", "file": 1},
                    {
                      "regexp": "^\\s+(\\d+):(\\d+)\\s+(error|warning)\\s+(.*?)\\s{2,}(.*)$",
                      "line": 1, "column": 2, "severity": 3,
                      "message": 4, "code": 5, "loop": true
                    }
                  ]
                }
              ]
            }"#,
        )
        .expect("matcher config");

        let mut registry = ProblemMatcherRegistry::new(discovery, directory.path().into());
        registry.register_workspace("workflow/job", workspace.clone());
        registry
            .add_from_file("workflow/job", "matcher.json")
            .expect("register matchers");
        let single = registry
            .scan(
                "workflow/job",
                "\u{1b}[31msrc/main.ts(7,3): warning TS100: check this\u{1b}[0m",
            )
            .expect("single-line issue");
        assert_eq!(single.path, "src/main.ts");
        assert_eq!(single.start_line, 7);
        assert_eq!(single.start_column, Some(3));
        assert_eq!(single.annotation_level, CheckAnnotationLevel::Warning);
        assert_eq!(single.title.as_deref(), Some("100"));

        assert!(registry.scan("workflow/job", "lint.js").is_none());
        let first = registry
            .scan("workflow/job", "  1:2  error  missing semicolon  semi")
            .expect("first loop issue");
        let second = registry
            .scan(
                "workflow/job",
                "  4:5  warning  unused value  no-unused-vars",
            )
            .expect("second loop issue");
        assert_eq!(first.path, "lint.js");
        assert_eq!(first.message, "missing semicolon");
        assert_eq!(second.start_line, 4);
        assert_eq!(second.annotation_level, CheckAnnotationLevel::Warning);
    }

    #[test]
    fn registration_is_scoped_replaceable_removable_and_contained() {
        let directory = tempfile::tempdir().expect("temporary matcher root");
        let workspace = directory.path().join("repository");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let matcher = |message: usize| {
            format!(
                r#"{{"problemMatcher":[{{"owner":"lint","severity":"warning","pattern":[{{"regexp":"^(.*)$","message":{message}}}]}}]}}"#
            )
        };
        std::fs::write(workspace.join("one.json"), matcher(1)).expect("first matcher");
        std::fs::write(workspace.join("two.json"), matcher(1)).expect("second matcher");
        let outside = tempfile::NamedTempFile::new().expect("outside matcher");
        std::fs::write(outside.path(), matcher(1)).expect("outside config");

        let mut registry = ProblemMatcherRegistry::new(workspace.clone(), directory.path().into());
        registry
            .add_from_file("one/job", "one.json")
            .expect("add first");
        registry
            .add_from_file(
                "one/job",
                workspace.join("two.json").to_str().expect("UTF-8 path"),
            )
            .expect("replace owner");
        assert!(registry.scan("two/job", "not scoped").is_none());
        assert!(registry.scan("one/job", "matched").is_some());
        registry
            .remove_owner("one/job", "LINT")
            .expect("remove case-insensitively");
        assert!(registry.scan("one/job", "removed").is_none());
        registry
            .add_from_file("one/job", "one.json")
            .expect("restore matcher");
        registry
            .remove_from_file("one/job", "one.json")
            .expect("remove by config file");
        assert!(registry.scan("one/job", "file removed").is_none());
        assert!(
            registry
                .add_from_file("one/job", outside.path().to_str().expect("UTF-8 path"))
                .is_err()
        );
    }
}
