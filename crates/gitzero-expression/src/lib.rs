use github_actions_expressions::{
    Evaluation, Expr, SpannedExpr,
    call::{Call, Function},
    context::Context,
    literal::Literal,
    op::{BinExpr, BinOp, UnOp},
};
use globset::{GlobBuilder, GlobMatcher};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeSet, HashMap},
    fmt::Write as _,
    fs::File,
    io::{self, BufReader, Read},
    path::{Component, Path, PathBuf},
    time::{Duration, Instant},
};
use thiserror::Error;
use walkdir::WalkDir;

const HASH_FILES_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExecutionStatus {
    #[default]
    Success,
    Failure,
    Cancelled,
    Skipped,
}

#[derive(Debug, Error)]
pub enum ExpressionError {
    #[error("invalid GitHub Actions expression: {0}")]
    Parse(#[from] github_actions_expressions::Error),
    #[error("unknown expression context '{0}'")]
    UnknownContext(String),
    #[error("cannot access '{index}' on {value_type}")]
    InvalidAccess {
        index: String,
        value_type: &'static str,
    },
    #[error("function {0} is not supported in this execution context")]
    UnsupportedFunction(&'static str),
    #[error("invalid arguments for expression function {0}")]
    InvalidFunction(&'static str),
    #[error("invalid hashFiles option '{0}'")]
    InvalidHashFilesOption(String),
    #[error("invalid hashFiles pattern '{pattern}': {reason}")]
    InvalidHashFilesPattern { pattern: String, reason: String },
    #[error("hashFiles workspace '{}' is unavailable", .path.display())]
    HashFilesWorkspace {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("hashFiles could not read '{}'", .path.display())]
    HashFilesIo {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("hashFiles encountered a non-UTF-8 path: '{}'", .0.display())]
    HashFilesNonUtf8Path(PathBuf),
    #[error("hashFiles could not finish within 120 seconds")]
    HashFilesTimeout,
    #[error("expression template is missing its closing braces")]
    UnclosedTemplate,
    #[error("expression value cannot be represented as JSON")]
    JsonConversion,
}

/// Context and runner-dependent features referenced by an expression or template.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExpressionAnalysis {
    /// Case-normalized root contexts referenced anywhere in the expression.
    pub context_roots: BTreeSet<String>,
    /// Whether the expression calls a job-status function.
    pub uses_status_function: bool,
    /// Whether the expression calls the workspace-dependent `hashFiles` function.
    pub uses_hash_files: bool,
}

impl ExpressionAnalysis {
    fn merge(&mut self, other: Self) {
        self.context_roots.extend(other.context_roots);
        self.uses_status_function |= other.uses_status_function;
        self.uses_hash_files |= other.uses_hash_files;
    }
}

/// Parse one expression and report every context and runner-dependent function it uses.
pub fn analyze_expression(source: &str) -> Result<ExpressionAnalysis, ExpressionError> {
    let source = unwrap_expression(source);
    let expression = Expr::parse(source)?;
    let mut analysis = ExpressionAnalysis::default();
    analyze_spanned_expression(&expression, &mut analysis);
    Ok(analysis)
}

/// Analyze every `${{ ... }}` expression embedded in a template string.
pub fn analyze_template(template: &str) -> Result<ExpressionAnalysis, ExpressionError> {
    let mut analysis = ExpressionAnalysis::default();
    let mut cursor = 0;
    while let Some(relative_start) = template[cursor..].find("${{") {
        let start = cursor + relative_start;
        let expression_start = start + 3;
        let expression_end = find_expression_end(template, expression_start)
            .ok_or(ExpressionError::UnclosedTemplate)?;
        analysis.merge(analyze_expression(
            &template[expression_start..expression_end],
        )?);
        cursor = expression_end + 2;
    }
    Ok(analysis)
}

fn analyze_spanned_expression(expression: &SpannedExpr<'_>, analysis: &mut ExpressionAnalysis) {
    match &expression.inner {
        Expr::Literal(_) | Expr::Star => {}
        Expr::Identifier(identifier) => {
            analysis
                .context_roots
                .insert(identifier.as_str().to_ascii_lowercase());
        }
        Expr::Index(index) => analyze_spanned_expression(index, analysis),
        Expr::Call(Call { func, args }) => {
            analysis.uses_status_function |= matches!(
                func,
                Function::Success | Function::Always | Function::Cancelled | Function::Failure
            );
            analysis.uses_hash_files |= matches!(func, Function::HashFiles);
            for argument in args {
                analyze_spanned_expression(argument, analysis);
            }
        }
        Expr::Context(context) => {
            if let Some(head) = context.parts.first() {
                match &head.inner {
                    Expr::Identifier(identifier) => {
                        analysis
                            .context_roots
                            .insert(identifier.as_str().to_ascii_lowercase());
                    }
                    _ => analyze_spanned_expression(head, analysis),
                }
            }
            for part in context.parts.iter().skip(1) {
                match &part.inner {
                    Expr::Identifier(_) | Expr::Star | Expr::Literal(_) => {}
                    Expr::Index(index) => analyze_spanned_expression(index, analysis),
                    _ => analyze_spanned_expression(part, analysis),
                }
            }
        }
        Expr::BinExpr(binary) => {
            analyze_spanned_expression(&binary.lhs, analysis);
            analyze_spanned_expression(&binary.rhs, analysis);
        }
        Expr::UnExpr { expr, .. } => analyze_spanned_expression(expr, analysis),
    }
}

#[derive(Clone, Debug, Default)]
pub struct EvaluationContext {
    roots: HashMap<String, Evaluation>,
    status: ExecutionStatus,
    workspace: Option<PathBuf>,
}

impl EvaluationContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_status(mut self, status: ExecutionStatus) -> Self {
        self.status = status;
        self
    }

    pub fn with_workspace(mut self, workspace: impl Into<PathBuf>) -> Self {
        self.workspace = Some(workspace.into());
        self
    }

    pub fn set_workspace(&mut self, workspace: impl Into<PathBuf>) {
        self.workspace = Some(workspace.into());
    }

    pub fn set_status(&mut self, status: ExecutionStatus) {
        self.status = status;
    }

    pub fn insert_json(
        &mut self,
        name: impl Into<String>,
        value: Value,
    ) -> Result<(), ExpressionError> {
        let value = Evaluation::try_from(value).map_err(|()| ExpressionError::JsonConversion)?;
        self.roots.insert(name.into().to_ascii_lowercase(), value);
        Ok(())
    }

    pub fn insert_string(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.roots.insert(
            name.into().to_ascii_lowercase(),
            Evaluation::String(value.into()),
        );
    }

    pub fn extend_json_object(
        &mut self,
        name: &str,
        entries: serde_json::Map<String, Value>,
    ) -> Result<(), ExpressionError> {
        let key = name.to_ascii_lowercase();
        let mut current = match self.roots.remove(&key) {
            Some(value) => value
                .try_into()
                .map_err(|()| ExpressionError::JsonConversion)?,
            None => Value::Object(Default::default()),
        };
        let Value::Object(object) = &mut current else {
            return Err(ExpressionError::InvalidAccess {
                index: name.to_owned(),
                value_type: json_type(&current),
            });
        };
        object.extend(entries);
        self.insert_json(key, current)
    }

    pub fn evaluate(&self, source: &str) -> Result<Evaluation, ExpressionError> {
        let source = unwrap_expression(source);
        let expression = Expr::parse(source)?;
        self.evaluate_spanned(&expression)
    }

    pub fn evaluate_json(&self, source: &str) -> Result<Value, ExpressionError> {
        self.evaluate(source)?
            .try_into()
            .map_err(|()| ExpressionError::JsonConversion)
    }

    pub fn evaluate_condition(&self, source: &str) -> Result<bool, ExpressionError> {
        Ok(self.evaluate(source)?.as_boolean())
    }

    pub fn render(&self, template: &str) -> Result<String, ExpressionError> {
        let mut rendered = String::with_capacity(template.len());
        let mut cursor = 0;
        while let Some(relative_start) = template[cursor..].find("${{") {
            let start = cursor + relative_start;
            rendered.push_str(&template[cursor..start]);
            let expression_start = start + 3;
            let expression_end = find_expression_end(template, expression_start)
                .ok_or(ExpressionError::UnclosedTemplate)?;
            let value = self.evaluate(&template[expression_start..expression_end])?;
            rendered.push_str(&value.sema().to_string());
            cursor = expression_end + 2;
        }
        rendered.push_str(&template[cursor..]);
        Ok(rendered)
    }

    fn evaluate_spanned(
        &self,
        expression: &SpannedExpr<'_>,
    ) -> Result<Evaluation, ExpressionError> {
        self.evaluate_expr(&expression.inner)
    }

    fn evaluate_expr(&self, expression: &Expr<'_>) -> Result<Evaluation, ExpressionError> {
        match expression {
            Expr::Literal(literal) => Ok(evaluate_literal(literal)),
            Expr::Context(context) => self.evaluate_context(context),
            Expr::Call(call) => self.evaluate_call(call),
            Expr::BinExpr(binary) => self.evaluate_binary(binary),
            Expr::UnExpr { op, expr } => match op {
                UnOp::Not => Ok(Evaluation::Boolean(
                    !self.evaluate_spanned(expr)?.as_boolean(),
                )),
            },
            Expr::Identifier(identifier) => self
                .roots
                .get(&identifier.as_str().to_ascii_lowercase())
                .cloned()
                .ok_or_else(|| ExpressionError::UnknownContext(identifier.as_str().to_owned())),
            Expr::Index(index) => self.evaluate_spanned(index),
            Expr::Star => Err(ExpressionError::InvalidAccess {
                index: "*".to_owned(),
                value_type: "a root expression",
            }),
        }
    }

    fn evaluate_binary(&self, binary: &BinExpr<'_>) -> Result<Evaluation, ExpressionError> {
        let left = self.evaluate_spanned(&binary.lhs)?;
        match binary.op {
            BinOp::And => {
                if left.as_boolean() {
                    self.evaluate_spanned(&binary.rhs)
                } else {
                    Ok(left)
                }
            }
            BinOp::Or => {
                if left.as_boolean() {
                    Ok(left)
                } else {
                    self.evaluate_spanned(&binary.rhs)
                }
            }
            _ => {
                let right = self.evaluate_spanned(&binary.rhs)?;
                let result = match binary.op {
                    BinOp::Eq => left.sema() == right.sema(),
                    BinOp::Neq => left.sema() != right.sema(),
                    BinOp::Gt => left.sema() > right.sema(),
                    BinOp::Ge => left.sema() >= right.sema(),
                    BinOp::Lt => left.sema() < right.sema(),
                    BinOp::Le => left.sema() <= right.sema(),
                    BinOp::And | BinOp::Or => unreachable!("handled above"),
                };
                Ok(Evaluation::Boolean(result))
            }
        }
    }

    fn evaluate_context(&self, context: &Context<'_>) -> Result<Evaluation, ExpressionError> {
        let mut parts = context.parts.iter();
        let Some(head) = parts.next() else {
            return Err(ExpressionError::UnknownContext(String::new()));
        };
        let first = match &head.inner {
            Expr::Identifier(identifier) => self
                .roots
                .get(&identifier.as_str().to_ascii_lowercase())
                .cloned()
                .ok_or_else(|| ExpressionError::UnknownContext(identifier.as_str().to_owned()))?,
            other => self.evaluate_expr(other)?,
        };
        let mut selection = Selection::One(first);
        for part in parts {
            selection = match &part.inner {
                Expr::Identifier(identifier) => {
                    selection.access(Evaluation::String(identifier.as_str().to_owned()))?
                }
                Expr::Index(index) => selection.access(self.evaluate_spanned(index)?)?,
                Expr::Star => selection.wildcard()?,
                other => selection.access(self.evaluate_expr(other)?)?,
            };
        }
        Ok(selection.finish())
    }

    fn evaluate_call(&self, call: &Call<'_>) -> Result<Evaluation, ExpressionError> {
        match call.func {
            Function::Success => {
                return Ok(Evaluation::Boolean(self.status == ExecutionStatus::Success));
            }
            Function::Always => return Ok(Evaluation::Boolean(true)),
            Function::Cancelled => {
                return Ok(Evaluation::Boolean(
                    self.status == ExecutionStatus::Cancelled,
                ));
            }
            Function::Failure => {
                return Ok(Evaluation::Boolean(self.status == ExecutionStatus::Failure));
            }
            Function::HashFiles => {}
            _ => {}
        }

        let arguments = call
            .args
            .iter()
            .map(|argument| self.evaluate_spanned(argument))
            .collect::<Result<Vec<_>, _>>()?;
        match call.func {
            Function::Contains => contains(&arguments),
            Function::StartsWith => starts_or_ends_with(&arguments, true),
            Function::EndsWith => starts_or_ends_with(&arguments, false),
            Function::Format => format_values(&arguments),
            Function::Join => join(&arguments),
            Function::ToJSON => to_json(&arguments),
            Function::FromJSON => from_json(&arguments),
            Function::Case => case(&arguments),
            Function::HashFiles => self.hash_files(&arguments),
            Function::Success | Function::Always | Function::Cancelled | Function::Failure => {
                unreachable!("handled above")
            }
        }
    }

    fn hash_files(&self, arguments: &[Evaluation]) -> Result<Evaluation, ExpressionError> {
        let workspace = self
            .workspace
            .as_deref()
            .ok_or(ExpressionError::UnsupportedFunction("hashFiles"))?;
        let arguments = arguments
            .iter()
            .map(|argument| argument.sema().to_string())
            .collect::<Vec<_>>();
        hash_files(workspace, &arguments).map(Evaluation::String)
    }
}

struct HashPattern {
    exclude: bool,
    matcher: GlobMatcher,
    descendant_matcher: Option<GlobMatcher>,
}

impl HashPattern {
    fn matches(&self, relative_path: &str) -> bool {
        self.matcher.is_match(relative_path)
            || self
                .descendant_matcher
                .as_ref()
                .is_some_and(|matcher| matcher.is_match(relative_path))
    }
}

fn hash_files(workspace: &Path, arguments: &[String]) -> Result<String, ExpressionError> {
    let mut follow_symbolic_links = false;
    let mut pattern_arguments = arguments;
    if let Some(first) = pattern_arguments.first()
        && first.starts_with("--")
    {
        if first.eq_ignore_ascii_case("--follow-symbolic-links") {
            follow_symbolic_links = true;
            pattern_arguments = &pattern_arguments[1..];
        } else {
            return Err(ExpressionError::InvalidHashFilesOption(first.clone()));
        }
    }

    let workspace =
        workspace
            .canonicalize()
            .map_err(|source| ExpressionError::HashFilesWorkspace {
                path: workspace.to_owned(),
                source,
            })?;
    let patterns = compile_hash_patterns(&workspace, pattern_arguments)?;
    if !patterns.iter().any(|pattern| !pattern.exclude) {
        return Ok(String::new());
    }

    let started = Instant::now();
    let mut matched_paths = Vec::new();
    let walker = WalkDir::new(&workspace)
        .follow_links(follow_symbolic_links)
        .sort_by_file_name()
        .into_iter()
        .filter_entry(|entry| {
            !entry.path_is_symlink()
                || entry
                    .path()
                    .canonicalize()
                    .is_ok_and(|target| target.starts_with(&workspace))
        });
    for entry in walker {
        if started.elapsed() >= HASH_FILES_TIMEOUT {
            return Err(ExpressionError::HashFilesTimeout);
        }
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) if error.loop_ancestor().is_some() => continue,
            Err(error) => {
                let path = error
                    .path()
                    .map(Path::to_owned)
                    .unwrap_or_else(|| workspace.clone());
                let source = error
                    .into_io_error()
                    .unwrap_or_else(|| io::Error::other("directory traversal failed"));
                return Err(ExpressionError::HashFilesIo { path, source });
            }
        };
        if entry.depth() == 0 {
            continue;
        }
        let path = entry.path();
        let relative = path
            .strip_prefix(&workspace)
            .expect("walked path is rooted in workspace");
        let relative = slash_path(relative)?;
        let included = patterns.iter().fold(false, |included, pattern| {
            if pattern.matches(&relative) {
                !pattern.exclude
            } else {
                included
            }
        });
        if !included {
            continue;
        }

        let resolved = match path.canonicalize() {
            Ok(resolved) => resolved,
            Err(source) if source.kind() == io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(ExpressionError::HashFilesIo {
                    path: path.to_owned(),
                    source,
                });
            }
        };
        if !resolved.starts_with(&workspace) {
            continue;
        }
        let metadata = resolved
            .metadata()
            .map_err(|source| ExpressionError::HashFilesIo {
                path: path.to_owned(),
                source,
            })?;
        if metadata.is_file() {
            matched_paths.push((path.to_owned(), resolved));
        }
    }

    matched_paths.sort_by(|left, right| left.0.cmp(&right.0));
    if matched_paths.is_empty() {
        return Ok(String::new());
    }

    let mut aggregate = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1_024];
    for (display_path, resolved_path) in matched_paths {
        if started.elapsed() >= HASH_FILES_TIMEOUT {
            return Err(ExpressionError::HashFilesTimeout);
        }
        let file = File::open(&resolved_path).map_err(|source| ExpressionError::HashFilesIo {
            path: display_path.clone(),
            source,
        })?;
        let mut reader = BufReader::new(file);
        let mut file_hash = Sha256::new();
        loop {
            let count =
                reader
                    .read(&mut buffer)
                    .map_err(|source| ExpressionError::HashFilesIo {
                        path: display_path.clone(),
                        source,
                    })?;
            if count == 0 {
                break;
            }
            file_hash.update(&buffer[..count]);
            if started.elapsed() >= HASH_FILES_TIMEOUT {
                return Err(ExpressionError::HashFilesTimeout);
            }
        }
        aggregate.update(file_hash.finalize());
    }
    let digest = aggregate.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(encoded, "{byte:02x}").expect("writing to a string cannot fail");
    }
    Ok(encoded)
}

fn compile_hash_patterns(
    workspace: &Path,
    arguments: &[String],
) -> Result<Vec<HashPattern>, ExpressionError> {
    let mut compiled = Vec::new();
    for argument in arguments {
        for line in argument.lines().map(str::trim) {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let original = line.to_owned();
            let (exclude, line) = strip_hash_pattern_negation(line);
            if line.is_empty() {
                return Err(ExpressionError::InvalidHashFilesPattern {
                    pattern: original,
                    reason: "pattern cannot be empty".to_owned(),
                });
            }
            let Some(pattern) = normalize_hash_pattern(workspace, line)? else {
                continue;
            };
            let matcher = build_hash_matcher(&original, &pattern)?;
            let descendant_matcher = if pattern.ends_with("**") {
                None
            } else {
                let base = pattern.trim_end_matches('/');
                let descendant = if base.is_empty() {
                    "**".to_owned()
                } else {
                    format!("{base}/**")
                };
                Some(build_hash_matcher(&original, &descendant)?)
            };
            compiled.push(HashPattern {
                exclude,
                matcher,
                descendant_matcher,
            });
        }
    }
    Ok(compiled)
}

fn strip_hash_pattern_negation(mut pattern: &str) -> (bool, &str) {
    let mut exclude = false;
    while let Some(remainder) = pattern.strip_prefix('!') {
        exclude = !exclude;
        pattern = remainder.trim_start();
    }
    (exclude, pattern)
}

fn normalize_hash_pattern(
    workspace: &Path,
    pattern: &str,
) -> Result<Option<String>, ExpressionError> {
    let workspace_text = slash_path(workspace)?;
    let mut normalized = pattern.to_owned();
    if normalized == workspace_text {
        normalized.clear();
    } else if let Some(relative) = normalized.strip_prefix(&format!("{workspace_text}/")) {
        normalized = relative.to_owned();
    } else if Path::new(pattern).is_absolute() {
        normalized = normalized.trim_start_matches('/').to_owned();
    }
    while let Some(relative) = normalized.strip_prefix("./") {
        normalized = relative.to_owned();
    }
    if normalized == "." {
        normalized.clear();
    }
    if normalized.starts_with('~') {
        return Ok(None);
    }
    if normalized
        .split('/')
        .any(|segment| segment == "." || segment == "..")
    {
        return Err(ExpressionError::InvalidHashFilesPattern {
            pattern: pattern.to_owned(),
            reason: "relative '.' and '..' segments are not allowed".to_owned(),
        });
    }
    if normalized.is_empty() {
        normalized = "**".to_owned();
    }
    Ok(Some(escape_globset_alternation(&normalized)))
}

fn escape_globset_alternation(pattern: &str) -> String {
    let mut escaped = String::with_capacity(pattern.len());
    let mut backslashes = 0_usize;
    for character in pattern.chars() {
        if matches!(character, '{' | '}') && backslashes.is_multiple_of(2) {
            escaped.push('\\');
        }
        escaped.push(character);
        if character == '\\' {
            backslashes += 1;
        } else {
            backslashes = 0;
        }
    }
    escaped
}

fn build_hash_matcher(original: &str, pattern: &str) -> Result<GlobMatcher, ExpressionError> {
    GlobBuilder::new(pattern)
        .literal_separator(true)
        .backslash_escape(true)
        .build()
        .map(|glob| glob.compile_matcher())
        .map_err(|error| ExpressionError::InvalidHashFilesPattern {
            pattern: original.to_owned(),
            reason: error.to_string(),
        })
}

fn slash_path(path: &Path) -> Result<String, ExpressionError> {
    let mut result = String::new();
    for component in path.components() {
        match component {
            Component::RootDir => result.push('/'),
            Component::Normal(value) => {
                if !result.is_empty() && !result.ends_with('/') {
                    result.push('/');
                }
                result.push_str(
                    value
                        .to_str()
                        .ok_or_else(|| ExpressionError::HashFilesNonUtf8Path(path.to_owned()))?,
                );
            }
            Component::CurDir => {}
            Component::ParentDir | Component::Prefix(_) => {
                return Err(ExpressionError::InvalidHashFilesPattern {
                    pattern: path.display().to_string(),
                    reason: "unsupported path component".to_owned(),
                });
            }
        }
    }
    Ok(result)
}

enum Selection {
    One(Evaluation),
    Many(Vec<Evaluation>),
}

impl Selection {
    fn access(self, index: Evaluation) -> Result<Self, ExpressionError> {
        match self {
            Self::One(value) => Ok(Self::One(access_value(value, &index)?)),
            Self::Many(values) => values
                .into_iter()
                .map(|value| access_value(value, &index))
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Many),
        }
    }

    fn wildcard(self) -> Result<Self, ExpressionError> {
        let expand = |value: Evaluation| match value {
            Evaluation::Array(values) => Ok(values),
            Evaluation::Object(values) => Ok(values.into_values().collect()),
            other => Err(ExpressionError::InvalidAccess {
                index: "*".to_owned(),
                value_type: evaluation_type(&other),
            }),
        };
        match self {
            Self::One(value) => expand(value).map(Self::Many),
            Self::Many(values) => values
                .into_iter()
                .map(expand)
                .collect::<Result<Vec<_>, _>>()
                .map(|values| Self::Many(values.into_iter().flatten().collect())),
        }
    }

    fn finish(self) -> Evaluation {
        match self {
            Self::One(value) => value,
            Self::Many(values) => Evaluation::Array(values),
        }
    }
}

fn access_value(value: Evaluation, index: &Evaluation) -> Result<Evaluation, ExpressionError> {
    let display_index = index.sema().to_string();
    match value {
        Evaluation::Object(values) => values
            .into_iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(&display_index))
            .map(|(_, value)| value)
            .map_or_else(|| Ok(Evaluation::String(String::new())), Ok),
        Evaluation::Array(values) => {
            let index = match index {
                Evaluation::Number(value) if value.fract() == 0.0 && *value >= 0.0 => {
                    *value as usize
                }
                Evaluation::String(value) => {
                    value
                        .parse::<usize>()
                        .map_err(|_| ExpressionError::InvalidAccess {
                            index: value.clone(),
                            value_type: "an array",
                        })?
                }
                _ => {
                    return Err(ExpressionError::InvalidAccess {
                        index: display_index,
                        value_type: "an array",
                    });
                }
            };
            values
                .get(index)
                .cloned()
                .map_or_else(|| Ok(Evaluation::String(String::new())), Ok)
        }
        Evaluation::Null => Ok(Evaluation::Null),
        Evaluation::String(value) if value.is_empty() => Ok(Evaluation::String(value)),
        other => Err(ExpressionError::InvalidAccess {
            index: display_index,
            value_type: evaluation_type(&other),
        }),
    }
}

fn evaluate_literal(literal: &Literal<'_>) -> Evaluation {
    match literal {
        Literal::String(value) => Evaluation::String(value.to_string()),
        Literal::Number(value) => Evaluation::Number(*value),
        Literal::Boolean(value) => Evaluation::Boolean(*value),
        Literal::Null => Evaluation::Null,
    }
}

fn contains(arguments: &[Evaluation]) -> Result<Evaluation, ExpressionError> {
    let [haystack, needle] = arguments else {
        return Err(ExpressionError::InvalidFunction("contains"));
    };
    let contains = match haystack {
        Evaluation::Array(values) => values.iter().any(|value| value.sema() == needle.sema()),
        Evaluation::Object(_) => return Err(ExpressionError::InvalidFunction("contains")),
        _ => haystack
            .sema()
            .to_string()
            .to_uppercase()
            .contains(&needle.sema().to_string().to_uppercase()),
    };
    Ok(Evaluation::Boolean(contains))
}

fn starts_or_ends_with(
    arguments: &[Evaluation],
    starts: bool,
) -> Result<Evaluation, ExpressionError> {
    let [haystack, needle] = arguments else {
        return Err(ExpressionError::InvalidFunction(if starts {
            "startsWith"
        } else {
            "endsWith"
        }));
    };
    if matches!(haystack, Evaluation::Array(_) | Evaluation::Object(_))
        || matches!(needle, Evaluation::Array(_) | Evaluation::Object(_))
    {
        return Ok(Evaluation::Boolean(false));
    }
    let haystack = haystack.sema().to_string().to_uppercase();
    let needle = needle.sema().to_string().to_uppercase();
    Ok(Evaluation::Boolean(if starts {
        haystack.starts_with(&needle)
    } else {
        haystack.ends_with(&needle)
    }))
}

fn format_values(arguments: &[Evaluation]) -> Result<Evaluation, ExpressionError> {
    let Some(template) = arguments.first() else {
        return Err(ExpressionError::InvalidFunction("format"));
    };
    let template = template.sema().to_string();
    let mut output = String::with_capacity(template.len());
    let bytes = template.as_bytes();
    let mut cursor = 0;
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'{' if bytes.get(cursor + 1) == Some(&b'{') => {
                output.push('{');
                cursor += 2;
            }
            b'}' if bytes.get(cursor + 1) == Some(&b'}') => {
                output.push('}');
                cursor += 2;
            }
            b'{' => {
                let end = template[cursor + 1..]
                    .find('}')
                    .map(|offset| cursor + 1 + offset)
                    .ok_or(ExpressionError::InvalidFunction("format"))?;
                let index = template[cursor + 1..end]
                    .split(':')
                    .next()
                    .and_then(|value| value.parse::<usize>().ok())
                    .ok_or(ExpressionError::InvalidFunction("format"))?;
                let value = arguments
                    .get(index + 1)
                    .ok_or(ExpressionError::InvalidFunction("format"))?;
                output.push_str(&value.sema().to_string());
                cursor = end + 1;
            }
            b'}' => return Err(ExpressionError::InvalidFunction("format")),
            _ => {
                let character = template[cursor..]
                    .chars()
                    .next()
                    .ok_or(ExpressionError::InvalidFunction("format"))?;
                output.push(character);
                cursor += character.len_utf8();
            }
        }
    }
    Ok(Evaluation::String(output))
}

fn join(arguments: &[Evaluation]) -> Result<Evaluation, ExpressionError> {
    if arguments.is_empty() || arguments.len() > 2 {
        return Err(ExpressionError::InvalidFunction("join"));
    }
    let separator = arguments
        .get(1)
        .map_or_else(|| ",".to_owned(), |value| value.sema().to_string());
    let value = match &arguments[0] {
        Evaluation::Array(values) => values
            .iter()
            .map(|value| value.sema().to_string())
            .collect::<Vec<_>>()
            .join(&separator),
        Evaluation::Object(_) => String::new(),
        value => value.sema().to_string(),
    };
    Ok(Evaluation::String(value))
}

fn to_json(arguments: &[Evaluation]) -> Result<Evaluation, ExpressionError> {
    let [value] = arguments else {
        return Err(ExpressionError::InvalidFunction("toJSON"));
    };
    let json: Value = value
        .clone()
        .try_into()
        .map_err(|()| ExpressionError::JsonConversion)?;
    let json = serde_json::to_string_pretty(&json).map_err(|_| ExpressionError::JsonConversion)?;
    Ok(Evaluation::String(json))
}

fn from_json(arguments: &[Evaluation]) -> Result<Evaluation, ExpressionError> {
    let [value] = arguments else {
        return Err(ExpressionError::InvalidFunction("fromJSON"));
    };
    let json: Value = serde_json::from_str(&value.sema().to_string())
        .map_err(|_| ExpressionError::InvalidFunction("fromJSON"))?;
    Evaluation::try_from(json).map_err(|()| ExpressionError::JsonConversion)
}

fn case(arguments: &[Evaluation]) -> Result<Evaluation, ExpressionError> {
    if arguments.len() < 3 || arguments.len().is_multiple_of(2) {
        return Err(ExpressionError::InvalidFunction("case"));
    }
    let default = arguments.last().cloned().expect("length checked");
    for pair in arguments[..arguments.len() - 1].chunks_exact(2) {
        match pair[0] {
            Evaluation::Boolean(true) => return Ok(pair[1].clone()),
            Evaluation::Boolean(false) => {}
            _ => return Err(ExpressionError::InvalidFunction("case")),
        }
    }
    Ok(default)
}

fn unwrap_expression(source: &str) -> &str {
    let trimmed = source.trim();
    trimmed
        .strip_prefix("${{")
        .and_then(|value| value.strip_suffix("}}"))
        .map(str::trim)
        .unwrap_or(trimmed)
}

fn find_expression_end(template: &str, start: usize) -> Option<usize> {
    let mut quoted = false;
    let mut characters = template[start..].char_indices().peekable();
    while let Some((offset, character)) = characters.next() {
        let absolute = start + offset;
        if character == '\'' {
            if quoted && characters.peek().is_some_and(|(_, next)| *next == '\'') {
                characters.next();
            } else {
                quoted = !quoted;
            }
        } else if !quoted && template[absolute..].starts_with("}}") {
            return Some(absolute);
        }
    }
    None
}

fn evaluation_type(value: &Evaluation) -> &'static str {
    match value {
        Evaluation::String(_) => "a string",
        Evaluation::Number(_) => "a number",
        Evaluation::Boolean(_) => "a boolean",
        Evaluation::Null => "null",
        Evaluation::Array(_) => "an array",
        Evaluation::Object(_) => "an object",
    }
}

fn json_type(value: &Value) -> &'static str {
    match value {
        Value::String(_) => "a string",
        Value::Number(_) => "a number",
        Value::Bool(_) => "a boolean",
        Value::Null => "null",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn context() -> EvaluationContext {
        let mut context = EvaluationContext::new();
        context
            .insert_json(
                "github",
                json!({
                    "sha": "abc123",
                    "event": {"pull_request": {"number": 42}},
                    "labels": [{"name": "bug"}, {"name": "urgent"}]
                }),
            )
            .expect("context");
        context
            .insert_json("env", json!({"MODE": "release"}))
            .expect("context");
        context
    }

    #[test]
    fn evaluates_contexts_and_github_coercions() {
        let context = context();
        assert_eq!(
            context.evaluate("github.sha").expect("evaluate"),
            Evaluation::String("abc123".to_owned())
        );
        assert!(
            context
                .evaluate_condition("github.event.pull_request.number == '42'")
                .expect("condition")
        );
        assert!(
            context
                .evaluate_condition("env.mode == 'RELEASE'")
                .expect("case insensitive")
        );
        assert_eq!(
            context
                .evaluate("github.missing.deep")
                .expect("missing property"),
            Evaluation::String(String::new())
        );
        assert_eq!(
            context
                .evaluate("github.missing || 'fallback'")
                .expect("missing fallback"),
            Evaluation::String("fallback".to_owned())
        );
    }

    #[test]
    fn supports_wildcard_object_filters() {
        let context = context();
        assert_eq!(
            context.evaluate("github.labels.*.name").expect("wildcard"),
            Evaluation::Array(vec![
                Evaluation::String("bug".to_owned()),
                Evaluation::String("urgent".to_owned()),
            ])
        );
    }

    #[test]
    fn renders_multiple_expressions_without_losing_literal_text() {
        let context = context();
        assert_eq!(
            context
                .render("sha=${{ github.sha }} mode=${{ env.MODE }}")
                .expect("render"),
            "sha=abc123 mode=release"
        );
    }

    #[test]
    fn evaluates_functions_and_short_circuit_values() {
        let context = context();
        assert_eq!(
            context
                .evaluate("format('{0}-{1}', github.sha, join(github.labels.*.name, ','))")
                .expect("functions"),
            Evaluation::String("abc123-bug,urgent".to_owned())
        );
        assert_eq!(
            context
                .evaluate("false && missing.value || 'fallback'")
                .expect("short circuit"),
            Evaluation::String("fallback".to_owned())
        );
    }

    #[test]
    fn status_functions_follow_execution_state() {
        let failed = context().with_status(ExecutionStatus::Failure);
        assert!(failed.evaluate_condition("failure()").expect("failure"));
        assert!(!failed.evaluate_condition("success()").expect("success"));
        assert!(failed.evaluate_condition("always()").expect("always"));

        let skipped = context().with_status(ExecutionStatus::Skipped);
        assert!(!skipped.evaluate_condition("failure()").expect("failure"));
        assert!(!skipped.evaluate_condition("success()").expect("success"));
        assert!(
            !skipped
                .evaluate_condition("cancelled()")
                .expect("cancelled")
        );
        assert!(skipped.evaluate_condition("always()").expect("always"));
    }

    #[test]
    fn renders_braces_inside_expression_strings() {
        let context = context();
        assert_eq!(
            context
                .render("${{ format('{{{0}}}', github.sha) }}")
                .expect("format"),
            "{abc123}"
        );
    }

    #[test]
    fn analyzes_nested_context_roots_and_special_functions() {
        let analysis = analyze_expression(
            "format('{0}', matrix[vars.axis]) || fromJSON(inputs.fallback).runner",
        )
        .expect("analysis");
        assert_eq!(
            analysis.context_roots,
            BTreeSet::from(["inputs".to_owned(), "matrix".to_owned(), "vars".to_owned(),])
        );
        assert!(!analysis.uses_status_function);
        assert!(!analysis.uses_hash_files);

        let analysis = analyze_expression("always() && hashFiles('Cargo.lock') != ''")
            .expect("special function analysis");
        assert!(analysis.context_roots.is_empty());
        assert!(analysis.uses_status_function);
        assert!(analysis.uses_hash_files);
    }

    #[test]
    fn analyzes_all_expressions_in_a_template() {
        let analysis =
            analyze_template("release-${{ matrix.channel }}-${{ github.event[vars.event_key] }}")
                .expect("template analysis");
        assert_eq!(
            analysis.context_roots,
            BTreeSet::from(["github".to_owned(), "matrix".to_owned(), "vars".to_owned(),])
        );
        assert!(matches!(
            analyze_template("${{ matrix.arch"),
            Err(ExpressionError::UnclosedTemplate)
        ));
    }

    #[test]
    fn hash_files_matches_hidden_files_and_ordered_patterns() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir(workspace.path().join("nested")).expect("nested directory");
        std::fs::write(workspace.path().join(".hidden.lock"), b"alpha\n").expect("alpha");
        std::fs::write(workspace.path().join("nested/beta.lock"), b"beta\n").expect("beta");
        std::fs::write(workspace.path().join("nested/ignored.txt"), b"ignored\n").expect("ignored");
        let context = EvaluationContext::new().with_workspace(workspace.path());

        assert_eq!(
            context
                .evaluate("hashFiles('**/*.lock', '!nested/**', 'nested/beta.lock')")
                .expect("hash files"),
            Evaluation::String(
                "24d116e0411b3a4a8d3d5c9c88c150bc4d4603a490294bd4b23d3ef549e1f1a0".to_owned(),
            )
        );
        assert_eq!(
            context
                .evaluate("hashFiles('**/*.lock', '!nested/**')")
                .expect("exclude nested"),
            Evaluation::String(
                "4bb706b95c7ea23f44bc5d035ad8841af479871295d2ae0c685d07174705c880".to_owned(),
            )
        );
    }

    #[test]
    fn hash_files_supports_rooted_and_implicit_descendant_patterns() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir(workspace.path().join("nested")).expect("nested directory");
        std::fs::write(workspace.path().join("nested/beta.lock"), b"beta\n").expect("beta");
        let context = EvaluationContext::new().with_workspace(workspace.path());
        let expected = Evaluation::String(
            "4f15b167c72188ea90d8970ceb3d45eaac56cde3dff2dd4fcd80e7faabd29987".to_owned(),
        );

        assert_eq!(
            context.evaluate("hashFiles('nested')").expect("directory"),
            expected
        );
        assert_eq!(
            context
                .evaluate("hashFiles('/nested/*.lock')")
                .expect("root-relative"),
            expected
        );
        assert_eq!(
            context
                .evaluate("hashFiles('missing/**')")
                .expect("no matches"),
            Evaluation::String(String::new())
        );
    }

    #[cfg(unix)]
    #[test]
    fn hash_files_keeps_symlinks_inside_the_workspace() {
        use std::os::unix::fs::symlink;

        const FOLLOW_OUTSIDE_DIRECTORY: &str =
            "hashFiles('--follow-symbolic-links', 'outside-directory/**/*.lock')";
        let root = tempfile::tempdir().expect("root");
        let workspace = root.path().join("workspace");
        let target = workspace.join("target");
        std::fs::create_dir_all(&target).expect("target directory");
        std::fs::write(target.join("beta.lock"), b"beta\n").expect("beta");
        symlink(&target, workspace.join("alias")).expect("inside symlink");
        let outside = root.path().join("outside.lock");
        std::fs::write(&outside, b"outside\n").expect("outside");
        symlink(&outside, workspace.join("outside.lock")).expect("outside symlink");
        let outside_directory = root.path().join("outside-directory");
        std::fs::create_dir(&outside_directory).expect("outside directory");
        std::fs::write(outside_directory.join("secret.lock"), b"secret\n").expect("secret");
        symlink(&outside_directory, workspace.join("outside-directory"))
            .expect("outside directory symlink");
        let context = EvaluationContext::new().with_workspace(&workspace);

        assert_eq!(
            context
                .evaluate("hashFiles('alias/**/*.lock')")
                .expect("do not descend through symlink"),
            Evaluation::String(String::new())
        );
        assert_eq!(
            context
                .evaluate("hashFiles('--follow-symbolic-links', 'alias/**/*.lock')")
                .expect("follow inside symlink"),
            Evaluation::String(
                "4f15b167c72188ea90d8970ceb3d45eaac56cde3dff2dd4fcd80e7faabd29987".to_owned(),
            )
        );
        assert_eq!(
            context
                .evaluate("hashFiles('outside.lock')")
                .expect("skip outside target"),
            Evaluation::String(String::new())
        );
        assert_eq!(
            context
                .evaluate(FOLLOW_OUTSIDE_DIRECTORY)
                .expect("do not descend outside workspace"),
            Evaluation::String(String::new())
        );
    }

    #[test]
    fn hash_files_rejects_invalid_options_and_parent_segments() {
        let workspace = tempfile::tempdir().expect("workspace");
        let context = EvaluationContext::new().with_workspace(workspace.path());

        assert!(matches!(
            context.evaluate("hashFiles('--unknown', '**')"),
            Err(ExpressionError::InvalidHashFilesOption(option)) if option == "--unknown"
        ));
        assert!(matches!(
            context.evaluate("hashFiles('../outside')"),
            Err(ExpressionError::InvalidHashFilesPattern { .. })
        ));
        assert!(matches!(
            EvaluationContext::new().evaluate("hashFiles('**')"),
            Err(ExpressionError::UnsupportedFunction("hashFiles"))
        ));
    }
}
