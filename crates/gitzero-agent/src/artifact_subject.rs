use anyhow::{Context, Result, bail};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{fs::File, io::AsyncReadExt, sync::Mutex};
use uuid::Uuid;

const MAX_ARTIFACTS_FILE_BYTES: u64 = 1_024 * 1_024;
const MAX_ARTIFACT_SUBJECTS: usize = 500;

#[derive(Clone)]
pub(crate) struct ArtifactSubjects {
    enabled: bool,
    workspace: PathBuf,
    subjects: Arc<Mutex<BTreeMap<String, ArtifactSubject>>>,
}

pub(crate) struct ArtifactSubjectFiles {
    pub(crate) declarations: PathBuf,
    pub(crate) list: PathBuf,
}

#[derive(Debug)]
pub(crate) struct ArtifactCapture {
    pub(crate) added: usize,
    pub(crate) total: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ArtifactSubject {
    name: String,
    digest: String,
    kind: ArtifactSubjectKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArtifactSubjectKind {
    File,
    Oci,
}

impl Serialize for ArtifactSubjectKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(match self {
            Self::File => "file",
            Self::Oci => "oci",
        })
    }
}

#[derive(Serialize)]
struct ArtifactSubjectList<'a> {
    version: u8,
    subjects: Vec<&'a ArtifactSubject>,
}

impl ArtifactSubjects {
    pub(crate) fn new(enabled: bool, workspace: PathBuf) -> Self {
        Self {
            enabled,
            workspace,
            subjects: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub(crate) async fn initialize_files(
        &self,
        directory: &Path,
        prefix: &str,
    ) -> Result<ArtifactSubjectFiles> {
        let nonce = Uuid::new_v4();
        let declarations = directory.join(format!("{prefix}-{nonce}-artifacts.txt"));
        let list = directory.join(format!("{prefix}-{nonce}-artifacts-list.json"));
        tokio::fs::write(&declarations, b"")
            .await
            .with_context(|| format!("initialize {}", declarations.display()))?;
        tokio::fs::write(&list, b"")
            .await
            .with_context(|| format!("initialize {}", list.display()))?;
        if self.enabled {
            let payload = {
                let subjects = self.subjects.lock().await;
                let mut ordered = subjects.values().collect::<Vec<_>>();
                ordered
                    .sort_by(|left, right| left.name.encode_utf16().cmp(right.name.encode_utf16()));
                serde_json::to_vec(&ArtifactSubjectList {
                    version: 1,
                    subjects: ordered,
                })?
            };
            tokio::fs::write(&list, payload)
                .await
                .with_context(|| format!("populate {}", list.display()))?;
        }
        Ok(ArtifactSubjectFiles { declarations, list })
    }

    pub(crate) async fn process(&self, path: &Path) -> Result<ArtifactCapture> {
        if !self.enabled {
            return Ok(ArtifactCapture { added: 0, total: 0 });
        }
        let metadata = match tokio::fs::metadata(path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let total = self.subjects.lock().await.len();
                return Ok(ArtifactCapture { added: 0, total });
            }
            Err(error) => return Err(error).with_context(|| format!("inspect {}", path.display())),
        };
        if metadata.len() == 0 {
            let total = self.subjects.lock().await.len();
            return Ok(ArtifactCapture { added: 0, total });
        }
        if metadata.len() > MAX_ARTIFACTS_FILE_BYTES {
            bail!(
                "$GITHUB_ARTIFACTS file exceeds the {} KiB limit ({} KiB)",
                MAX_ARTIFACTS_FILE_BYTES / 1_024,
                metadata.len() / 1_024
            );
        }

        let contents = tokio::fs::read(path)
            .await
            .with_context(|| format!("read {}", path.display()))?;
        let contents = contents
            .strip_prefix(&[0xEF, 0xBB, 0xBF])
            .unwrap_or(&contents);
        let contents = String::from_utf8_lossy(contents);
        let mut parsed = Vec::new();
        for (index, raw) in runner_lines(&contents).into_iter().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let subject = self
                .parse_line(line)
                .await
                .with_context(|| format!("invalid $GITHUB_ARTIFACTS line {}", index + 1))?;
            parsed.push((index + 1, subject));
        }

        let mut aggregate = self.subjects.lock().await;
        let mut added = 0;
        for (line, subject) in parsed {
            if let Some(existing) = aggregate.get(&subject.name) {
                if existing.digest == subject.digest {
                    continue;
                }
                bail!(
                    "invalid $GITHUB_ARTIFACTS line {line}: subject '{}' conflicts with digest '{}' already declared for that name (new digest '{}')",
                    subject.name,
                    existing.digest,
                    subject.digest
                );
            }
            if aggregate.len() >= MAX_ARTIFACT_SUBJECTS {
                bail!(
                    "invalid $GITHUB_ARTIFACTS line {line}: a job may declare at most {MAX_ARTIFACT_SUBJECTS} artifact subjects"
                );
            }
            aggregate.insert(subject.name.clone(), subject);
            added += 1;
        }
        Ok(ArtifactCapture {
            added,
            total: aggregate.len(),
        })
    }

    async fn parse_line(&self, line: &str) -> Result<ArtifactSubject> {
        if line.contains('=') {
            bail!("entries containing '=' are reserved and not permitted");
        }
        if starts_with_ignore_ascii_case(line, "file://") {
            let path = &line["file://".len()..];
            if path.trim().is_empty() {
                bail!("file:// entries must include a path");
            }
            return self.file_subject(path).await;
        }
        if starts_with_ignore_ascii_case(line, "oci://") {
            let value = &line["oci://".len()..];
            let (reference, algorithm, hex) = parse_oci(value)
                .context("oci:// entries must include an @sha{256,384,512}:<hex> digest")?;
            return oci_subject(reference, algorithm, hex);
        }
        if has_uri_scheme(line) {
            bail!("unsupported URI scheme");
        }
        if let Some((reference, algorithm, hex)) = parse_oci(line)
            && expected_hex_length(algorithm) == Some(hex.len())
        {
            return oci_subject(reference, algorithm, hex);
        }
        self.file_subject(line).await
    }

    async fn file_subject(&self, declared_path: &str) -> Result<ArtifactSubject> {
        let declared = Path::new(declared_path);
        let path = if declared.is_absolute() {
            declared.to_owned()
        } else {
            normalize_path(&self.workspace.join(declared))
        };
        let metadata = match tokio::fs::metadata(&path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if !declared.is_absolute() {
                    bail!(
                        "file '{declared_path}' does not exist (relative paths are resolved against the workspace root '{}')",
                        self.workspace.display()
                    );
                }
                bail!("file '{declared_path}' does not exist");
            }
            Err(error) => {
                return Err(error).with_context(|| format!("inspect '{}'", path.display()));
            }
        };
        if !metadata.is_file() {
            if metadata.is_dir() {
                bail!("'{declared_path}' is a directory, not a regular file");
            }
            bail!("'{declared_path}' is not a regular file");
        }
        let name = path
            .file_name()
            .context("artifact file path does not have a file name")?
            .to_string_lossy()
            .into_owned();
        let mut file = File::open(&path)
            .await
            .with_context(|| format!("open artifact file {}", path.display()))?;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; 64 * 1_024];
        loop {
            let read = file
                .read(&mut buffer)
                .await
                .with_context(|| format!("hash artifact file {}", path.display()))?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        let digest = hasher.finalize();
        let hex = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        Ok(ArtifactSubject {
            name,
            digest: format!("sha256:{hex}"),
            kind: ArtifactSubjectKind::File,
        })
    }
}

pub(crate) fn artifacts_file_enabled(value: Option<&str>) -> bool {
    value.is_some_and(|value| {
        value.eq_ignore_ascii_case("1")
            || value.eq_ignore_ascii_case("true")
            || value.eq_ignore_ascii_case("$true")
    })
}

fn starts_with_ignore_ascii_case(value: &str, prefix: &str) -> bool {
    value
        .get(..prefix.len())
        .is_some_and(|value| value.eq_ignore_ascii_case(prefix))
}

fn runner_lines(value: &str) -> Vec<&str> {
    let bytes = value.as_bytes();
    let mut lines = Vec::new();
    let mut start = 0;
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'\n' && bytes[index] != b'\r' {
            index += 1;
            continue;
        }
        lines.push(&value[start..index]);
        if bytes[index] == b'\r' && bytes.get(index + 1) == Some(&b'\n') {
            index += 2;
        } else {
            index += 1;
        }
        start = index;
    }
    if start < value.len() {
        lines.push(&value[start..]);
    }
    lines
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            component => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

fn has_uri_scheme(value: &str) -> bool {
    let Some((scheme, _)) = value.split_once("://") else {
        return false;
    };
    let mut characters = scheme.chars();
    characters
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic())
        && characters.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '+' | '.' | '-')
        })
}

fn parse_oci(value: &str) -> Option<(&str, &str, &str)> {
    let (reference, digest) = value.rsplit_once('@')?;
    let (algorithm, hex) = digest.split_once(':')?;
    if reference.is_empty()
        || !matches!(algorithm, "sha256" | "sha384" | "sha512")
        || hex.is_empty()
        || !hex.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    Some((reference, algorithm, hex))
}

fn expected_hex_length(algorithm: &str) -> Option<usize> {
    match algorithm {
        "sha256" => Some(64),
        "sha384" => Some(96),
        "sha512" => Some(128),
        _ => None,
    }
}

fn oci_subject(reference: &str, algorithm: &str, hex: &str) -> Result<ArtifactSubject> {
    let expected = expected_hex_length(algorithm).context("unsupported OCI digest algorithm")?;
    if hex.len() != expected {
        bail!(
            "digest '{algorithm}' must be {expected} hex characters, got {}",
            hex.len()
        );
    }
    Ok(ArtifactSubject {
        name: reference.to_owned(),
        digest: format!("{algorithm}:{}", hex.to_ascii_lowercase()),
        kind: ArtifactSubjectKind::Oci,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_runner_boolean_conversion_for_enabling_values() {
        for value in ["1", "true", "TRUE", "$true", "$TRUE"] {
            assert!(artifacts_file_enabled(Some(value)), "{value}");
        }
        for value in ["", "0", "false", "$false", "yes", " true "] {
            assert!(!artifacts_file_enabled(Some(value)), "{value}");
        }
        assert!(!artifacts_file_enabled(None));
    }

    #[tokio::test]
    async fn exposes_empty_files_but_ignores_writes_when_disabled() {
        let fixture = tempfile::tempdir().expect("artifact subject fixture");
        let subjects = ArtifactSubjects::new(false, fixture.path().to_owned());
        let files = subjects
            .initialize_files(fixture.path(), "step")
            .await
            .expect("initialize disabled files");
        assert_eq!(tokio::fs::read(&files.list).await.unwrap(), b"");
        tokio::fs::write(&files.declarations, b"missing.txt\n")
            .await
            .unwrap();
        let capture = subjects.process(&files.declarations).await.unwrap();
        assert_eq!((capture.added, capture.total), (0, 0));
    }

    #[tokio::test]
    async fn captures_files_and_oci_subjects_in_a_sorted_job_list() {
        let fixture = tempfile::tempdir().expect("artifact subject fixture");
        tokio::fs::write(fixture.path().join("z.txt"), b"file subject")
            .await
            .unwrap();
        let subjects = ArtifactSubjects::new(true, fixture.path().to_owned());
        let first = subjects
            .initialize_files(fixture.path(), "first")
            .await
            .unwrap();
        assert_eq!(
            tokio::fs::read_to_string(&first.list).await.unwrap(),
            r#"{"version":1,"subjects":[]}"#
        );
        let digest = "A".repeat(64);
        tokio::fs::write(
            &first.declarations,
            format!("# comment\nz.txt\noci://example/image@sha256:{digest}\nz.txt\n"),
        )
        .await
        .unwrap();
        let capture = subjects.process(&first.declarations).await.unwrap();
        assert_eq!((capture.added, capture.total), (2, 2));

        let second = subjects
            .initialize_files(fixture.path(), "second")
            .await
            .unwrap();
        let list: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&second.list).await.unwrap()).unwrap();
        assert_eq!(list["version"], 1);
        assert_eq!(list["subjects"][0]["name"], "example/image");
        assert_eq!(
            list["subjects"][0]["digest"],
            format!("sha256:{}", "a".repeat(64))
        );
        assert_eq!(list["subjects"][0]["kind"], "oci");
        assert_eq!(list["subjects"][1]["name"], "z.txt");
        assert_eq!(
            list["subjects"][1]["digest"],
            "sha256:3b6a47c58ad6c3d75f7b4e8c490abc7cb5ab8549e4b041b58b260449e98525b0"
        );
        assert_eq!(list["subjects"][1]["kind"], "file");
    }

    #[tokio::test]
    async fn accepts_a_bom_and_runner_line_endings_and_sorts_names_by_utf16_ordinal() {
        let fixture = tempfile::tempdir().expect("artifact subject fixture");
        tokio::fs::write(fixture.path().join("file.txt"), b"subject")
            .await
            .unwrap();
        let subjects = ArtifactSubjects::new(true, fixture.path().to_owned());
        let files = subjects
            .initialize_files(fixture.path(), "ordinal")
            .await
            .unwrap();
        let digest = "0".repeat(64);
        tokio::fs::write(
            &files.declarations,
            format!(
                "\u{feff}missing/../file.txt\r{}@sha256:{digest}\r\n{}@sha256:{digest}\n",
                '\u{e000}', '\u{10000}'
            ),
        )
        .await
        .unwrap();
        let capture = subjects.process(&files.declarations).await.unwrap();
        assert_eq!((capture.added, capture.total), (3, 3));
        let next = subjects
            .initialize_files(fixture.path(), "next")
            .await
            .unwrap();
        let list: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&next.list).await.unwrap()).unwrap();
        assert_eq!(
            list["subjects"]
                .as_array()
                .unwrap()
                .iter()
                .map(|subject| subject["name"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["file.txt", "\u{10000}", "\u{e000}"]
        );
    }

    #[tokio::test]
    async fn rejects_malformed_entries_before_mutating_the_job_aggregate() {
        let fixture = tempfile::tempdir().expect("artifact subject fixture");
        tokio::fs::write(fixture.path().join("valid.txt"), b"valid")
            .await
            .unwrap();
        let subjects = ArtifactSubjects::new(true, fixture.path().to_owned());
        let first = subjects
            .initialize_files(fixture.path(), "first")
            .await
            .unwrap();
        tokio::fs::write(
            &first.declarations,
            b"valid.txt\nhttps://example.test/value\n",
        )
        .await
        .unwrap();
        let error = subjects.process(&first.declarations).await.unwrap_err();
        assert!(format!("{error:#}").contains("unsupported URI scheme"));
        let second = subjects
            .initialize_files(fixture.path(), "second")
            .await
            .unwrap();
        assert_eq!(
            tokio::fs::read_to_string(&second.list).await.unwrap(),
            r#"{"version":1,"subjects":[]}"#
        );
    }

    #[tokio::test]
    async fn rejects_conflicting_digests_and_reserved_or_invalid_entries() {
        let fixture = tempfile::tempdir().expect("artifact subject fixture");
        let path = fixture.path().join("subject.txt");
        tokio::fs::write(&path, b"first").await.unwrap();
        let subjects = ArtifactSubjects::new(true, fixture.path().to_owned());
        let first = subjects
            .initialize_files(fixture.path(), "first")
            .await
            .unwrap();
        tokio::fs::write(&first.declarations, b"subject.txt\n")
            .await
            .unwrap();
        subjects.process(&first.declarations).await.unwrap();
        tokio::fs::write(&path, b"second").await.unwrap();
        let second = subjects
            .initialize_files(fixture.path(), "second")
            .await
            .unwrap();
        tokio::fs::write(&second.declarations, b"file://subject.txt\n")
            .await
            .unwrap();
        assert!(
            format!(
                "{:#}",
                subjects.process(&second.declarations).await.unwrap_err()
            )
            .contains("conflicts with digest")
        );

        for entry in ["value=name", "oci://image@sha256:abcd", "file://", "."] {
            let files = subjects
                .initialize_files(fixture.path(), "invalid")
                .await
                .unwrap();
            tokio::fs::write(&files.declarations, format!("{entry}\n"))
                .await
                .unwrap();
            assert!(
                subjects.process(&files.declarations).await.is_err(),
                "{entry}"
            );
        }
    }

    #[tokio::test]
    async fn enforces_the_runner_file_and_job_subject_limits() {
        let fixture = tempfile::tempdir().expect("artifact subject fixture");
        let subjects = ArtifactSubjects::new(true, fixture.path().to_owned());
        let oversized = subjects
            .initialize_files(fixture.path(), "oversized")
            .await
            .unwrap();
        tokio::fs::write(
            &oversized.declarations,
            vec![b'x'; MAX_ARTIFACTS_FILE_BYTES as usize + 1],
        )
        .await
        .unwrap();
        assert!(
            format!(
                "{:#}",
                subjects.process(&oversized.declarations).await.unwrap_err()
            )
            .contains("1024 KiB limit")
        );

        {
            let mut aggregate = subjects.subjects.lock().await;
            for index in 0..MAX_ARTIFACT_SUBJECTS {
                let name = format!("subject-{index:03}");
                aggregate.insert(
                    name.clone(),
                    ArtifactSubject {
                        name,
                        digest: format!("sha256:{}", "0".repeat(64)),
                        kind: ArtifactSubjectKind::Oci,
                    },
                );
            }
        }
        let over_limit = subjects
            .initialize_files(fixture.path(), "over-limit")
            .await
            .unwrap();
        tokio::fs::write(
            &over_limit.declarations,
            format!("final@sha256:{}\n", "1".repeat(64)),
        )
        .await
        .unwrap();
        assert!(
            format!(
                "{:#}",
                subjects
                    .process(&over_limit.declarations)
                    .await
                    .unwrap_err()
            )
            .contains("at most 500")
        );
    }
}
