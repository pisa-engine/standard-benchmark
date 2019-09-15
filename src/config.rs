//! This module contains all the config definitions that are deserialized
//! from a YAML configuration file.

use crate::{Error, Executor};
use boolinator::Boolinator;
use failure::ResultExt;
use log::warn;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::{fmt, fs};
use strum_macros::{Display, EnumIter, EnumString};
use url::Url;

fn process(args: &'static str) -> Command {
    let mut args = args.split(' ');
    let mut cmd = Command::new(args.next().unwrap());
    cmd.args(args.collect::<Vec<&str>>());
    cmd
}

pub(crate) fn resolve_files<P: AsRef<Path>>(path: P) -> Result<Vec<PathBuf>, Error> {
    let pattern = path.as_ref().to_str().unwrap();
    let files: Vec<_> = glob::glob(pattern)
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    (!files.is_empty()).ok_or(format!(
        "could not resolve any files for pattern: {}",
        pattern
    ))?;
    Ok(files)
}

/// Representation of experimental stages.
#[derive(
    Clone, Copy, Serialize, Deserialize, Debug, Hash, PartialEq, Eq, EnumIter, EnumString, Display,
)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    /// Program compilation, including git operations if applicable.
    #[strum(serialize = "compile")]
    Compile,
    /// Index building, including parsing, inverting, and compression.
    #[strum(serialize = "build_index")]
    BuildIndex,
    /// Parsing index, a subset of `BuildIndex`.
    #[strum(serialize = "parse")]
    Parse,
    /// Parsing index batches, a subset of `Parse`.
    #[strum(serialize = "parse_batches")]
    ParseBatches,
    /// Joining index batches, a subset of `Parse`.
    #[strum(serialize = "join")]
    Join,
    /// Inverting forward index, a subset of `BuildIndex`.
    #[strum(serialize = "invert")]
    Invert,
    /// Extracting WAND metadata index, a subset of `BuildIndex`.
    #[strum(serialize = "wand")]
    Wand,
    /// Compressing inverted index, a subset of `BuildIndex`.
    #[strum(serialize = "compress")]
    Compress,
    /// Running experiments.
    #[strum(serialize = "run")]
    Run,
}

fn true_default() -> bool {
    true
}

fn default_stages() -> HashMap<Stage, bool> {
    use Stage::*;
    [
        Compile,
        BuildIndex,
        Parse,
        ParseBatches,
        Join,
        Wand,
        Compress,
        Invert,
        Run,
    ]
    .iter()
    .cloned()
    .map(|stage| (stage, true))
    .collect()
}

/// Main config.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Config {
    /// All relative paths will fall back on to this directory.
    pub workdir: PathBuf,
    /// Source of the PISA tools.
    #[serde(default)]
    pub source: Source,
    /// Source of the PISA tools.
    pub collections: Vec<Collection>,
    /// List of experiments.
    #[serde(default)]
    pub runs: Vec<Run>,
    /// Enabled/disabled stages. Anything missing is implicitly enabled.
    #[serde(default = "default_stages")]
    pub stages: HashMap<Stage, bool>,
    /// Use `--scorer`. `false` for legacy PISA code before `ql3`.
    #[serde(default = "true_default")]
    pub use_scorer: bool,
    /// Clean up before running: remove work dir.
    #[serde(default)]
    pub clean: bool,
}

impl Config {
    /// Disable a particular stage.
    pub fn disable(&mut self, stage: Stage) {
        self.stages.insert(stage, false);
    }
    /// Returns `true` if a given stage is effectively enabled.
    pub fn enabled(&self, stage: Stage) -> bool {
        self.stages.get(&stage).cloned().unwrap_or(true)
    }

    fn git_clone(dir: &Path, url: &Url) -> Result<(), Error> {
        let status = Command::new("git")
            .arg("clone")
            .arg(&url.to_string())
            .arg(dir.to_str().unwrap())
            .status()?;
        if status.success() {
            Ok(())
        } else {
            Err(Error::from("git-clone has failed"))
        }
    }

    /// Construct an executor for a set of PISA tools.
    pub fn executor(&self) -> Result<Executor, Error> {
        match &self.source {
            Source::System => Ok(Executor::new()),
            Source::Git { branch, url } => {
                let dir = self.workdir.join("pisa");
                if !dir.exists() {
                    Self::git_clone(dir.as_ref(), &url)?;
                };
                let build_dir = dir.join("build");
                fs::create_dir_all(&build_dir).context("Could not create build directory")?;
                if self.stages.get(&Stage::Compile).cloned().unwrap_or(true) {
                    process("git reset --hard")
                        .current_dir(&dir)
                        .status()?
                        .success()
                        .ok_or("git-reset failed")?;
                    Command::new("git")
                        .args(&["checkout", branch])
                        .current_dir(&dir)
                        .status()?
                        .success()
                        .ok_or("git-checkout failed")?;
                    Command::new("git")
                        .arg("pull")
                        .current_dir(&dir)
                        .status()?
                        .success()
                        .ok_or("git-pull failed")?;
                    process("cmake -DCMAKE_BUILD_TYPE=Release ..")
                        .current_dir(&build_dir)
                        .status()?
                        .success()
                        .ok_or("cmake failed")?;
                    process("cmake --build .")
                        .current_dir(&build_dir)
                        .status()?
                        .success()
                        .ok_or("cmake --build failed")?;
                } else {
                    warn!("Compilation has been suppressed");
                }
                Ok(Executor::from(build_dir.join("bin")))
            }
            Source::Path(path) => Ok(Executor::from(path.to_path_buf())),
            Source::Docker(_) => unimplemented!(),
        }
    }
}

/// Source of PISA executables.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Source {
    /// Based on remote code on a Git repository.
    Git {
        /// Git branch to use.
        branch: String,
        /// HTTPS URL of the repository
        #[serde(with = "serde_url")]
        url: Url,
    },
    /// Executables in a given directory.
    Path(PathBuf),
    /// Executables in a given docker image.
    Docker(String),
    /// Executables on the system `PATH`.
    System,
}

impl Default for Source {
    fn default() -> Self {
        Self::System
    }
}

/// Supported types of collections:
/// <https://pisa.readthedocs.io/en/latest/parsing.html#supported-formats>
#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum CollectionKind {
    /// -f trecweb
    TrecWeb,
    /// -f wapo
    WashingtonPost,
    /// -f warc
    Warc,
}

/// Algorithm name.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Algorithm(String);

impl From<&str> for Algorithm {
    fn from(algorithm: &str) -> Self {
        Self(String::from(algorithm))
    }
}

impl fmt::Display for Algorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for Algorithm {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

/// Posting list encoding name.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Encoding(pub String);

impl From<&str> for Encoding {
    fn from(encoding: &str) -> Self {
        Self(String::from(encoding))
    }
}

impl fmt::Display for Encoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for Encoding {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

/// Field to use when using TREC topic format.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TopicField {
    /// Field `<title>`
    Title,
    /// Field `<desc>`
    Desc,
    /// Field `<narr>`
    Narr,
}

impl fmt::Display for TopicField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Title => write!(f, "title"),
            Self::Desc => write!(f, "desc"),
            Self::Narr => write!(f, "narr"),
        }
    }
}

/// File with query topics.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Topics {
    /// Colon-delimited query format.
    Simple {
        /// File path.
        path: PathBuf,
    },
    /// TREC format
    Trec {
        /// File path.
        path: PathBuf,
        /// TREC field to use.
        field: TopicField,
    },
}

/// Collection built before experiments.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Collection {
    /// Name indentifier.
    pub name: String,
    /// Type of collection format.
    pub kind: CollectionKind,
    /// Directory where the collection resides.
    pub input_dir: PathBuf,
    /// Basename for forward index.
    pub fwd_index: PathBuf,
    /// Basename for inverted index.
    pub inv_index: PathBuf,
    /// List of encodings with which to compress the inverted index.
    pub encodings: Vec<Encoding>,
}

/// Type of experiment.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RunKind {
    /// Query effectiveness evaluation.
    Evaluate {
        /// Path to query relevance file in TREC format.
        qrels: PathBuf,
    },
    /// Query speed performance.
    Benchmark,
}

/// An experimental run.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Run {
    /// Collection name.
    pub collection: String,
    /// Collection format.
    pub kind: RunKind,
    /// A list of posting list encodings.
    pub encodings: Vec<Encoding>,
    /// A list of query processing algorithms.
    pub algorithms: Vec<Algorithm>,
    /// A basename for output files.
    pub output: PathBuf,
    /// A list of topic/query files.
    pub topics: Vec<Topics>,
}

mod serde_url {
    use serde::{de, Deserializer, Serializer};
    use std::fmt;
    use url::Url;

    struct UrlVisitor;

    impl<'de> de::Visitor<'de> for UrlVisitor {
        type Value = Url;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("an URL")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let url = Url::parse(value).map_err(|err| E::custom(err.to_string()))?;
            Ok(url)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Url, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(UrlVisitor)
    }

    pub fn serialize<S>(url: &Url, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(url.as_str())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_yaml;

    #[test]
    fn test_parse_source() -> Result<(), serde_yaml::Error> {
        let source: Source = serde_yaml::from_str(
            "git:
  branch: master
  url: https://github.com/pisa-engine/pisa.git",
        )?;
        assert_eq!(
            source,
            Source::Git {
                branch: "master".to_string(),
                url: Url::parse("https://github.com/pisa-engine/pisa.git").unwrap()
            }
        );

        let source: Source = serde_yaml::from_str("path: /path/to/bin")?;
        assert_eq!(source, Source::Path(PathBuf::from("/path/to/bin")));

        let source: Source = serde_yaml::from_str("docker: tag")?;
        assert_eq!(source, Source::Docker(String::from("tag")));

        Ok(())
    }

    #[test]
    fn test_parse_collection_kind() -> Result<(), serde_yaml::Error> {
        assert_eq!(
            serde_yaml::from_str::<CollectionKind>("trec-web")?,
            CollectionKind::TrecWeb
        );
        assert_eq!(
            serde_yaml::from_str::<CollectionKind>("warc")?,
            CollectionKind::Warc
        );
        assert_eq!(
            serde_yaml::from_str::<CollectionKind>("washington-post")?,
            CollectionKind::WashingtonPost
        );
        Ok(())
    }

    #[test]
    fn test_parse_topic() -> Result<(), serde_yaml::Error> {
        assert_eq!(
            serde_yaml::from_str::<Topics>(
                "kind: simple
path: /path/to/topics"
            )?,
            Topics::Simple {
                path: PathBuf::from("/path/to/topics")
            }
        );
        assert_eq!(
            serde_yaml::from_str::<Topics>(
                "kind: trec
field: title
path: /path/to/topics"
            )?,
            Topics::Trec {
                field: TopicField::Title,
                path: PathBuf::from("/path/to/topics")
            }
        );
        Ok(())
    }

    #[test]
    fn test_parse_collection() -> Result<(), serde_yaml::Error> {
        assert_eq!(
            serde_yaml::from_str::<Collection>(
                "name: wapo
kind: washington-post
input_dir: /path/to/input
fwd_index: /path/to/fwd
inv_index: /path/to/inv
encodings:
  - block_simdbp
  - ef"
            )?,
            Collection {
                name: String::from("wapo"),
                kind: CollectionKind::WashingtonPost,
                input_dir: PathBuf::from("/path/to/input"),
                fwd_index: PathBuf::from("/path/to/fwd"),
                inv_index: PathBuf::from("/path/to/inv"),
                encodings: vec![Encoding::from("block_simdbp"), Encoding::from("ef")],
            }
        );
        Ok(())
    }

    #[test]
    fn test_parse_run() -> Result<(), serde_yaml::Error> {
        assert_eq!(
            serde_yaml::from_str::<Run>(
                "collection: wapo
kind:
  evaluate:
    qrels: /path/to/qrels
encodings:
  - block_simdbp
  - ef
algorithms:
  - and
  - wand
output: /path/to/output
topics:
  - kind: simple
    path: /path/to/simple/topics
  - kind: trec
    field: narr
    path: /path/to/trec/topics"
            )?,
            Run {
                collection: String::from("wapo"),
                kind: RunKind::Evaluate {
                    qrels: PathBuf::from("/path/to/qrels")
                },
                encodings: vec![Encoding::from("block_simdbp"), Encoding::from("ef")],
                algorithms: vec![Algorithm::from("and"), Algorithm::from("wand")],
                topics: vec![
                    Topics::Simple {
                        path: PathBuf::from("/path/to/simple/topics")
                    },
                    Topics::Trec {
                        field: TopicField::Narr,
                        path: PathBuf::from("/path/to/trec/topics")
                    },
                ],
                output: "/path/to/output".into()
            }
        );
        Ok(())
    }
}
