//! This module contains all the config definitions that are deserialized
//! from a YAML configuration file.

use crate::{CommandDebug, Error, Executor};
use boolinator::Boolinator;
use failure::ResultExt;
use log::warn;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::{Into, TryFrom};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;
use std::{fmt, fs};
use strum_macros::{Display, EnumIter, EnumString};

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

#[cfg_attr(tarpaulin, skip)]
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

/// Represents a variable passed to `CMake`, such as `-DCMAKE_BUILD_TYPE:BOOL=OFF`,
/// where `:BOOL` is optional.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct CMakeVar {
    pub(crate) name: String,
    pub(crate) typedef: Option<String>,
    pub(crate) value: String,
}

impl FromStr for CMakeVar {
    type Err = Error;
    fn from_str(var: &str) -> Result<Self, Self::Err> {
        if let Some(pos) = var.find('=') {
            let (name_type, value) = var.split_at(pos);
            let pos = name_type.find(':').unwrap_or_else(|| name_type.len());
            let (name, typedef) = name_type.split_at(pos);
            Ok(Self {
                name: String::from(name),
                typedef: if typedef.is_empty() {
                    None
                } else {
                    Some(String::from(&typedef[1..]))
                },
                value: String::from(&value[1..]),
            })
        } else {
            Err(Error::from("CMake var definition must contain `=`."))
        }
    }
}

impl TryFrom<&str> for CMakeVar {
    type Error = Error;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl TryFrom<String> for CMakeVar {
    type Error = Error;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        value[..].parse()
    }
}

impl fmt::Display for CMakeVar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}={}",
            self.name,
            self.typedef
                .as_ref()
                .map_or_else(String::new, |t| format!(":{}", t)),
            self.value
        )
    }
}

impl Into<String> for CMakeVar {
    fn into(self) -> String {
        format!("{}", self)
    }
}

/// Main config interface.
pub trait Config {
    /// All relative paths will fall back on to this directory.
    fn workdir(&self) -> &Path;
    /// Source of the PISA tools.
    fn source(&self) -> &Source;
    /// List of collections.
    fn collections(&self) -> &[Collection];
    /// List of experiments.
    fn runs(&self) -> &[Run];
    /// Disable a particular stage.
    fn disable(&mut self, stage: Stage);
    /// Returns `true` if a given stage is effectively enabled.
    fn enabled(&self, stage: Stage) -> bool;
    /// Construct an executor for a set of PISA tools.
    fn executor(&self) -> Result<Executor, Error>;
    /// Use `--scorer`. `false` for legacy PISA code before `ql3`.
    fn use_scorer(&self) -> bool;
    /// Clean up before running: remove work dir.
    fn clean(&self) -> bool;

    /// Retrieve a collection at a given index.
    ///
    /// # Panics
    ///
    /// Panics when the index is out of bounds.
    fn collection(&self, idx: usize) -> &Collection {
        &self.collections()[idx]
    }

    /// Retrieve a run at a given index.
    ///
    /// # Panics
    ///
    /// Panics when the index is out of bounds.
    fn run(&self, idx: usize) -> &Run {
        &self.runs()[idx]
    }
}

/// Marker trait to signify that the paths are resolved with respect to the work dir.
pub trait Resolved {}

/// Main config.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct RawConfig {
    /// All relative paths will fall back on to this directory.
    pub workdir: PathBuf,
    /// Source of the PISA tools.
    #[serde(default)]
    pub source: Source,
    /// List of collections.
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

struct GitRepository<'a> {
    dir: &'a Path,
}

impl<'a> GitRepository<'a> {
    fn open(dir: &'a Path) -> Self {
        Self { dir }
    }
    fn clone(url: &str, dir: &'a Path) -> Result<Self, Error> {
        Command::new("git")
            .arg("clone")
            .arg(url)
            .arg(dir)
            .log()
            .status()?
            .success()
            .ok_or("git-clone failed")?;
        Ok(Self { dir })
    }
    fn reset(&self) -> Result<(), Error> {
        Command::new("git")
            .current_dir(self.dir)
            .arg("reset")
            .arg("--hard")
            .log()
            .status()?
            .success()
            .ok_or(Error::from("git-reset failed"))
    }
    fn checkout(&self, branch: &str) -> Result<(), Error> {
        Command::new("git")
            .current_dir(self.dir)
            .arg("checkout")
            .arg(branch)
            .log()
            .status()?
            .success()
            .ok_or(Error::from("git-checkout failed"))
    }
}

struct CMake<'a> {
    cmake_vars: &'a [CMakeVar],
    dir: &'a Path,
}

impl<'a> CMake<'a> {
    fn new(cmake_vars: &'a [CMakeVar], dir: &'a Path) -> Self {
        Self { cmake_vars, dir }
    }
    fn configure(&self) -> Result<(), Error> {
        let mut cmd = Command::new("cmake");
        for var in self.cmake_vars {
            cmd.arg(format!("-D{}", var.to_string()));
        }
        cmd.arg("..")
            .current_dir(self.dir)
            .log()
            .status()?
            .success()
            .ok_or("cmake failed")?;
        Ok(())
    }
    fn build(&self) -> Result<(), Error> {
        process("cmake --build .")
            .current_dir(self.dir)
            .log()
            .status()?
            .success()
            .ok_or("cmake --build failed")?;
        Ok(())
    }
}

impl Config for RawConfig {
    fn workdir(&self) -> &Path {
        self.workdir.as_ref()
    }
    fn source(&self) -> &Source {
        &self.source
    }
    fn collections(&self) -> &[Collection] {
        &self.collections
    }
    fn runs(&self) -> &[Run] {
        &self.runs
    }
    fn disable(&mut self, stage: Stage) {
        self.stages.insert(stage, false);
    }
    fn enabled(&self, stage: Stage) -> bool {
        self.stages.get(&stage).cloned().unwrap_or(true)
    }
    fn use_scorer(&self) -> bool {
        self.use_scorer
    }
    fn clean(&self) -> bool {
        self.clean
    }

    fn executor(&self) -> Result<Executor, Error> {
        match &self.source {
            Source::System => Ok(Executor::new()),
            Source::Git {
                branch,
                url,
                cmake_vars,
            } => {
                let dir = self.workdir.join("pisa");
                let repo = if dir.exists() {
                    GitRepository::open(&dir)
                } else {
                    GitRepository::clone(&url, &dir)?
                };
                let build_dir = dir.join("build");
                fs::create_dir_all(&build_dir).context("Could not create build directory")?;
                if self.stages.get(&Stage::Compile).cloned().unwrap_or(true) {
                    repo.reset()?;
                    repo.checkout(&branch)?;
                    let cmake = CMake::new(&cmake_vars, &build_dir);
                    cmake.configure()?;
                    cmake.build()?;
                } else {
                    warn!("Compilation has been suppressed");
                }
                Ok(Executor::from(build_dir.join("bin"))?)
            }
            Source::Path(path) => Ok(Executor::from(path.to_path_buf())?),
            Source::Docker(_) => unimplemented!(),
        }
    }
}

/// This is simply a wrapper signifying that paths are resolved with respect to the work dir.
///
/// It is introduced so that it can be taken as argument to functions that assume
/// the paths are resolved.
#[derive(Debug)]
pub struct ResolvedPathsConfig(RawConfig);

fn resolve_path(workdir: &Path, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        workdir.join(path)
    }
}

impl ResolvedPathsConfig {
    /// Resolves all relative paths with respect to the work dir.
    pub fn from(config: RawConfig) -> Self {
        let workdir = config.workdir().to_path_buf();
        Self(RawConfig {
            collections: config
                .collections
                .into_iter()
                .map(|mut c| {
                    c.fwd_index = resolve_path(&workdir, c.fwd_index);
                    c.inv_index = resolve_path(&workdir, c.inv_index);
                    c
                })
                .collect(),
            runs: config
                .runs
                .into_iter()
                .map(|mut r| {
                    r.output = resolve_path(&workdir, r.output);
                    r.compare_with = r.compare_with.map(|p| resolve_path(&workdir, p));
                    r
                })
                .collect(),
            ..config
        })
    }
}

impl Config for ResolvedPathsConfig {
    fn workdir(&self) -> &Path {
        self.0.workdir()
    }
    fn source(&self) -> &Source {
        &self.0.source()
    }
    fn collections(&self) -> &[Collection] {
        &self.0.collections()
    }
    fn runs(&self) -> &[Run] {
        &self.0.runs()
    }
    fn disable(&mut self, stage: Stage) {
        self.0.disable(stage);
    }
    fn enabled(&self, stage: Stage) -> bool {
        self.0.enabled(stage)
    }
    fn use_scorer(&self) -> bool {
        self.0.use_scorer()
    }
    fn clean(&self) -> bool {
        self.0.clean()
    }
    fn executor(&self) -> Result<Executor, Error> {
        self.0.executor()
    }
}

impl Resolved for ResolvedPathsConfig {}

fn default_cmake_vars() -> Vec<CMakeVar> {
    vec![CMakeVar {
        name: "CMAKE_BUILD_TYPE".to_string(),
        typedef: None,
        value: "Release".to_string(),
    }]
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
        url: String,
        /// CMake flags, e.g., `-DPISA_ENABLE_TESTING=OFF`.
        #[serde(default = "default_cmake_vars")]
        cmake_vars: Vec<CMakeVar>,
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

/// Posting list encoding name.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Scorer(pub String);

impl From<&str> for Scorer {
    fn from(encoding: &str) -> Self {
        Self(String::from(encoding))
    }
}

impl fmt::Display for Scorer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for Scorer {
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

pub(crate) fn default_scorers() -> Vec<Scorer> {
    vec![Scorer::from("bm25")]
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
    /// List of scorers for which to build WAND data.
    #[serde(default = "default_scorers")]
    pub scorers: Vec<Scorer>,
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

pub(crate) fn default_scorer() -> Scorer {
    Scorer::from("bm25")
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
    /// Ranking scoring function.
    #[serde(default = "default_scorer")]
    pub scorer: Scorer,
    /// A path prefix to results of another run.
    #[serde(default)]
    pub compare_with: Option<PathBuf>,
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_yaml;

    #[test]
    fn test_cmake_var() {
        let var: CMakeVar = "CMAKE_BUILD_TYPE:BOOL=ON".parse().unwrap();
        assert_eq!(
            var,
            CMakeVar {
                name: "CMAKE_BUILD_TYPE".to_string(),
                typedef: Some("BOOL".to_string()),
                value: "ON".to_string()
            }
        );
    }

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
                url: "https://github.com/pisa-engine/pisa.git".to_string(),
                cmake_vars: vec![CMakeVar {
                    name: "CMAKE_BUILD_TYPE".to_string(),
                    typedef: None,
                    value: "Release".to_string(),
                }]
            }
        );

        let source: Source = serde_yaml::from_str(
            "git:
  branch: master
  url: https://github.com/pisa-engine/pisa.git
  cmake_vars:
    - CMAKE_BUILD_TYPE:BOOL=Release
    - PISA_ENABLE_TESTING=OFF
    - PISA_ENABLE_BENCHMARKING:BOOL=False",
        )?;
        assert_eq!(
            source,
            Source::Git {
                branch: "master".to_string(),
                url: "https://github.com/pisa-engine/pisa.git".to_string(),
                cmake_vars: vec![
                    CMakeVar {
                        name: "CMAKE_BUILD_TYPE".to_string(),
                        typedef: Some("BOOL".to_string()),
                        value: "Release".to_string(),
                    },
                    CMakeVar {
                        name: "PISA_ENABLE_TESTING".to_string(),
                        typedef: None,
                        value: "OFF".to_string(),
                    },
                    CMakeVar {
                        name: "PISA_ENABLE_BENCHMARKING".to_string(),
                        typedef: Some("BOOL".to_string()),
                        value: "False".to_string(),
                    },
                ]
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
                scorers: default_scorers(),
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
                output: "/path/to/output".into(),
                scorer: default_scorer(),
                compare_with: None,
            }
        );
        Ok(())
    }

    #[test]
    fn test_resolve_paths() {
        let config = RawConfig {
            workdir: PathBuf::from("/workdir"),
            collections: vec![
                Collection {
                    name: String::from("wapo"),
                    kind: CollectionKind::WashingtonPost,
                    input_dir: PathBuf::from("/path/to/input"),
                    fwd_index: PathBuf::from("/path/to/fwd"),
                    inv_index: PathBuf::from("/path/to/inv"),
                    encodings: vec![Encoding::from("ef")],
                    scorers: default_scorers(),
                },
                Collection {
                    name: String::from("wapo2"),
                    kind: CollectionKind::WashingtonPost,
                    input_dir: PathBuf::from("input"),
                    fwd_index: PathBuf::from("fwd"),
                    inv_index: PathBuf::from("inv"),
                    encodings: vec![Encoding::from("ef")],
                    scorers: default_scorers(),
                },
            ],
            runs: vec![
                Run {
                    collection: String::from("wapo"),
                    kind: RunKind::Benchmark,
                    encodings: vec![Encoding::from("ef")],
                    algorithms: vec![Algorithm::from("and")],
                    topics: vec![Topics::Simple {
                        path: PathBuf::from("/path/to/simple/topics"),
                    }],
                    output: "/path/to/output".into(),
                    scorer: default_scorer(),
                    compare_with: None,
                },
                Run {
                    collection: String::from("wapo"),
                    kind: RunKind::Benchmark,
                    encodings: vec![Encoding::from("ef")],
                    algorithms: vec![Algorithm::from("and")],
                    topics: vec![Topics::Simple {
                        path: PathBuf::from("/path/to/simple/topics"),
                    }],
                    output: "output".into(),
                    scorer: default_scorer(),
                    compare_with: Some(PathBuf::from("compare")),
                },
            ],
            source: Source::System,
            clean: true,
            ..RawConfig::default()
        };
        let config = ResolvedPathsConfig::from(config);
        assert_eq!(
            config.collection(0).fwd_index,
            PathBuf::from("/path/to/fwd")
        );
        assert_eq!(
            config.collection(0).inv_index,
            PathBuf::from("/path/to/inv")
        );
        assert_eq!(
            config.collection(1).fwd_index,
            PathBuf::from("/workdir/fwd")
        );
        assert_eq!(
            config.collection(1).inv_index,
            PathBuf::from("/workdir/inv")
        );
        assert_eq!(config.run(0).output, PathBuf::from("/path/to/output"));
        assert_eq!(config.run(1).output, PathBuf::from("/workdir/output"));
        assert_eq!(
            config.run(1).compare_with,
            Some(PathBuf::from("/workdir/compare"))
        );
        assert_eq!(config.source(), &Source::System);
        assert!(config.clean());
    }
}
