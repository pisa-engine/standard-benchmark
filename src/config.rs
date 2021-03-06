//! This module contains all the config definitions that are deserialized
//! from a YAML configuration file.

use crate::{CommandDebug, Error, Executor, RegressionMargin};
use boolinator::Boolinator;
use failure::{bail, format_err, ResultExt};
use itertools::iproduct;
use log::warn;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::convert::{Into, TryFrom};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;
use std::{fmt, fs, mem};
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
    /// Compare with a gold standard (if such is defined).
    #[strum(serialize = "compare")]
    Compare,
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
        (&value).parse()
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

/// Batch sizes for building index.
///
/// # Examples
///
/// By default, all are equal to 10,000.
/// ```
/// # use stdbench::config::BatchSizes;
/// let batch_sizes = BatchSizes::default();
/// assert_eq!(batch_sizes.parse, 10_000);
/// assert_eq!(batch_sizes.invert, 10_000);
/// ```
#[derive(Copy, Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct BatchSizes {
    /// Batch size for `parse_collection`.
    #[serde(default = "default_batch_size")]
    pub parse: usize,
    /// Batch size for `invert`.
    #[serde(default = "default_batch_size")]
    pub invert: usize,
}

fn default_batch_size() -> usize {
    10_000
}

impl Default for BatchSizes {
    fn default() -> Self {
        Self {
            parse: default_batch_size(),
            invert: default_batch_size(),
        }
    }
}

/// Thread counts for building index.
///
/// By default, all are equal to `None`, which will cause the tools to be called
/// without `--threads` parameter, and the thread pool will be calculated by TBB.
#[derive(Copy, Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct Threads {
    /// Thread count for `parse_collection`.
    #[serde(default)]
    pub parse: Option<usize>,
    /// Thread count for `invert`.
    #[serde(default)]
    pub invert: Option<usize>,
}

impl Default for Threads {
    fn default() -> Self {
        Self {
            parse: None,
            invert: None,
        }
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
    /// Batch size of a particular batched job.
    fn batch_sizes(&self) -> BatchSizes;
    /// Thread counts of a particular batched job.
    fn threads(&self) -> Threads;
    /// Performance regression margin.
    fn margin(&self) -> RegressionMargin;

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
///
/// # Global-Level Run Parameters
///
/// It is possible to define a global list of encodings and algorithms
/// to be used in all runs.
/// These values, if exist, act like defaults, which are overridden once
/// they appear in the run configuration.
/// On the other hand, the config validation step will fail if a value is absent
/// from both global and run configuration.
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
    /// Batch sizes.
    #[serde(default)]
    pub batch_sizes: BatchSizes,
    /// Thread counts.
    #[serde(default)]
    pub threads: Threads,
    #[serde(default)]
    /// A list of posting list encodings.
    pub encodings: Option<Vec<Encoding>>,
    #[serde(default)]
    /// A list of query processing algorithms.
    pub algorithms: Option<Vec<Algorithm>>,
    #[serde(default)]
    /// Performance regression margin.
    pub margin: RegressionMargin,
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
    fn build(&self, threads: usize) -> Result<(), Error> {
        process("cmake --build . -- -j")
            .arg(threads.to_string())
            .current_dir(self.dir)
            .log()
            .status()?
            .success()
            .ok_or("cmake --build failed")?;
        Ok(())
    }
}

fn update_repo(repo: &git2::Repository, refname: &str) -> Result<(), Error> {
    let mut oid: Option<git2::Oid> = None;
    {
        let mut cb = git2::RemoteCallbacks::new();
        cb.update_tips(|_, old, new| {
            if old.is_zero() {
                oid = Some(new);
            }
            true
        });
        let mut fo = git2::FetchOptions::new();
        fo.remote_callbacks(cb);
        let mut remote = repo.find_remote("origin")?;
        remote.fetch(&[refname], Some(&mut fo), None)?;
    }

    if let Some(oid) = oid {
        let obj = repo.find_object(oid, None)?;
        repo.checkout_tree(&obj, Some(git2::build::CheckoutBuilder::new().force()))?;
        return Ok(());
    }

    if let Ok(reference) = repo.resolve_reference_from_short_name(refname) {
        if reference.is_branch() {
            let origin_ref = repo
                .find_branch(refname, git2::BranchType::Local)?
                .upstream()?
                .into_reference();
            let origin_commit = repo.reference_to_annotated_commit(&origin_ref)?;
            repo.merge(
                &[&origin_commit],
                None,
                Some(git2::build::CheckoutBuilder::new().use_theirs(true).force()),
            )?;
        } else if reference.is_tag() {
            repo.checkout_tree(
                &reference.peel(git2::ObjectType::Any)?,
                Some(git2::build::CheckoutBuilder::new().force()),
            )?;
        } else {
            return Err(Error::from(format!(
                "Reference is not a tag or a branch: {}",
                reference.name().unwrap_or("invalid-utf8")
            )));
        }
    } else {
        let oid = git2::Oid::from_str(refname)?;
        let obj = repo.find_object(oid, None)?;
        repo.checkout_tree(&obj, Some(git2::build::CheckoutBuilder::new().force()))?;
    }
    Ok(())
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
    fn batch_sizes(&self) -> BatchSizes {
        self.batch_sizes
    }
    fn threads(&self) -> Threads {
        self.threads
    }
    fn margin(&self) -> RegressionMargin {
        self.margin
    }

    fn executor(&self) -> Result<Executor, Error> {
        match &self.source {
            Source::System => Ok(Executor::new()),
            Source::Git {
                branch,
                url,
                cmake_vars,
                local_path,
                compile_threads,
            } => {
                let dir = if local_path.is_absolute() {
                    local_path.to_path_buf()
                } else {
                    self.workdir.join(&local_path)
                };
                let repo = if dir.exists() {
                    git2::Repository::open(&dir)?
                } else {
                    git2::Repository::clone_recurse(&url, &dir).map_err(|_| "git-clone failed")?
                };
                let build_dir = dir.join("build");
                fs::create_dir_all(&build_dir).context("Could not create build directory")?;
                if self.stages.get(&Stage::Compile).cloned().unwrap_or(true) {
                    repo.checkout_head(Some(git2::build::CheckoutBuilder::new().force()))?;
                    update_repo(&repo, &branch)?;
                    let cmake = CMake::new(&cmake_vars, &build_dir);
                    cmake.configure()?;
                    cmake.build(*compile_threads)?;
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
pub struct ResolvedPathsConfig(pub RawConfig);

fn resolve_path(workdir: &Path, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        workdir.join(path)
    }
}

trait PathExists {
    fn exists_or(&self, message: &str) -> Result<(), Error>;
}

impl PathExists for Path {
    fn exists_or(&self, message: &str) -> Result<(), Error> {
        self.exists()
            .ok_or_else(|| format_err!("{}: {}", message, self.display()))?;
        Ok(())
    }
}

pub(crate) fn output_path_formatter<'a>(
    //base: &Path,
    algorithm: &'a Algorithm,
    encoding: &'a Encoding,
    topics_file_idx: usize,
    suffix: &'a str,
) -> impl Fn(&'a Path) -> PathBuf + 'a {
    move |path: &'a Path| -> PathBuf {
        format_output_path(path, algorithm, encoding, topics_file_idx, suffix)
    }
}

pub(crate) fn format_output_path(
    base: &Path,
    algorithm: &Algorithm,
    encoding: &Encoding,
    topics_file_idx: usize,
    suffix: &str,
) -> PathBuf {
    PathBuf::from(format!(
        "{}.{}.{}.{}.{}",
        base.display(),
        algorithm,
        encoding,
        topics_file_idx,
        suffix
    ))
}

impl ResolvedPathsConfig {
    fn resolve_run_with<'a>(
        workdir: &'a Path,
        algorithms: &'a Option<Vec<Algorithm>>,
        encodings: &'a Option<Vec<Encoding>>,
    ) -> impl 'a + FnMut(Run) -> Result<Run, failure::Error> {
        move |mut r: Run| {
            r.output = resolve_path(workdir, r.output);
            r.compare_with = r.compare_with.map(|p| resolve_path(&workdir, p));
            if r.algorithms.is_empty() {
                if let Some(algorithms) = algorithms {
                    r.algorithms.extend(algorithms.iter().cloned());
                } else {
                    bail!("Missing algorithms: {:?}", &r);
                }
            }
            if r.encodings.is_empty() {
                if let Some(encodings) = encodings {
                    r.encodings.extend(encodings.iter().cloned());
                } else {
                    bail!("Missing encodings: {:?}", &r);
                }
            }
            Ok(r)
        }
    }

    fn resolve_collection_with<'a>(
        workdir: &'a Path,
        encodings: &'a Option<Vec<Encoding>>,
    ) -> impl 'a + FnMut(Collection) -> Result<Collection, failure::Error> {
        move |mut c: Collection| {
            c.fwd_index = resolve_path(&workdir, c.fwd_index);
            c.inv_index = resolve_path(&workdir, c.inv_index);
            if c.encodings.is_empty() {
                if let Some(encodings) = encodings {
                    c.encodings.extend(encodings.iter().cloned());
                } else {
                    bail!("Missing encodings: {:?}", &c);
                }
            }
            Ok(c)
        }
    }

    /// Resolves all relative paths with respect to the work dir.
    pub fn from(mut config: RawConfig) -> Result<Self, Error> {
        let algorithms = mem::replace(&mut config.algorithms, None);
        let encodings = mem::replace(&mut config.encodings, None);
        let workdir = config.workdir().to_path_buf();
        let resolve_run = Self::resolve_run_with(&workdir, &algorithms, &encodings);
        let runs: Result<_, _> = config.runs.into_iter().map(resolve_run).collect();
        let resolve_coll = Self::resolve_collection_with(&workdir, &encodings);
        let collections: Result<_, _> = config.collections.into_iter().map(resolve_coll).collect();
        let config = Self(RawConfig {
            collections: collections?,
            runs: runs?,
            ..config
        });
        config.verify()?;
        Ok(config)
    }

    fn verify(&self) -> Result<(), Error> {
        let mut collection_names: HashSet<&str> = HashSet::new();
        for collection in self.collections() {
            collection.input_dir.as_ref().map_or_else(
                || collection.verify_index_exists(),
                |p| p.exists_or("Collection dir not found"),
            )?;
            collection_names.insert(&collection.name);
        }
        for run in self.runs() {
            collection_names
                .contains(&run.collection.as_ref())
                .ok_or_else(|| format_err!("Collection not defined: {}", run.collection))?;
            if let RunKind::Evaluate { qrels } = &run.kind {
                qrels.exists_or("Qrels file not found")?;
            }
            for topics in &run.topics {
                let topics_path = match topics {
                    Topics::Trec { path, .. } | Topics::Simple { path } => path,
                };
                topics_path.exists_or("Topics not found")?;
            }
            if let Some(compare_with) = &run.compare_with {
                for (algorithm, encoding, topics_idx) in
                    iproduct!(&run.algorithms, &run.encodings, 0..run.topics.len())
                {
                    match run.kind {
                        RunKind::Evaluate { .. } => format_output_path(
                            compare_with,
                            algorithm,
                            encoding,
                            topics_idx,
                            "trec_eval",
                        )
                        .exists_or("Missing baseline")?,
                        RunKind::Benchmark => format_output_path(
                            compare_with,
                            algorithm,
                            encoding,
                            topics_idx,
                            "bench",
                        )
                        .exists_or("Missing baseline")?,
                    }
                }
            }
        }
        Ok(())
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
    fn batch_sizes(&self) -> BatchSizes {
        self.0.batch_sizes()
    }
    fn threads(&self) -> Threads {
        self.0.threads()
    }
    fn margin(&self) -> RegressionMargin {
        self.0.margin()
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

fn default_local_path() -> PathBuf {
    PathBuf::from("pisa")
}

fn default_no_threads() -> usize {
    1_usize
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
        /// Local path to pull the code to other than `workdir/pisa`.
        /// Partial paths will be rooted at the working directory.
        #[serde(default = "default_local_path")]
        local_path: PathBuf,
        /// Use this many threads when calling `make`.
        #[serde(default = "default_no_threads")]
        compile_threads: usize,
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
    /// Robust04 collection. Uses `-f trectext`.
    Robust,
    /// NYT collection. Uses `-f plaintext`.
    /// This works for a pre-processed file: originally, NYT is in XML format,
    /// but here we assume it's already in plain. It will look for `*.plain`
    /// file in the directory.
    NewYorkTimes,
    /// -f wapo
    WashingtonPost,
    /// -f warc
    Warc,
}

/// Algorithm name.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
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
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
pub struct Encoding(pub String);

impl FromStr for Encoding {
    type Err = Error;
    fn from_str(encoding: &str) -> Result<Self, Self::Err> {
        Ok(Self::from(encoding))
    }
}

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
    #[serde(default)]
    pub input_dir: Option<PathBuf>,
    /// Basename for forward index.
    pub fwd_index: PathBuf,
    /// Basename for inverted index.
    pub inv_index: PathBuf,
    /// List of encodings with which to compress the inverted index.
    #[serde(default)]
    pub encodings: Vec<Encoding>,
    /// List of scorers for which to build WAND data.
    #[serde(default = "default_scorers")]
    pub scorers: Vec<Scorer>,
}

impl Collection {
    fn with_appended<P: AsRef<Path>>(path: P, extension: &str) -> PathBuf {
        let mut file_name = path.as_ref().file_name().unwrap().to_os_string();
        file_name.push(extension);
        path.as_ref().with_file_name(file_name)
    }
    pub(crate) fn documents(&self) -> PathBuf {
        Self::with_appended(&self.fwd_index, ".documents")
    }
    pub(crate) fn terms(&self) -> PathBuf {
        Self::with_appended(&self.fwd_index, ".terms")
    }
    pub(crate) fn document_lexicon(&self) -> PathBuf {
        Self::with_appended(&self.fwd_index, ".doclex")
    }
    pub(crate) fn term_lexicon(&self) -> PathBuf {
        Self::with_appended(&self.fwd_index, ".termlex")
    }
    pub(crate) fn wand(&self) -> PathBuf {
        Self::with_appended(&self.inv_index, ".wand")
    }
    pub(crate) fn enc_index(&self, encoding: &Encoding) -> PathBuf {
        Self::with_appended(&self.inv_index, &format!(".{}", encoding))
    }
    fn verify_index_exists(&self) -> Result<(), Error> {
        self.document_lexicon()
            .exists()
            .ok_or("Document lexicon missing")?;
        self.term_lexicon().exists().ok_or("Term lexicon missing")?;
        self.wand().exists().ok_or("WAND data missing")?;
        for encoding in &self.encodings {
            self.enc_index(encoding)
                .exists()
                .ok_or_else(|| format!("Missing index encoded with: {}", encoding))?;
        }
        Ok(())
    }
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
    #[serde(default)]
    pub encodings: Vec<Encoding>,
    /// A list of query processing algorithms.
    #[serde(default)]
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
    use crate::tests::mkfiles;
    use rstest::{fixture, rstest};
    use serde_yaml;
    use tempdir::TempDir;

    #[test]
    fn test_true_default() {
        assert!(true_default());
    }

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
        assert_eq!(
            &format!(
                "{}",
                CMakeVar {
                    name: "CMAKE_BUILD_TYPE".to_string(),
                    typedef: Some("BOOL".to_string()),
                    value: "ON".to_string()
                }
            ),
            "CMAKE_BUILD_TYPE:BOOL=ON"
        );
        assert_eq!(
            &format!(
                "{}",
                CMakeVar {
                    name: "CMAKE_BUILD_TYPE".to_string(),
                    typedef: None,
                    value: "ON".to_string()
                }
            ),
            "CMAKE_BUILD_TYPE=ON"
        );
        let strvar: String = CMakeVar {
            name: "CMAKE_BUILD_TYPE".to_string(),
            typedef: Some("BOOL".to_string()),
            value: "ON".to_string(),
        }
        .into();
        assert_eq!(&strvar, "CMAKE_BUILD_TYPE:BOOL=ON");
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
                }],
                local_path: PathBuf::from("pisa"),
                compile_threads: 1_usize,
            }
        );

        let source: Source = serde_yaml::from_str(
            "git:
  branch: master
  url: https://github.com/pisa-engine/pisa.git
  cmake_vars:
    - CMAKE_BUILD_TYPE:BOOL=Release
    - PISA_ENABLE_TESTING=OFF
    - PISA_ENABLE_BENCHMARKING:BOOL=False
  local_path: pisa-master
  compile_threads: 2",
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
                ],
                local_path: PathBuf::from("pisa-master"),
                compile_threads: 2,
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
                input_dir: Some(PathBuf::from("/path/to/input")),
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

    #[fixture]
    fn tmp() -> TempDir {
        TempDir::new("").expect("Unable to create a temporary directory")
    }

    #[allow(dead_code)]
    struct ResolveFixture {
        tmp: TempDir,
        workdir: PathBuf,
        input: PathBuf,
        qrels: PathBuf,
        topics: PathBuf,
        bench: PathBuf,
        trec: PathBuf,
        config: RawConfig,
    }

    #[fixture]
    #[allow(clippy::needless_pass_by_value)]
    fn resolve_fixture(tmp: TempDir) -> ResolveFixture {
        mkfiles(
            tmp.path(),
            &[
                "input",
                "qrels",
                "simple_topics",
                "compare.and.ef.0.bench",
                "compare.and.ef.0.trec_eval",
            ],
        )
        .expect("Unable to create temporary files");
        let workdir = tmp.path().to_path_buf();
        let config = RawConfig {
            workdir: workdir.clone(),
            collections: vec![
                Collection {
                    name: String::from("wapo"),
                    kind: CollectionKind::WashingtonPost,
                    input_dir: Some(workdir.join("input")),
                    fwd_index: workdir.join("fwd"),
                    inv_index: workdir.join("inv"),
                    encodings: vec![Encoding::from("ef")],
                    scorers: default_scorers(),
                },
                Collection {
                    name: String::from("wapo2"),
                    kind: CollectionKind::WashingtonPost,
                    input_dir: Some(workdir.join("input")),
                    fwd_index: workdir.join("fwd"),
                    inv_index: workdir.join("inv"),
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
                        path: workdir.join("simple_topics"),
                    }],
                    output: workdir.join("output"),
                    scorer: default_scorer(),
                    compare_with: None,
                },
                Run {
                    collection: String::from("wapo"),
                    kind: RunKind::Benchmark,
                    encodings: vec![Encoding::from("ef")],
                    algorithms: vec![Algorithm::from("and")],
                    topics: vec![Topics::Simple {
                        path: workdir.join("simple_topics"),
                    }],
                    output: "output".into(),
                    scorer: default_scorer(),
                    compare_with: Some(workdir.join("compare")),
                },
                Run {
                    collection: String::from("wapo"),
                    kind: RunKind::Evaluate {
                        qrels: workdir.join("qrels"),
                    },
                    encodings: vec![Encoding::from("ef")],
                    algorithms: vec![Algorithm::from("and")],
                    topics: vec![Topics::Simple {
                        path: workdir.join("simple_topics"),
                    }],
                    output: "output".into(),
                    scorer: default_scorer(),
                    compare_with: Some(tmp.path().join("compare")),
                },
            ],
            source: Source::System,
            clean: true,
            ..RawConfig::default()
        };
        ResolveFixture {
            input: tmp.path().join("input"),
            qrels: tmp.path().join("qrels"),
            topics: tmp.path().join("simple_topics"),
            bench: tmp.path().join("compare.and.ef.0.bench"),
            trec: tmp.path().join("compare.and.ef.0.trec_eval"),
            config,
            tmp,
            workdir,
        }
    }

    #[rstest]
    #[allow(clippy::needless_pass_by_value)]
    fn test_resolve_paths(resolve_fixture: ResolveFixture) {
        let workdir = resolve_fixture.workdir;
        let config = ResolvedPathsConfig::from(resolve_fixture.config).unwrap();
        assert_eq!(config.collection(0).fwd_index, workdir.join("fwd"));
        assert_eq!(config.collection(0).inv_index, workdir.join("inv"));
        assert_eq!(config.collection(1).fwd_index, workdir.join("fwd"));
        assert_eq!(config.collection(1).inv_index, workdir.join("inv"));
        assert_eq!(config.run(0).output, workdir.join("output"));
        assert_eq!(config.run(1).output, workdir.join("output"));
        assert_eq!(config.run(1).compare_with, Some(workdir.join("compare")));
        assert_eq!(config.source(), &Source::System);
        assert!(config.clean());
    }

    #[rstest]
    #[allow(clippy::needless_pass_by_value)]
    fn test_resolve_paths_global_algorithms_and_encodings(mut resolve_fixture: ResolveFixture) {
        for run in &mut resolve_fixture.config.runs {
            run.encodings.clear();
        }
        for coll in &mut resolve_fixture.config.collections {
            coll.encodings.clear();
        }
        resolve_fixture.config.encodings = Some(vec![Encoding::from("ef")]);
        resolve_fixture.config.algorithms = Some(vec![Algorithm::from("and")]);
        let workdir = resolve_fixture.workdir;
        let config = ResolvedPathsConfig::from(resolve_fixture.config).unwrap();
        assert_eq!(config.collection(0).fwd_index, workdir.join("fwd"));
        assert_eq!(config.collection(0).inv_index, workdir.join("inv"));
        assert_eq!(config.collection(1).fwd_index, workdir.join("fwd"));
        assert_eq!(config.collection(1).inv_index, workdir.join("inv"));
        assert_eq!(config.run(0).output, workdir.join("output"));
        assert_eq!(config.run(1).output, workdir.join("output"));
        assert_eq!(config.run(1).compare_with, Some(workdir.join("compare")));
        assert_eq!(config.source(), &Source::System);
        assert!(config.clean());
    }

    #[rstest]
    #[allow(clippy::needless_pass_by_value)]
    fn test_resolve_paths_missing_algorithms(mut resolve_fixture: ResolveFixture) {
        for run in &mut resolve_fixture.config.runs {
            run.algorithms.clear();
        }
        assert!(ResolvedPathsConfig::from(resolve_fixture.config)
            .err()
            .unwrap()
            .to_string()
            .starts_with("Missing algorithms"));
    }

    #[rstest]
    #[allow(clippy::needless_pass_by_value)]
    fn test_resolve_paths_missing_encodings(mut resolve_fixture: ResolveFixture) {
        for run in &mut resolve_fixture.config.runs {
            run.encodings.clear();
        }
        assert!(ResolvedPathsConfig::from(resolve_fixture.config)
            .err()
            .unwrap()
            .to_string()
            .starts_with("Missing encodings"));
    }

    #[rstest]
    #[allow(clippy::needless_pass_by_value)]
    fn test_resolve_paths_missing_encodings_in_coll(mut resolve_fixture: ResolveFixture) {
        for coll in &mut resolve_fixture.config.collections {
            coll.encodings.clear();
        }
        assert!(ResolvedPathsConfig::from(resolve_fixture.config)
            .err()
            .unwrap()
            .to_string()
            .starts_with("Missing encodings"));
    }

    #[rstest]
    #[allow(clippy::needless_pass_by_value)]
    fn test_resolve_paths_external_index(mut resolve_fixture: ResolveFixture) {
        let index_dir = resolve_fixture.workdir.join("external");
        fs::create_dir(&index_dir).unwrap();
        mkfiles(
            &index_dir,
            &["fwd.doclex", "fwd.termlex", "inv", "inv.wand", "inv.ef"],
        )
        .expect("Unable to create temporary files");
        mem::replace(
            &mut resolve_fixture.config.collections[0],
            Collection {
                name: String::from("wapo"),
                kind: CollectionKind::WashingtonPost,
                input_dir: None,
                fwd_index: index_dir.join("fwd"),
                inv_index: index_dir.join("inv"),
                encodings: vec![Encoding::from("ef")],
                scorers: default_scorers(),
            },
        );
        let config = ResolvedPathsConfig::from(resolve_fixture.config).unwrap();
        assert_eq!(config.collection(0).fwd_index, index_dir.join("fwd"));
        assert_eq!(config.collection(0).inv_index, index_dir.join("inv"));
    }

    #[test]
    fn test_parse_batch_sizes() -> Result<(), serde_yaml::Error> {
        assert_eq!(
            serde_yaml::from_str::<BatchSizes>(
                "parse: 10
invert: 9"
            )?,
            BatchSizes {
                parse: 10,
                invert: 9
            }
        );
        assert_eq!(
            serde_yaml::from_str::<BatchSizes>("parse: 10")?,
            BatchSizes {
                parse: 10,
                invert: 10_000
            }
        );
        assert_eq!(
            serde_yaml::from_str::<BatchSizes>("invert: 9")?,
            BatchSizes {
                parse: 10_000,
                invert: 9
            }
        );
        Ok(())
    }

    #[test]
    fn test_parse_threads() -> Result<(), serde_yaml::Error> {
        assert_eq!(
            serde_yaml::from_str::<Threads>(
                "parse: 10
invert: 9"
            )?,
            Threads {
                parse: Some(10),
                invert: Some(9)
            }
        );
        assert_eq!(
            serde_yaml::from_str::<Threads>("parse: 10")?,
            Threads {
                parse: Some(10),
                invert: None
            }
        );
        assert_eq!(
            serde_yaml::from_str::<Threads>("invert: 9")?,
            Threads {
                parse: None,
                invert: Some(9)
            }
        );
        Ok(())
    }
}
