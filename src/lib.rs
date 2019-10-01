#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![deny(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

//! This library contains all necessary tools to run a PISA benchmark
//! on a collection of a significant size.

use lazy_static::lazy_static;
use log::debug;
use regex::Regex;
use std::path::Path;
use std::process::Command;
use std::{fmt, fs};

pub mod config;
pub use config::{
    Algorithm, CMakeVar, Collection, Config, Encoding, RawConfig, Resolved, ResolvedPathsConfig,
    Run, Scorer, Source, Stage,
};

mod executor;
pub use executor::Executor;

pub mod build;

mod error;
pub use error::Error;

pub mod run;

/// If the parent directory of `path` does not exist, create it.
///
/// # Examples
///
/// ```
/// # extern crate stdbench;
/// # extern crate tempdir;
/// # use stdbench::*;
/// # use std::path::Path;
/// # use tempdir::TempDir;
/// assert_eq!(
///     ensure_parent_exists(Path::new("/")),
///     Err(Error::from("cannot access parent of path: /"))
/// );
///
/// let tmp = TempDir::new("parent_exists").unwrap();
/// let parent = tmp.path().join("parent");
/// let child = parent.join("child");
/// assert!(ensure_parent_exists(child.as_path()).is_ok());
/// assert!(parent.exists());
/// ```
pub fn ensure_parent_exists(path: &Path) -> Result<(), Error> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("cannot access parent of path: {}", path.display()))?;
    fs::create_dir_all(parent)?;
    Ok(())
}

/// Extension trait for `std::process::Command` that allows to format and log the command.
pub trait CommandDebug: fmt::Debug {
    /// Log the command as DEBUG.
    fn log(&mut self) -> &mut Self {
        debug!("[EXEC] {}", self.to_string());
        self
    }

    /// Return `String` representation.
    #[cfg_attr(tarpaulin, skip)] // False-positive for the macro
    fn to_string(&self) -> String {
        lazy_static! {
            static ref RE: Regex = Regex::new(r#""([^"^\s]+)""#).unwrap();
        }
        RE.captures_iter(format!("{:?}", self).as_ref())
            .map(|arg| arg.get(1).unwrap().as_str())
            .collect::<Vec<_>>()
            .join(" ")
    }
}

impl CommandDebug for Command {}

#[cfg(test)]
mod tests {
    extern crate tempdir;

    use super::*;
    use config::*;
    use std::collections::HashMap;
    use std::env::{set_var, var};
    use std::fs::File;
    use std::fs::Permissions;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use tempdir::TempDir;

    pub(crate) fn mkfiles(root: &Path, paths: &[&str]) -> Result<(), Error> {
        for path in paths {
            if path.ends_with('/') {
                fs::create_dir(root.join(path))?;
            } else {
                File::create(root.join(path))?;
            }
        }
        Ok(())
    }

    pub(crate) struct MockSetup {
        pub config: ResolvedPathsConfig,
        pub executor: Executor,
        pub programs: HashMap<&'static str, PathBuf>,
        pub outputs: HashMap<&'static str, PathBuf>,
        pub term_count: usize,
    }

    pub(crate) fn mock_program<P: AsRef<Path>>(
        bin: P,
        setup: &mut MockSetup,
        program: &'static str,
        mode: EchoMode,
    ) {
        let path = bin.as_ref().join(format!("{}.out", program));
        let prog = bin.as_ref().join(program);
        make_echo(&prog, &path, mode).unwrap();
        setup.outputs.insert(program, path);
        setup.programs.insert(program, prog);
    }

    pub(crate) fn mock_set_up(tmp: &TempDir) -> MockSetup {
        use EchoMode::Redirect;
        mkfiles(tmp.path(), &["coll/", "gov2/", "cw09b/", "qrels", "topics"]).unwrap();
        let collections = vec![
            Collection {
                name: "wapo".to_string(),
                kind: CollectionKind::WashingtonPost,
                input_dir: tmp.path().join("coll"),
                fwd_index: tmp.path().join("fwd"),
                inv_index: tmp.path().join("inv"),
                encodings: vec!["block_simdbp".into(), "block_qmx".into()],
                scorers: default_scorers(),
            },
            Collection {
                name: "gov2".to_string(),
                kind: CollectionKind::TrecWeb,
                input_dir: tmp.path().join("gov2"),
                fwd_index: tmp.path().join("gov2/fwd"),
                inv_index: tmp.path().join("gov2/inv"),
                encodings: vec!["block_simdbp".into(), "block_qmx".into()],
                scorers: default_scorers(),
            },
            Collection {
                name: "cw09b".to_string(),
                kind: CollectionKind::Warc,
                input_dir: tmp.path().join("cw09b"),
                fwd_index: tmp.path().join("cw09b/fwd"),
                inv_index: tmp.path().join("cw09b/inv"),
                encodings: vec!["block_simdbp".into(), "block_qmx".into()],
                scorers: default_scorers(),
            },
        ];
        let runs = vec![
            Run {
                collection: "wapo".into(),
                kind: RunKind::Evaluate {
                    qrels: tmp.path().join("qrels"),
                },
                encodings: vec!["block_simdbp".into(), "block_qmx".into()],
                algorithms: vec!["wand".into(), "maxscore".into()],
                topics: vec![Topics::Trec {
                    path: tmp.path().join("topics"),
                    field: TopicField::Title,
                }],
                output: tmp.path().join("output.trec"),
                scorer: default_scorer(),
                compare_with: None,
            },
            Run {
                collection: "wapo".into(),
                kind: RunKind::Evaluate {
                    qrels: tmp.path().join("qrels"),
                },
                encodings: vec!["block_simdbp".into()],
                algorithms: vec!["wand".into(), "maxscore".into()],
                topics: vec![Topics::Simple {
                    path: tmp.path().join("topics"),
                }],
                output: tmp.path().join("output.trec"),
                scorer: default_scorer(),
                compare_with: None,
            },
            Run {
                collection: "wapo".into(),
                kind: RunKind::Benchmark,
                encodings: vec!["block_simdbp".into()],
                algorithms: vec!["wand".into(), "maxscore".into()],
                topics: vec![Topics::Trec {
                    path: tmp.path().join("topics"),
                    field: TopicField::Title,
                }],
                output: tmp.path().join("bench.json"),
                scorer: default_scorer(),
                compare_with: None,
            },
        ];

        let bin = tmp.path().join("bin");
        fs::create_dir(&bin).expect("Could not create bin directory");

        let config = ResolvedPathsConfig::from(RawConfig {
            workdir: tmp.path().to_path_buf(),
            source: Source::Path(bin.clone()),
            use_scorer: true,
            collections,
            runs,
            ..RawConfig::default()
        })
        .unwrap();

        let data_dir = tmp.path().join("coll").join("data");
        fs::create_dir_all(&data_dir).unwrap();
        std::fs::File::create(data_dir.join("f.jl")).unwrap();
        let executor = config.executor().unwrap();

        let gov2_dir = tmp.path().join("gov2");
        let gov2_0_dir = gov2_dir.join("GX000");
        let gov2_1_dir = gov2_dir.join("GX001");
        fs::create_dir_all(&gov2_0_dir).unwrap();
        fs::create_dir_all(&gov2_1_dir).unwrap();
        fs::File::create(gov2_0_dir.join("00.gz")).unwrap();
        fs::File::create(gov2_0_dir.join("01.gz")).unwrap();
        fs::File::create(gov2_1_dir.join("02.gz")).unwrap();
        fs::File::create(gov2_1_dir.join("03.gz")).unwrap();

        let cw_dir = tmp.path().join("cw09b");
        let cw_0_dir = cw_dir.join("en0000");
        let cw_1_dir = cw_dir.join("en0001");
        fs::create_dir_all(&cw_0_dir).unwrap();
        fs::create_dir_all(&cw_1_dir).unwrap();
        fs::File::create(cw_0_dir.join("00.warc.gz")).unwrap();
        fs::File::create(cw_0_dir.join("01.warc.gz")).unwrap();
        fs::File::create(cw_1_dir.join("02.warc.gz")).unwrap();
        fs::File::create(cw_1_dir.join("03.warc.gz")).unwrap();

        let mut mock_setup = MockSetup {
            config,
            executor,
            programs: HashMap::new(),
            outputs: HashMap::new(),
            term_count: 3,
        };

        mock_program(&bin, &mut mock_setup, "parse_collection", Redirect);
        mock_program(&bin, &mut mock_setup, "invert", Redirect);
        mock_program(&bin, &mut mock_setup, "create_freq_index", Redirect);
        mock_program(&bin, &mut mock_setup, "create_wand_data", Redirect);
        mock_program(&bin, &mut mock_setup, "lexicon", Redirect);
        mock_program(&bin, &mut mock_setup, "evaluate_queries", Redirect);
        mock_program(&bin, &mut mock_setup, "queries", Redirect);
        mock_program(&bin, &mut mock_setup, "extract_topics", Redirect);
        mock_program(&bin, &mut mock_setup, "trec_eval", Redirect);
        set_var(
            "PATH",
            format!(
                "{}:{}",
                bin.display(),
                var("PATH").unwrap_or_else(|_| String::from(""))
            ),
        );
        std::fs::write(tmp.path().join("fwd.terms"), "term1\nterm2\nterm3\n").unwrap();

        mock_setup
    }

    #[derive(Debug, PartialEq, Eq)]
    pub(crate) struct EchoOutput(pub Vec<String>);

    impl EchoOutput {}

    impl From<&str> for EchoOutput {
        fn from(output: &str) -> Self {
            Self(output.split('\n').map(|s| s.into()).collect())
        }
    }

    impl From<String> for EchoOutput {
        fn from(output: String) -> Self {
            let sref: &str = &output;
            Self::from(sref)
        }
    }
    impl From<&Path> for EchoOutput {
        fn from(path: &Path) -> Self {
            use std::io::{BufRead, BufReader};
            let file = fs::File::open(path)
                .unwrap_or_else(|_| panic!("Cannot open echo output: {}", path.display()));
            let lines: Result<Vec<String>, std::io::Error> =
                BufReader::new(&file).lines().collect();
            Self(lines.unwrap())
        }
    }

    #[derive(Clone, Copy)]
    pub enum EchoMode {
        Redirect,
        Stdout,
    }

    #[cfg(unix)]
    pub(crate) fn make_echo<P, Q>(program: P, output: Q, mode: EchoMode) -> Result<(), Error>
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        let source = "#!/bin/bash\necho \"$0 $@\"";
        let suffix = match mode {
            EchoMode::Redirect => format!(">> {}", output.as_ref().display()),
            EchoMode::Stdout => "".to_string(),
        };
        if cfg!(unix) {
            let source = format!("{}{}", source, suffix);
            std::fs::write(&program, &source)?;
            std::fs::set_permissions(&program, Permissions::from_mode(0o744)).unwrap();
            Ok(())
        } else {
            Err("this function is only supported on UNIX systems".into())
        }
    }

    #[test]
    fn test_make_echo() {
        let tmp = TempDir::new("echo").expect("Failed to create echo dir");
        let echo = tmp.path().join("e");
        let output = tmp.path().join("output");
        make_echo(&echo, &output, EchoMode::Redirect).expect("Failed to make echo");
        let executor = Executor::from(tmp.path().to_path_buf()).unwrap();
        executor
            .command("e")
            .args(&["arg1", "--a", "arg2"])
            .status()
            .expect("Failed to execute echo");
        assert_eq!(
            EchoOutput::from(output.as_path()),
            EchoOutput::from(format!("{} arg1 --a arg2", echo.display()))
        );
    }
}
