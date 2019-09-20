//! Objects and functions dealing with executing PISA command line tools.

use crate::{Algorithm, Collection, CommandDebug, Encoding, Error, Scorer};
use boolinator::Boolinator;
use failure::ResultExt;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Executes PISA tools.
#[derive(Debug, Default, PartialEq)]
pub struct Executor {
    /// The path where the tools are, or None if the system path should be used.
    path: Option<PathBuf>,
}

impl Executor {
    /// Creates an executor with the system path.
    pub fn new() -> Self {
        Self { path: None }
    }

    /// Creates an executor with a custom path.
    pub fn from(path: PathBuf) -> Result<Self, Error> {
        if path.is_dir() {
            Ok(Self { path: Some(path) })
        } else {
            Err(Error::from(format!(
                "Failed to construct executor: not a directory: {}",
                path.display()
            )))
        }
    }

    /// Creates a command for `program`, resolving the absolute path if necessary.
    pub fn command(&self, program: &str) -> Command {
        Command::new(
            self.path
                .as_ref()
                .unwrap_or(&PathBuf::new())
                .join(program)
                .to_str()
                .unwrap()
                .to_string(),
        )
    }

    /// Runs `invert` command.
    pub fn invert<P1, P2>(
        &self,
        fwd_index: P1,
        inv_index: P2,
        term_count: usize,
    ) -> Result<(), Error>
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>,
    {
        let fwd = fwd_index
            .as_ref()
            .to_str()
            .ok_or("Failed to parse forward index path")?;
        let inv = inv_index
            .as_ref()
            .to_str()
            .ok_or("Failed to parse inverted index path")?;
        let mut invert = self.command("invert");
        invert
            .args(&["-i", fwd])
            .args(&["-o", inv])
            .args(&["--term-count", &term_count.to_string()])
            .log()
            .status()
            .context("Failed to execute: invert")?
            .success()
            .ok_or("Failed to invert index")?;
        Ok(())
    }

    /// Runs `create_freq_index` command.
    pub fn compress<P>(&self, inv_index: P, encoding: &Encoding) -> Result<(), Error>
    where
        P: AsRef<Path>,
    {
        let Encoding(encoding) = encoding;
        let inv = inv_index
            .as_ref()
            .to_str()
            .ok_or("Failed to parse inverted index path")?;
        let mut compress = self.command("create_freq_index");
        compress
            .args(&["-t", encoding])
            .args(&["-c", inv])
            .args(&["-o", &format!("{}.{}", inv, encoding)])
            .arg("--check")
            .log()
            .status()
            .context("Failed to execute: create_freq_index")?
            .success()
            .ok_or("Failed to compress index")?;
        Ok(())
    }

    /// Runs `create_freq_index` command.
    pub fn create_wand_data<P>(&self, inv_index: P, scorer: Option<&Scorer>) -> Result<(), Error>
    where
        P: AsRef<Path>,
    {
        let inv = inv_index
            .as_ref()
            .to_str()
            .ok_or("Failed to parse inverted index path")?;
        let mut command = self.command("create_wand_data");
        command.args(&["-c", inv, "-o", &format!("{}.wand", inv)]);
        if let Some(scorer) = scorer {
            command.args(&["--scorer", scorer.as_ref()]);
        }
        command
            .log()
            .status()
            .context("Failed to execute create_wand_data")?
            .success()
            .ok_or("Failed to create WAND data")?;
        Ok(())
    }

    /// Runs `lexicon build` command.
    pub fn build_lexicon<P1, P2>(&self, input: P1, output: P2) -> Result<(), Error>
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>,
    {
        let input = input
            .as_ref()
            .to_str()
            .ok_or("Failed to parse input path")?;
        let output = output
            .as_ref()
            .to_str()
            .ok_or("Failed to parse output path")?;
        self.command("lexicon")
            .args(&["build", input, output])
            .log()
            .status()
            .context("Failed to execute lexicon build")?
            .success()
            .ok_or("Failed to build lexicon")?;
        Ok(())
    }

    /// Runs `extract_topics` command.
    pub fn extract_topics<P1, P2>(&self, input: P1, output: P2) -> Result<(), Error>
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>,
    {
        let input = input
            .as_ref()
            .to_str()
            .ok_or("Failed to parse input path")?;
        let output = output
            .as_ref()
            .to_str()
            .ok_or("Failed to parse output path")?;
        self.command("extract_topics")
            .args(&["-i", input, "-o", output])
            .log()
            .status()
            .context("Failed to execute extract_topics")?
            .success()
            .ok_or("Failed to extract topics")?;
        Ok(())
    }

    /// Runs `evaluate_queries` command.
    pub fn evaluate_queries<S>(
        &self,
        collection: &Collection,
        encoding: &Encoding,
        algorithm: &Algorithm,
        queries: S,
        scorer: Option<&Scorer>,
    ) -> Result<String, Error>
    where
        S: AsRef<str>,
    {
        let inv = collection
            .inv_index
            .to_str()
            .ok_or("Failed to parse inverted index path")?;
        let fwd = collection
            .fwd_index
            .to_str()
            .ok_or("Failed to parse forward index path")?;
        let mut command = self.command("evaluate_queries");
        command
            .args(&["-t", encoding.as_ref()])
            .args(&["-i", &format!("{}.{}", inv, encoding)])
            .args(&["-w", &format!("{}.wand", inv)])
            .args(&["-a", algorithm.as_ref()])
            .args(&["-q", queries.as_ref()])
            .args(&["--terms", &format!("{}.termmap", fwd)])
            .args(&["--documents", &format!("{}.docmap", fwd)])
            .args(&["--stemmer", "porter2"])
            .args(&["-k", "1000"]);
        if let Some(scorer) = scorer {
            command.args(&["--scorer", scorer.as_ref()]);
        }
        let output = command
            .log()
            .output()
            .context("Failed to run evaluate_queries")?;
        if output.status.success() {
            Ok(String::from_utf8(output.stdout).unwrap())
        } else {
            Err(Error::from(String::from_utf8(output.stderr).unwrap()))
        }
    }

    /// Runs `queries` command.
    pub fn benchmark<S>(
        &self,
        collection: &Collection,
        encoding: &Encoding,
        algorithm: &Algorithm,
        queries: S,
        scorer: Option<&Scorer>,
    ) -> Result<String, Error>
    where
        S: AsRef<str>,
    {
        let inv = collection
            .inv_index
            .to_str()
            .ok_or("Failed to parse inverted index path")?;
        let fwd = collection
            .fwd_index
            .to_str()
            .ok_or("Failed to parse forward index path")?;
        let mut command = self.command("queries");
        command
            .args(&["-t", encoding.as_ref()])
            .args(&["-i", &format!("{}.{}", inv, encoding)])
            .args(&["-w", &format!("{}.wand", inv)])
            .args(&["-a", &algorithm.to_string()])
            .args(&["-q", queries.as_ref()])
            .args(&["--terms", &format!("{}.termmap", fwd)])
            .args(&["--stemmer", "porter2"])
            .args(&["-k", "1000"]);
        if let Some(scorer) = scorer {
            command.args(&["--scorer", scorer.as_ref()]);
        }
        let output = command.log().output().context("Failed to run queries")?;
        if output.status.success() {
            Ok(String::from_utf8(output.stdout).unwrap())
        } else {
            Err(Error::from(String::from_utf8(output.stderr).unwrap()))
        }
    }
}

#[cfg(test)]
mod test {
    extern crate tempdir;

    use crate::run::process_run;
    use crate::tests::{mock_set_up, MockSetup};
    use crate::{Config, Error, Executor, Stage};
    use crate::{Encoding, RawConfig, ResolvedPathsConfig, Scorer, Source};
    use std::fs::create_dir_all;
    use std::fs::Permissions;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::process::Command;
    use tempdir::TempDir;

    fn test_exec<F>(prog: &str, err: &'static str, exec: F)
    where
        F: Fn(&MockSetup) -> Result<(), Error>,
    {
        {
            let tmp = TempDir::new("executor").unwrap();
            let setup: MockSetup = mock_set_up(&tmp);
            assert!(exec(&setup).is_ok());
        }
        {
            let tmp = TempDir::new("executor").unwrap();
            let setup: MockSetup = mock_set_up(&tmp);
            std::fs::write(setup.programs.get(prog).unwrap(), "#!/bin/bash\nexit 1").unwrap();
            assert_eq!(exec(&setup), Err(Error::from(err)));
        }
        {
            let tmp = TempDir::new("executor").unwrap();
            let setup: MockSetup = mock_set_up(&tmp);
            std::fs::remove_file(setup.programs.get(prog).unwrap()).unwrap();
            assert!(exec(&setup).is_err());
        }
    }

    #[test]
    fn test_new_executor() {
        assert_eq!(Executor::new(), Executor { path: None });
    }

    #[test]
    #[cfg_attr(target_family, unix)]
    fn test_invert() {
        test_exec("invert", "Failed to invert index", |setup: &MockSetup| {
            setup.executor.invert(
                &setup.config.collection(0).fwd_index,
                &setup.config.collection(0).inv_index,
                setup.term_count,
            )
        });
    }

    #[test]
    #[cfg_attr(target_family, unix)]
    fn test_compress() {
        test_exec(
            "create_freq_index",
            "Failed to compress index",
            |setup: &MockSetup| {
                setup.executor.compress(
                    &setup.config.collection(0).fwd_index,
                    &Encoding::from("block_simdbp"),
                )
            },
        );
    }

    #[test]
    #[cfg_attr(target_family, unix)]
    fn test_create_wand_data() {
        test_exec(
            "create_wand_data",
            "Failed to create WAND data",
            |setup: &MockSetup| {
                setup.executor.create_wand_data(
                    &setup.config.collection(0).inv_index,
                    Some(&Scorer::from("bm25")),
                )
            },
        );
    }

    #[test]
    #[cfg_attr(target_family, unix)]
    fn test_custom_path_source_executor() {
        let tmp = TempDir::new("tmp").unwrap();
        let program = "#!/bin/bash
    echo ok";
        let program_path = tmp.path().join("program");
        std::fs::write(&program_path, &program).unwrap();
        let permissions = Permissions::from_mode(0o744);
        std::fs::set_permissions(&program_path, permissions).unwrap();

        let config = ResolvedPathsConfig::from(RawConfig {
            workdir: PathBuf::from("workdir"),
            source: Source::Path(tmp.path().to_path_buf()),
            ..RawConfig::default()
        });
        let executor = config.executor().unwrap();
        let output = executor.command("program").output().unwrap();
        assert_eq!(std::str::from_utf8(&output.stdout).unwrap(), "ok\n");
    }

    #[test]
    fn test_git_executor_wrong_bin() {
        assert_eq!(
            RawConfig {
                source: Source::Path(PathBuf::from("/nonexistent/path")),
                ..RawConfig::default()
            }
            .executor()
            .err(),
            Some("Failed to construct executor: not a directory: /nonexistent/path".into())
        );
    }

    #[test]
    fn test_init_git_failed_clone() {
        let tmp = TempDir::new("tmp").unwrap();
        let workdir = tmp.path().join("work");
        create_dir_all(&workdir).unwrap();

        let conf = ResolvedPathsConfig::from(RawConfig {
            workdir,
            source: Source::Git {
                branch: "master".into(),
                url: "http://examp.le".into(),
                cmake_vars: vec![],
            },
            ..RawConfig::default()
        });
        let expected = "failed to resolve address for examp.le".to_string();
        assert_eq!(
            conf.executor().err().unwrap().to_string()[..expected.len()],
            expected
        );
    }

    fn run_from(dir: PathBuf) -> impl Fn(&'static str) -> () {
        move |args: &'static str| {
            let mut args = args.split(' ');
            Command::new(args.next().unwrap())
                .current_dir(&dir)
                .args(args.collect::<Vec<&str>>())
                .status()
                .expect("failed git command");
        }
    }

    fn set_up_git() -> (TempDir, PathBuf, PathBuf) {
        let tmp = TempDir::new("tmp").unwrap();
        let workdir = tmp.path().join("work");
        let origin_dir = tmp.path().join("origin");
        create_dir_all(&workdir).unwrap();
        create_dir_all(&origin_dir).unwrap();
        let run = run_from(origin_dir.clone());
        run("git init");
        let cmakelists = "cmake_minimum_required(VERSION 3.0)
                 add_custom_target(build-time-make-directory ALL
                 COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/bin)";
        std::fs::write(origin_dir.join("CMakeLists.txt"), &cmakelists)
            .expect("Unable to write file");
        run("git add CMakeLists.txt");
        run("git commit -m \"c\"");
        (tmp, workdir, origin_dir)
    }

    #[test]
    fn test_init_git() {
        let (_tmp, workdir, origin_dir) = set_up_git();
        let conf = ResolvedPathsConfig::from(RawConfig {
            workdir: workdir.clone(),
            source: Source::Git {
                url: origin_dir.to_string_lossy().to_string(),
                branch: "master".into(),
                cmake_vars: vec![],
            },
            ..RawConfig::default()
        });
        assert_eq!(
            conf.executor(),
            Ok(Executor {
                path: Some(workdir.join("pisa").join("build").join("bin"))
            })
        );
    }

    #[test]
    fn test_init_git_exists() {
        let (_tmp, workdir, origin_dir) = set_up_git();
        let url = origin_dir.to_string_lossy().to_string();
        git2::Repository::clone(&url, &workdir.join("pisa")).unwrap();
        let conf = ResolvedPathsConfig::from(RawConfig {
            workdir: workdir.clone(),
            source: Source::Git {
                url,
                branch: "master".into(),
                cmake_vars: vec![],
            },
            ..RawConfig::default()
        });
        assert_eq!(
            conf.executor(),
            Ok(Executor {
                path: Some(workdir.join("pisa").join("build").join("bin"))
            })
        );
    }

    #[test]
    fn test_init_git_suppress_compilation() {
        let (_tmp, workdir, origin_dir) = set_up_git();
        let mut conf = ResolvedPathsConfig::from(RawConfig {
            workdir: workdir.clone(),
            source: Source::Git {
                url: origin_dir.to_string_lossy().to_string(),
                branch: "master".into(),
                cmake_vars: vec![],
            },
            ..RawConfig::default()
        });
        conf.disable(Stage::Compile);
        assert_eq!(
            conf.executor().err(),
            Some(Error::from(format!(
                "Failed to construct executor: not a directory: {}",
                workdir
                    .join("pisa")
                    .join("build")
                    .join("bin")
                    .to_str()
                    .unwrap()
            )))
        );
    }

    #[test]
    fn test_process_run() {
        let tmp = TempDir::new("executor").unwrap();
        let MockSetup {
            config,
            executor,
            programs,
            outputs,
            ..
        } = mock_set_up(&tmp);
        let run = &config.run(0);
        let collection = &config.collection(0);
        process_run(&executor, run, collection, true).unwrap();
        let topics_path = if let crate::config::Topics::Trec {
            path: topics_path, ..
        } = &run.topics[0]
        {
            topics_path
        } else {
            panic!();
        };
        assert_eq!(
            std::fs::read_to_string(outputs.get("extract_topics").unwrap()).unwrap(),
            format!(
                "{0} -i {1} -o {1}\n",
                programs.get("extract_topics").unwrap().display(),
                topics_path.display()
            )
        );
        assert_eq!(
            std::fs::read_to_string(outputs.get("evaluate_queries").unwrap()).unwrap(),
            format!(
                "{0} -t block_simdbp -i {1}.block_simdbp -w {1}.wand -a wand -q {3}.title \
                 --terms {2}.termmap --documents {2}.docmap --stemmer porter2 -k 1000 \
                 --scorer bm25\n\
                 {0} -t block_qmx -i {1}.block_qmx -w {1}.wand -a wand -q {3}.title \
                 --terms {2}.termmap --documents {2}.docmap --stemmer porter2 -k 1000 \
                 --scorer bm25\n\
                 {0} -t block_simdbp -i {1}.block_simdbp -w {1}.wand -a maxscore -q {3}.title \
                 --terms {2}.termmap --documents {2}.docmap --stemmer porter2 -k 1000 \
                 --scorer bm25\n\
                 {0} -t block_qmx -i {1}.block_qmx -w {1}.wand -a maxscore -q {3}.title \
                 --terms {2}.termmap --documents {2}.docmap --stemmer porter2 -k 1000 \
                 --scorer bm25\n",
                programs.get("evaluate_queries").unwrap().display(),
                collection.inv_index.display(),
                collection.fwd_index.display(),
                topics_path.display()
            )
        );
    }

    #[test]
    fn test_evaluate_fails() {
        let tmp = TempDir::new("executor").unwrap();
        let MockSetup {
            config,
            executor,
            programs,
            ..
        } = mock_set_up(&tmp);
        let run = &config.run(0);
        let collection = &config.collection(0);
        std::fs::write(
            programs.get("evaluate_queries").unwrap(),
            "#!/bin/bash\nexit 1",
        )
        .unwrap();
        assert!(process_run(&executor, run, collection, true).is_err());
    }

    #[test]
    fn test_bench_fails() {
        let tmp = TempDir::new("executor").unwrap();
        let MockSetup {
            config,
            executor,
            programs,
            ..
        } = mock_set_up(&tmp);
        let run = &config.run(2);
        let collection = &config.collection(0);
        std::fs::write(programs.get("queries").unwrap(), "#!/bin/bash\nexit 1").unwrap();
        assert!(process_run(&executor, run, collection, true).is_err());
    }
}
