extern crate experiment;

use super::config::*;
use super::*;
use experiment::process::Process;
use log::warn;
use std::path::{Path, PathBuf};

/// Implementations of this trait execute PISA tools.
pub trait PisaExecutor {
    /// Builds a process object for a program with given arguments.
    fn command(&self, program: &str, args: &[&str]) -> Process;
}
impl PisaExecutor {
    /// Construct an executor based on the passed config.
    pub fn from(config: &Config) -> Result<Box<PisaExecutor>, Error> {
        match &config.source {
            CodeSource::Git { url, branch } => init_git(config, url, branch),
            CodeSource::Docker { .. } => unimplemented!(),
        }
    }
}

/// This executor simply executes the commands as passed,
/// as if they were on the system path.
#[derive(Default)]
pub struct SystemPathExecutor {}
impl SystemPathExecutor {
    pub fn new() -> SystemPathExecutor {
        SystemPathExecutor {}
    }
}
impl PisaExecutor for SystemPathExecutor {
    fn command(&self, program: &str, args: &[&str]) -> Process {
        Process::new(program, args)
    }
}

/// An executor using compiled code from git repository.
pub struct GitPisaExecutor {
    bin: PathBuf,
}
impl GitPisaExecutor {
    pub fn new<P>(bin_path: P) -> Result<GitPisaExecutor, Error>
    where
        P: AsRef<Path>,
    {
        if bin_path.as_ref().is_dir() {
            Ok(GitPisaExecutor {
                bin: bin_path.as_ref().to_path_buf(),
            })
        } else {
            Err(Error(format!(
                "Failed to construct git executor: not a directory: {}",
                bin_path.as_ref().display()
            )))
        }
    }
}
impl PisaExecutor for GitPisaExecutor {
    fn command(&self, program: &str, args: &[&str]) -> Process {
        Process::new(&self.bin.join(program).to_str().unwrap().to_string(), args)
    }
}

fn init_git(config: &Config, url: &str, branch: &str) -> Result<Box<PisaExecutor>, Error> {
    let dir = config.workdir.join("pisa");
    if !dir.exists() {
        let clone = Process::new("git", &["clone", &url, dir.to_str().unwrap()]);
        printed(clone).execute().unwrap_or_else(exit_gracefully);
    };
    let build_dir = dir.join("build");

    if config.is_suppressed(Stage::Compile) {
        warn!("Compilation has been suppressed");
    } else {
        let checkout = Process::new("git", &["-C", &dir.to_str().unwrap(), "checkout", branch]);
        printed(checkout).execute().unwrap_or_else(exit_gracefully);
        create_dir_all(&build_dir).unwrap_or_else(exit_gracefully);
        let cmake = Process::new(
            "cmake",
            &[
                "-DCMAKE_BUILD_TYPE=Release",
                "-S",
                &dir.to_str().unwrap(),
                "-B",
                &build_dir.to_str().unwrap(),
            ],
        );
        printed(cmake).execute().unwrap_or_else(exit_gracefully);
        let build = Process::new("cmake", &["--build", &build_dir.to_str().unwrap()]);
        printed(build).execute().unwrap_or_else(exit_gracefully);
    }
    let executor = GitPisaExecutor::new(build_dir.join("bin"))?;
    Ok(Box::new(executor))
}
