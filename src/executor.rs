//! Objects and functions dealing with executing PISA command line tools.

extern crate boolinator;
extern crate downcast_rs;
extern crate experiment;
extern crate failure;

use super::config::Encoding;
use super::*;
use boolinator::Boolinator;
use downcast_rs::Downcast;
use experiment::process::Process;
use failure::ResultExt;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::path::{Path, PathBuf};

/// Implementations of this trait execute PISA tools.
pub trait PisaExecutor: Debug + Downcast {
    /// Builds a process object for a program with given arguments.
    fn command(&self, program: &str, args: &[&str]) -> Process;
}
impl_downcast!(PisaExecutor);
#[cfg_attr(tarpaulin, skip)] // Due to so many false positives
impl PisaExecutor {
    /// Runs `invert` command.
    pub fn invert<P1, P2>(
        &self,
        forward_index: P1,
        inverted_index: P2,
        term_count: usize,
    ) -> Result<(), Error>
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>,
    {
        let fwd = forward_index
            .as_ref()
            .to_str()
            .ok_or("Failed to parse forward index path")?;
        let inv = inverted_index
            .as_ref()
            .to_str()
            .ok_or("Failed to parse inverted index path")?;
        let cmd = self.command(
            "invert",
            &[
                "-i",
                fwd,
                "-o",
                inv,
                "--term-count",
                &term_count.to_string(),
            ],
        );
        printed(cmd)
            .execute()
            .context("Failed to execute: invert")?
            .success()
            .ok_or("Failed to invert index")?;
        Ok(())
    }

    /// Runs `create_freq_index` command.
    pub fn compress<P>(&self, inverted_index: P, encoding: &Encoding) -> Result<(), Error>
    where
        P: AsRef<Path>,
    {
        let inv = inverted_index
            .as_ref()
            .to_str()
            .ok_or("Failed to parse inverted index path")?;
        let cmd = self.command(
            "create_freq_index",
            &[
                "-t",
                encoding.as_ref(),
                "-c",
                inv,
                "-o",
                &format!("{}.{}", inv, encoding),
                "--check",
            ],
        );
        printed(cmd)
            .execute()
            .context("Failed to execute: create_freq_index")?
            .success()
            .ok_or("Failed to compress index")?;
        Ok(())
    }

    /// Runs `create_freq_index` command.
    pub fn create_wand_data<P>(&self, inverted_index: P) -> Result<(), Error>
    where
        P: AsRef<Path>,
    {
        let inv = inverted_index
            .as_ref()
            .to_str()
            .ok_or("Failed to parse inverted index path")?;
        let cmd = self.command(
            "create_wand_data",
            &["-c", inv, "-o", &format!("{}.wand", inv)],
        );
        printed(cmd)
            .execute()
            .context("Failed to execute create_wand_data")?
            .success()
            .ok_or("Failed to create WAND data")?;
        Ok(())
    }
}

/// This executor simply executes the commands as passed,
/// as if they were on the system path.
#[derive(Default, Debug)]
pub struct SystemPathExecutor {}
impl SystemPathExecutor {
    /// A convenience function, equivalent to `SystemPathExecutor{}`.
    pub fn new() -> Self {
        Self {}
    }
}
impl PisaExecutor for SystemPathExecutor {
    fn command(&self, program: &str, args: &[&str]) -> Process {
        Process::new(program, args)
    }
}

/// An executor using compiled code from git repository.
#[derive(Debug, PartialEq, Clone)]
pub struct CustomPathExecutor {
    bin: PathBuf,
}
impl TryFrom<&Path> for CustomPathExecutor {
    type Error = Error;
    fn try_from(bin_path: &Path) -> Result<Self, Error> {
        if bin_path.is_dir() {
            Ok(Self {
                bin: bin_path.to_path_buf(),
            })
        } else {
            Err(format!(
                "Failed to construct executor: not a directory: {}",
                bin_path.display()
            )
            .into())
        }
    }
}
impl TryFrom<PathBuf> for CustomPathExecutor {
    type Error = Error;
    fn try_from(bin_path: PathBuf) -> Result<Self, Error> {
        Self::try_from(bin_path.as_path())
    }
}
impl TryFrom<&str> for CustomPathExecutor {
    type Error = Error;
    fn try_from(bin_path: &str) -> Result<Self, Error> {
        Self::try_from(Path::new(bin_path))
    }
}
impl CustomPathExecutor {
    /// Returns a reference to the `bin` path, where the tools reside.
    pub fn path(&self) -> &Path {
        self.bin.as_path()
    }
}
impl PisaExecutor for CustomPathExecutor {
    fn command(&self, program: &str, args: &[&str]) -> Process {
        Process::new(&self.bin.join(program).to_str().unwrap().to_string(), args)
    }
}

#[cfg(test)]
mod tests {
    extern crate downcast_rs;
    extern crate tempdir;

    use super::super::tests::{mock_set_up, MockSetup};
    use super::config::*;
    use super::source::*;
    use super::*;
    use std::fs::create_dir_all;
    use std::fs::Permissions;
    use std::os::unix::fs::PermissionsExt;
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
    #[cfg_attr(target_family, unix)]
    fn test_invert() {
        test_exec("invert", "Failed to invert index", |setup: &MockSetup| {
            setup.executor.invert(
                &setup.config.collections[0].forward_index,
                &setup.config.collections[0].inverted_index,
                setup.term_count,
            )
        });
    }

    #[test]
    #[cfg_attr(target_family, unix)]
    fn test_compress() {
        test_exec(
            "compress",
            "Failed to compress index",
            |setup: &MockSetup| {
                setup.executor.compress(
                    &setup.config.collections[0].forward_index,
                    &Encoding::from("block_simdbp"),
                )
            },
        );
    }

    #[test]
    #[cfg_attr(target_family, unix)]
    fn test_create_wand_data() {
        test_exec("wand", "Failed to create WAND data", |setup: &MockSetup| {
            setup
                .executor
                .create_wand_data(&setup.config.collections[0].inverted_index)
        });
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

        let source = CustomPathSource::from(tmp.path());
        let config = Config::new("workdir", Box::new(source));
        let executor = config.executor().unwrap();
        let output = executor.command("program", &[]).command().output().unwrap();
        assert_eq!(std::str::from_utf8(&output.stdout).unwrap(), "ok\n");
    }

    #[test]
    fn test_git_executor_wrong_bin() {
        assert_eq!(
            CustomPathExecutor::try_from(PathBuf::from("/nonexistent/path")),
            Err("Failed to construct executor: not a directory: /nonexistent/path".into())
        );
    }

    #[test]
    fn test_init_git_failed_clone() {
        let tmp = TempDir::new("tmp").unwrap();
        let workdir = tmp.path().join("work");
        create_dir_all(&workdir).unwrap();

        let conf = Config::new(&workdir, Box::new(GitSource::new("xxx", "master")));
        assert_eq!(
            conf.source.executor(&conf).err(),
            Some(Error::from("cloning failed"))
        );
    }

    fn run_from(dir: PathBuf) -> impl Fn(&'static str) -> () {
        move |args: &'static str| {
            let mut args = args.split(" ").into_iter();
            Command::new(args.next().unwrap())
                .current_dir(&dir)
                .args(args.collect::<Vec<&str>>())
                .status()
                .expect("failed git command");
            ()
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
        let conf = Config::new(
            &workdir,
            Box::new(GitSource::new(&origin_dir.to_str().unwrap(), "master")),
        );
        assert_eq!(
            conf.source
                .executor(&conf)
                .unwrap()
                .downcast_ref::<CustomPathExecutor>(),
            CustomPathExecutor::try_from(
                workdir
                    .join("pisa")
                    .join("build")
                    .join("bin")
                    .to_str()
                    .unwrap()
            )
            .ok()
            .as_ref()
        );
    }

    #[test]
    fn test_init_git_suppress_compilation() {
        let (_tmp, workdir, origin_dir) = set_up_git();
        let mut conf = Config::new(
            &workdir,
            Box::new(GitSource::new(&origin_dir.to_str().unwrap(), "master")),
        );
        conf.suppress_stage(Stage::Compile);
        assert_eq!(
            conf.source.executor(&conf).err(),
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
}
