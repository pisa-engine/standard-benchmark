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
        let cmd = self.command("lexicon", &["build", input, output]);
        printed(cmd)
            .execute()
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
        let cmd = self.command("extract_topics", &["-i", input, "-o", output]);
        printed(cmd)
            .execute()
            .context("Failed to execute extract_topics")?
            .success()
            .ok_or("Failed to extract topics")?;
        Ok(())
    }

    /// Runs `evaluate_queries` command.
    pub fn evaluate_queries<P1, P2, P3>(
        &self,
        inverted_index: P1,
        forward_index: P2,
        encoding: &Encoding,
        queries: P3,
    ) -> Result<String, Error>
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>,
        P3: AsRef<Path>,
    {
        let inv = inverted_index
            .as_ref()
            .to_str()
            .ok_or("Failed to parse inverted index path")?;
        let queries = queries
            .as_ref()
            .to_str()
            .ok_or("Failed to parse queries path")?;
        let fwd = forward_index
            .as_ref()
            .to_str()
            .ok_or("Failed to parse forward index path")?;
        let cmd = self.command(
            "evaluate_queries",
            &[
                "-t",
                encoding.as_ref(),
                "-i",
                &format!("{}.{}", inv, encoding),
                "-w",
                &format!("{}.wand", inv),
                "-a",
                "wand",
                "-q",
                queries,
                "--terms",
                &format!("{}.termmap", fwd),
                "--documents",
                &format!("{}.docmap", fwd),
                "--stemmer",
                "porter2",
            ],
        );
        let output = printed(cmd)
            .command()
            .output()
            .context("Failed to run evaluate_queries")?;
        if output.status.success() {
            Ok(String::from_utf8(output.stdout).unwrap())
        } else {
            Err(Error::from(String::from_utf8(output.stderr).unwrap()))
        }
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
mod tests;
