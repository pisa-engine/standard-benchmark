//! Objects and functions dealing with executing PISA command line tools.

extern crate boolinator;
extern crate downcast_rs;
extern crate failure;

use crate::command::ExtCommand;
use crate::config::{Algorithm, Collection, Encoding};
use crate::run::EvaluateData;
use crate::*;
use boolinator::Boolinator;
use downcast_rs::Downcast;
use failure::ResultExt;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::path::{Path, PathBuf};

/// Implementations of this trait execute PISA tools.
pub trait PisaExecutor: Debug + Downcast {
    /// Builds a process object for a program with given arguments.
    fn command(&self, program: &str) -> ExtCommand;
}
impl_downcast!(PisaExecutor);
#[cfg_attr(tarpaulin, skip)] // Due to so many false positives
impl dyn PisaExecutor {
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
        self.command("invert")
            .args(&[
                "-i",
                fwd,
                "-o",
                inv,
                "--term-count",
                &term_count.to_string(),
            ])
            .status()
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
        self.command("create_freq_index")
            .args(&[
                "-t",
                encoding.as_ref(),
                "-c",
                inv,
                "-o",
                &format!("{}.{}", inv, encoding),
                "--check",
            ])
            .status()
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
        self.command("create_wand_data")
            .args(&[
                "-c",
                inv,
                "-o",
                &format!("{}.wand", inv),
                "--scorer",
                "bm25",
            ])
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
        _run_data: &EvaluateData, // To be used in the future
        queries: S,
    ) -> Result<String, Error>
    where
        S: AsRef<str>,
    {
        let inv = collection
            .inverted_index
            .to_str()
            .ok_or("Failed to parse inverted index path")?;
        let fwd = collection
            .forward_index
            .to_str()
            .ok_or("Failed to parse forward index path")?;
        let encoding = &collection.encodings.first().unwrap();
        let output = self
            .command("evaluate_queries")
            .args(&["-t", encoding.as_ref()])
            .args(&["-i", &format!("{}.{}", inv, encoding)])
            .args(&["-w", &format!("{}.wand", inv)])
            .args(&["-a", "wand"])
            .args(&["-q", queries.as_ref()])
            .args(&["--terms", &format!("{}.termmap", fwd)])
            .args(&["--documents", &format!("{}.docmap", fwd)])
            .args(&["--stemmer", "porter2"])
            .args(&["-k", "1000"])
            .args(&["--scorer", "bm25"])
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
    ) -> Result<String, Error>
    where
        S: AsRef<str>,
    {
        let inv = collection
            .inverted_index
            .to_str()
            .ok_or("Failed to parse inverted index path")?;
        let fwd = collection
            .forward_index
            .to_str()
            .ok_or("Failed to parse forward index path")?;
        let output = self
            .command("queries")
            .args(&["-t", encoding.as_ref()])
            .args(&["-i", &format!("{}.{}", inv, encoding)])
            .args(&["-w", &format!("{}.wand", inv)])
            .args(&["-a", &algorithm.to_string()])
            .args(&["-q", queries.as_ref()])
            .args(&["--terms", &format!("{}.termmap", fwd)])
            .args(&["--stemmer", "porter2"])
            .args(&["-k", "1000"])
            .args(&["--scorer", "bm25"])
            .output()
            .context("Failed to run queries")?;
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
    fn command(&self, program: &str) -> ExtCommand {
        ExtCommand::new(program)
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
    fn command(&self, program: &str) -> ExtCommand {
        ExtCommand::new(&self.bin.join(program).to_str().unwrap().to_string())
    }
}

#[cfg(test)]
mod tests;
