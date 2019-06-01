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

extern crate downcast_rs;
extern crate experiment;
extern crate failure;
extern crate json;
extern crate strum;

use downcast_rs::impl_downcast;
use error::Error;
use experiment::process::Process;
use experiment::Verbosity;
use log::debug;
use std::fmt;
use std::fs::create_dir_all;
use std::path::Path;
use strum_macros::{Display, EnumIter, EnumString};

pub mod build;
pub mod config;
pub mod error;
pub mod executor;
pub mod run;
pub mod source;

/// Available stages of the experiment.
/// # Examples
///
/// All names are lowercase:
///
/// ```
/// # extern crate stdbench;
/// # use::stdbench::*;
/// assert_eq!("compile".parse(), Ok(Stage::Compile));
/// assert_eq!("build".parse(), Ok(Stage::BuildIndex));
/// assert_eq!("parse".parse(), Ok(Stage::ParseCollection));
/// assert_eq!("invert".parse(), Ok(Stage::Invert));
/// assert!("?".parse::<Stage>().is_err());
/// assert_eq!("compile", format!("{}", Stage::Compile));
/// assert_eq!("build", format!("{}", Stage::BuildIndex));
/// assert_eq!("parse", format!("{}", Stage::ParseCollection));
/// assert_eq!("invert", format!("{}", Stage::Invert));
/// ```
#[cfg_attr(tarpaulin, skip)]
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, EnumString, Display, EnumIter)]
pub enum Stage {
    /// Compilation stage; includes things such as: fetching code, configuring,
    /// and actual compilation of the source code. The exact meaning depends on
    /// the type of the source being processed.
    #[strum(serialize = "compile")]
    Compile,
    /// Includes building forward/inverted index and index compressing.
    #[strum(serialize = "build")]
    BuildIndex,
    /// A subset of `BuildIndex`; means: build an inverted index but assume the
    /// forward index has been already built (e.g., in a previous run).
    #[strum(serialize = "parse")]
    ParseCollection,
    /// Inverting stage; mean: compress an inverted index but do not invert forward
    /// index, assuming it has been done already.
    #[strum(serialize = "invert")]
    Invert,
}

/// Prints the passed command and returns it back.
#[cfg_attr(tarpaulin, skip)]
pub fn printed(cmd: Process) -> Process {
    debug!("[EXEC] {}", cmd.display(Verbosity::Verbose));
    cmd
}

/// If the parent directory of `path` does not exist, create it.
///
/// # Examples
///
/// ```
/// # extern crate stdbench;
/// # extern crate tempdir;
/// # use stdbench::*;
/// # use stdbench::error::*;
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
    create_dir_all(parent)?;
    Ok(())
}

/// Executes a `$cmd`, checks for results, and returns an error with `$errmsg` message.
/// It is designed to be similar to `?` operator, removing bulky boilerplate from
/// functions that execute many consecutive commands.
///
/// This macro will return error in one of the two cases:
/// - command execution failed,
/// - command returned an exit status equivalent to an error.
///
/// This macro is intended to be used in simple cases when we do not want to capture
/// the output or learn more about exit status, since the only feedback we get
/// is the error message passed at the call site.
///
/// # Example
///
/// ```
/// # #[macro_use]
/// # extern crate stdbench;
/// extern crate boolinator;
/// # use stdbench::error::Error;
/// # use std::process::Command;
/// use boolinator::Boolinator;
/// # fn main() {
/// fn f() -> Result<(), Error> {
///     execute!(Command::new("ls"); "couldn't ls");
///     execute!(Command::new("cat").args(&["some_file"]); "couldn't cat");
///     Ok(())
/// }
///
/// match f() {
///     Ok(()) => println!(),
///     Err(err) => println!("Here's what went wrong"),
/// }
/// # }
/// ```
#[macro_export]
macro_rules! execute {
    ($cmd:expr; $errmsg:expr) => {{
        $cmd.status()?.success().ok_or(Error::from($errmsg))?;
    }};
}

#[cfg(test)]
mod tests;
