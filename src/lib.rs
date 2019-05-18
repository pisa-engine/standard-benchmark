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

use downcast_rs::impl_downcast;
use error::Error;
use experiment::process::Process;
use experiment::Verbosity;
use log::debug;
use std::fmt;
use std::fs::create_dir_all;
use std::path::Path;
use std::str::FromStr;

pub mod build;
pub mod config;
pub mod error;
pub mod executor;
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
/// assert_eq!("?".parse::<Stage>(), Err("invalid stage: ?".into()));
/// assert_eq!("compile", format!("{}", Stage::Compile));
/// assert_eq!("build", format!("{}", Stage::BuildIndex));
/// assert_eq!("parse", format!("{}", Stage::ParseCollection));
/// assert_eq!("invert", format!("{}", Stage::Invert));
/// ```
#[cfg_attr(tarpaulin, skip)]
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub enum Stage {
    /// Compilation stage; includes things such as: fetching code, configuring,
    /// and actual compilation of the source code. The exact meaning depends on
    /// the type of the source being processed.
    Compile,
    /// Includes building forward/inverted index and index compressing.
    BuildIndex,
    /// A subset of `BuildIndex`; means: build an inverted index but assume the
    /// forward index has been already built (e.g., in a previous run).
    ParseCollection,
    /// Inverting stage; mean: compress an inverted index but do not invert forward
    /// index, assuming it has been done already.
    /// **Note**: it implicitly suppresses parsing as in `ParseCollection`
    Invert,
}
impl FromStr for Stage {
    type Err = Error;

    /// Parse string and return a stage enum if string correct.
    fn from_str(name: &str) -> Result<Self, Error> {
        match name.to_lowercase().as_ref() {
            "compile" => Ok(Stage::Compile),
            "build" => Ok(Stage::BuildIndex),
            "parse" => Ok(Stage::ParseCollection),
            "invert" => Ok(Stage::Invert),
            _ => Err(format!("invalid stage: {}", &name).into()),
        }
    }
}
impl fmt::Display for Stage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Stage::Compile => "compile",
                Stage::BuildIndex => "build",
                Stage::ParseCollection => "parse",
                Stage::Invert => "invert",
            }
        )
    }
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
mod tests {
    extern crate tempdir;

    use super::config::*;
    use super::executor::PisaExecutor;
    use super::source::*;
    use super::*;
    use boolinator::Boolinator;
    use std::collections::HashMap;
    use std::convert::TryFrom;
    use std::fs::Permissions;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use tempdir::TempDir;

    pub(crate) struct MockSetup {
        pub config: Config,
        pub executor: Box<PisaExecutor>,
        pub programs: HashMap<&'static str, PathBuf>,
        pub outputs: HashMap<&'static str, PathBuf>,
        pub term_count: usize,
    }

    pub(crate) fn mock_set_up(tmp: &TempDir) -> MockSetup {
        let mut output_paths: HashMap<&'static str, PathBuf> = HashMap::new();
        let mut programs: HashMap<&'static str, PathBuf> = HashMap::new();

        let parse_path = tmp.path().join("parse_collection.out");
        let parse_prog = tmp.path().join("parse_collection");
        make_echo(&parse_prog, &parse_path).unwrap();
        output_paths.insert("parse", parse_path);
        programs.insert("parse", parse_prog);

        let invert_path = tmp.path().join("invert.out");
        let invert_prog = tmp.path().join("invert");
        make_echo(&invert_prog, &invert_path).unwrap();
        output_paths.insert("invert", invert_path);
        programs.insert("invert", invert_prog);
        std::fs::write(tmp.path().join("fwd.terms"), "term1\nterm2\nterm3\n").unwrap();

        let compress_path = tmp.path().join("create_freq_index.out");
        let compress_prog = tmp.path().join("create_freq_index");
        make_echo(&compress_prog, &compress_path).unwrap();
        output_paths.insert("compress", compress_path);
        programs.insert("compress", compress_prog);

        let wand_path = tmp.path().join("create_wand_data.out");
        let wand_prog = tmp.path().join("create_wand_data");
        make_echo(&wand_prog, &wand_path).unwrap();
        output_paths.insert("wand", wand_path);
        programs.insert("wand", wand_prog);

        let mut config = Config::new(tmp.path(), Box::new(CustomPathSource::from(tmp.path())));
        config.collections.push(Collection {
            name: String::from("wapo"),
            collection_dir: tmp.path().join("coll"),
            forward_index: tmp.path().join("fwd"),
            inverted_index: tmp.path().join("inv"),
            encodings: vec!["block_simdbp".into(), "block_qmx".into()],
        });

        let data_dir = tmp.path().join("coll").join("data");
        create_dir_all(&data_dir).unwrap();
        std::fs::File::create(data_dir.join("f.jl")).unwrap();
        let executor = config.executor().unwrap();
        MockSetup {
            config,
            executor,
            programs,
            outputs: output_paths,
            term_count: 3,
        }
    }

    pub(crate) fn make_echo<P, Q>(program: P, output: Q) -> Result<(), Error>
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        if cfg!(unix) {
            let code = format!(
                "#!/bin/bash\necho -n \"$0 $@\" >> {}",
                output.as_ref().display()
            );
            std::fs::write(&program, &code)?;
            std::fs::set_permissions(&program, Permissions::from_mode(0o744)).unwrap();
            Ok(())
        } else {
            Err("this function is only supported on UNIX systems".into())
        }
    }

    #[test]
    fn test_make_echo() {
        let tmp = TempDir::new("echo").unwrap();
        let echo = tmp.path().join("e");
        let output = tmp.path().join("output");
        make_echo(&echo, &output).unwrap();
        let executor = super::executor::CustomPathExecutor::try_from(tmp.path()).unwrap();
        executor
            .command("e", &["arg1", "--a", "arg2"])
            .command()
            .status()
            .unwrap();
        let output_text = std::fs::read_to_string(&output).unwrap();
        assert_eq!(output_text, format!("{} arg1 --a arg2", echo.display()));
    }

    #[test]
    fn test_execute_failed_to_start() {
        struct MockCommand {};
        impl MockCommand {
            fn status(&self) -> Result<std::process::ExitStatus, &'static str> {
                Err("Oops")
            }
        }
        let f = || -> Result<(), Error> {
            execute!(MockCommand{}; "err");
            Ok(())
        };
        assert_eq!(f().err(), Some(Error::from("Oops")));
    }

}
