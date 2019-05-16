extern crate downcast_rs;
extern crate experiment;
extern crate json;

use downcast_rs::impl_downcast;
use experiment::process::Process;
use experiment::Verbosity;
use log::debug;
use std::fmt;
use std::fs::create_dir_all;
use std::path::Path;

pub mod build;
pub mod config;
pub mod executor;
pub mod source;

#[cfg_attr(tarpaulin, skip)]
#[derive(Debug, PartialEq)]
pub struct Error(pub String);
impl Error {
    pub fn new(msg: &str) -> Error {
        Error(String::from(msg))
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error(format!("{:?}", e))
    }
}

/// Available stages of the experiment.
#[cfg_attr(tarpaulin, skip)]
#[derive(Debug, PartialEq, Eq, Hash)]
/// # Examples
///
/// All names are lowercase:
///
/// ```
/// # extern crate stdbench;
/// # use::stdbench::*;
/// assert_eq!(Stage::from_name("compile"), Some(Stage::Compile));
/// assert_eq!(Stage::from_name("build"), Some(Stage::BuildIndex));
/// assert_eq!(Stage::from_name("parse"), Some(Stage::ParseCollection));
/// assert_eq!(Stage::from_name("invert"), Some(Stage::Invert));
/// assert_eq!(Stage::from_name("?"), None);
/// assert_eq!("compile", format!("{}", Stage::Compile));
/// assert_eq!("build", format!("{}", Stage::BuildIndex));
/// assert_eq!("parse", format!("{}", Stage::ParseCollection));
/// assert_eq!("invert", format!("{}", Stage::Invert));
/// ```
pub enum Stage {
    Compile,
    BuildIndex,
    ParseCollection,
    Invert,
}
impl Stage {
    /// Parse string and return a stage enum if string correct.
    pub fn from_name(name: &str) -> Option<Stage> {
        match name.to_lowercase().as_ref() {
            "compile" => Some(Stage::Compile),
            "build" => Some(Stage::BuildIndex),
            "parse" => Some(Stage::ParseCollection),
            "invert" => Some(Stage::Invert),
            _ => None,
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
/// # use std::path::Path;
/// # use tempdir::TempDir;
/// assert_eq!(
///     ensure_parent_exists(Path::new("/")),
///     Err(Error::new("cannot access parent of path: /"))
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
        .ok_or_else(|| Error(format!("cannot access parent of path: {}", path.display())))?;
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
/// # use stdbench::Error;
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
        $cmd.status()
            .map_err(|e| Error(format!("{}", e)))?
            .success()
            .ok_or(Error::new($errmsg))?;
    }};
}

/// # Example
///
/// ```
/// # #[macro_use]
/// # extern crate stdbench;
/// # use stdbench::Error;
/// # fn main() {
/// fn always_fail_with(msg: &str) -> Result<(), Error> {
///     fail!("Failed with message: {}", msg)
/// }
///
/// assert_eq!(always_fail_with("oops"), Err(Error::new("Failed with message: oops")));
/// # }
/// ```
#[macro_export]
macro_rules! fail {
    ($($arg:tt)+) => (
        Err(Error(format!($($arg)+)))
    )
}

#[cfg(test)]
mod tests {
    extern crate tempdir;

    use super::executor::PisaExecutor;
    use super::*;
    use boolinator::Boolinator;
    use std::fs::Permissions;
    use std::os::unix::fs::PermissionsExt;
    use tempdir::TempDir;

    pub(crate) fn make_echo<P, Q>(program: P, output: Q) -> Result<(), Error>
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        if cfg!(unix) {
            let code = format!(
                "#!/bin/bash\necho -n \"$0 $@\" > {}",
                output.as_ref().display()
            );
            std::fs::write(&program, &code)?;
            std::fs::set_permissions(&program, Permissions::from_mode(0o744)).unwrap();
            Ok(())
        } else {
            fail!("this function is only supported on UNIX systems")
        }
    }

    #[test]
    fn test_make_echo() {
        let tmp = TempDir::new("echo").unwrap();
        let echo = tmp.path().join("e");
        let output = tmp.path().join("output");
        make_echo(&echo, &output).unwrap();
        let executor = super::executor::CustomPathExecutor::new(tmp.path()).unwrap();
        executor
            .command("e", &["arg1", "--a", "arg2"])
            .command()
            .status()
            .unwrap();
        let output_text = std::fs::read_to_string(&output).unwrap();
        assert_eq!(output_text, format!("{} arg1 --a arg2", echo.display()));
    }

    #[test]
    fn test_error() {
        let error = Error::new("error message");
        assert_eq!(error, Error(String::from("error message")));
        assert_eq!(format!("{}", error), String::from("error message"));
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
        assert_eq!(f().err(), Some(Error::new("Oops")));
    }

}
