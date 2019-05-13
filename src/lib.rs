#[macro_use]
#[allow(unused_imports)]
extern crate json;
extern crate experiment;
#[macro_use]
#[allow(unused_imports)]
extern crate downcast_rs;

use experiment::process::Process;
use experiment::Verbosity;
use log::info;
use std::fmt;
use std::fs::create_dir_all;

pub mod config;
pub mod executor;

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
/// assert_eq!(Stage::from_name("?"), None);
/// assert_eq!("compile", format!("{}", Stage::Compile));
/// assert_eq!("build", format!("{}", Stage::BuildIndex));
/// assert_eq!("parse", format!("{}", Stage::ParseCollection));
/// ```
pub enum Stage {
    Compile,
    BuildIndex,
    ParseCollection,
}
impl Stage {
    /// Parse string and return a stage enum if string correct.
    pub fn from_name(name: &str) -> Option<Stage> {
        match name.to_lowercase().as_ref() {
            "compile" => Some(Stage::Compile),
            "build" => Some(Stage::BuildIndex),
            "parse" => Some(Stage::ParseCollection),
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
            }
        )
    }
}

/// Prints the passed command and returns it back.
#[cfg_attr(tarpaulin, skip)]
pub fn printed(cmd: Process) -> Process {
    info!("=> {}", cmd.display(Verbosity::Verbose));
    // TODO: why is info not working?
    println!("EXEC - {}", cmd.display(Verbosity::Verbose));
    cmd
}

#[cfg(test)]
#[cfg_attr(tarpaulin, skip)]
mod tests {
    use super::*;

    #[test]
    fn test_error() {
        let error = Error::new("error message");
        assert_eq!(error, Error(String::from("error message")));
        assert_eq!(format!("{}", error), String::from("error message"));
    }

}

#[macro_export]
macro_rules! checked_execute {
    ($cmd:expr) => {{
        $cmd.execute()
            .map_err(|e| Error(format!("{}", e)))?
            .success()
            .ok_or(Error::new(""))?;
    }};
}
