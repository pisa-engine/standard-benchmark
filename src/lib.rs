#[macro_use]
#[allow(unused_imports)]
extern crate json;
extern crate experiment;

use experiment::process::Process;
use experiment::Verbosity;
use log::{error, info};
use std::fmt::Display;
use std::fs::create_dir_all;
use std::{fmt, process};

pub mod config;
pub mod executor;

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
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum Stage {
    Compile,
    BuildIndex,
    ParseCollection,
}
impl Stage {
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
        write!(f, "{:?}", self)
    }
}

/// Prints the passed command and returns it back.
pub fn printed(cmd: Process) -> Process {
    info!("=> {}", cmd.display(Verbosity::Verbose));
    // TODO: why is info not working?
    println!("EXEC - {}", cmd.display(Verbosity::Verbose));
    cmd
}

/// Prints out the error with the logger and exits the program.
pub fn exit_gracefully<E: Display, R>(e: E) -> R {
    error!("{}", e);
    // TODO: why is error not working?
    println!("ERROR - {}", e);
    process::exit(1);
}
