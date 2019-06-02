//! This module extends the standard `Command` with abilities
//! to print and pipe commands.

extern crate lazy_static;
extern crate log;
extern crate os_pipe;
extern crate regex;

use crate::error::Error;
use lazy_static::lazy_static;
use log::debug;
use os_pipe::pipe;
use regex::Regex;
use std::{
    collections::VecDeque,
    ffi::OsStr,
    fmt,
    path::Path,
    process::{Command, ExitStatus, Output},
};

enum Verbosity {
    Verbose,
    Silent,
}

/// Extension of `Command`.
pub struct ExtCommand {
    verbosity: Verbosity,
    command: Command,
    pipeline: Vec<Command>,
}
impl ExtCommand {
    /// Constructs a new verbose command.
    pub fn new<S: AsRef<OsStr>>(program: S) -> ExtCommand {
        ExtCommand {
            verbosity: Verbosity::Verbose,
            command: Command::new(program),
            pipeline: vec![],
        }
    }

    /// Adds a single argument.
    pub fn arg<S: AsRef<OsStr>>(mut self, arg: S) -> ExtCommand {
        if let Some(cmd) = self.pipeline.last_mut() {
            cmd.arg(arg);
        } else {
            self.command.arg(arg);
        }
        self
    }

    /// Adds a sequence of arguments.
    pub fn args<I, S>(mut self, args: I) -> ExtCommand
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        if let Some(cmd) = self.pipeline.last_mut() {
            cmd.args(args);
        } else {
            self.command.args(args);
        }
        self
    }

    /// Changes the directory from which the command will be executed.
    pub fn current_dir<P: AsRef<Path>>(mut self, dir: P) -> ExtCommand {
        if let Some(cmd) = self.pipeline.last_mut() {
            cmd.current_dir(dir);
        } else {
            self.command.current_dir(dir);
        }
        self
    }

    fn spawn(mut self) -> Result<Command, Error> {
        self.command.spawn()?;
        drop(self.command);
        let mut cmds = self.pipeline.into_iter().collect::<VecDeque<_>>();
        let last = cmds.pop_back().unwrap();
        for mut cmd in cmds.into_iter() {
            cmd.spawn()?;
            drop(cmd);
        }
        Ok(last)
    }

    /// Executes the command and waits for the output.
    pub fn output(mut self) -> Result<Output, Error> {
        match &self.verbosity {
            Verbosity::Verbose => debug!("[EXEC] {}", &self),
            _ => {}
        }
        if self.pipeline.is_empty() {
            Ok(self.command.output()?)
        } else {
            Ok(self.spawn()?.output()?)
        }
    }

    /// Executes the command and waits for the output.
    pub fn status(mut self) -> Result<ExitStatus, Error> {
        match &self.verbosity {
            Verbosity::Verbose => debug!("[EXEC] {}", &self),
            _ => {}
        }
        if self.pipeline.is_empty() {
            Ok(self.command.status()?)
        } else {
            Ok(self.spawn()?.status()?)
        }
    }

    /// Pipe another command.
    pub fn pipe_command(mut self, mut command: Command) -> ExtCommand {
        let (reader, writer) = pipe().expect("Failed opening a pipe");
        if let Some(cmd) = self.pipeline.last_mut() {
            cmd.stdout(writer);
        } else {
            self.command.stdout(writer);
        }
        command.stdin(reader);
        self.pipeline.push(command);
        self
    }

    /// Pipe another command.
    pub fn pipe_new<S: AsRef<OsStr>>(mut self, program: S) -> ExtCommand {
        let (reader, writer) = pipe().expect("Failed opening a pipe");
        if let Some(cmd) = self.pipeline.last_mut() {
            cmd.stdout(writer);
        } else {
            self.command.stdout(writer);
        }
        let mut new_command = Command::new(program);
        new_command.stdin(reader);
        self.pipeline.push(new_command);
        self
    }

    /// Turn off automatic printing.
    pub fn mute(mut self) -> ExtCommand {
        self.verbosity = Verbosity::Silent;
        self
    }
}

impl From<Command> for ExtCommand {
    fn from(command: Command) -> ExtCommand {
        ExtCommand {
            verbosity: Verbosity::Verbose,
            command,
            pipeline: vec![],
        }
    }
}

fn write_single_command(f: &mut fmt::Formatter, command: &Command) -> fmt::Result {
    lazy_static! {
        static ref RE: Regex = Regex::new(r#""([^"^\s]+)""#).unwrap();
    }
    let cmd = RE
        .captures_iter(format!("{:?}", command).as_ref())
        .map(|arg| arg.get(1).unwrap().as_str())
        .collect::<Vec<_>>()
        .join(" ");
    write!(f, "{}", cmd)
}

impl fmt::Display for ExtCommand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write_single_command(f, &self.command)?;
        for pc in &self.pipeline {
            write!(f, "\n    | ")?;
            write_single_command(f, &pc)?;
        }
        write!(f, "")
    }
}

#[cfg(test)]
mod tests;
