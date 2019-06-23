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
    process::{Command, ExitStatus, Output, Stdio},
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
    pub fn new<S: AsRef<OsStr>>(program: S) -> Self {
        Self {
            verbosity: Verbosity::Verbose,
            command: Command::new(program),
            pipeline: vec![],
        }
    }
    /// Constructs a new verbose command from the argument.
    pub fn from(command: Command) -> Self {
        Self {
            verbosity: Verbosity::Verbose,
            command,
            pipeline: vec![],
        }
    }

    /// Adds a single argument.
    pub fn arg<S: AsRef<OsStr>>(mut self, arg: S) -> Self {
        if let Some(cmd) = self.pipeline.last_mut() {
            cmd.arg(arg);
        } else {
            self.command.arg(arg);
        }
        self
    }

    /// Adds a sequence of arguments.
    pub fn args<I, S>(mut self, args: I) -> Self
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
    pub fn current_dir<P: AsRef<Path>>(mut self, dir: P) -> Self {
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
        for mut cmd in cmds {
            cmd.spawn()?;
            drop(cmd);
        }
        Ok(last)
    }

    /// Executes the command and waits for the output.
    pub fn output(mut self) -> Result<Output, Error> {
        if let Verbosity::Verbose = &self.verbosity {
            debug!("[EXEC] {}", &self);
        }
        if self.pipeline.is_empty() {
            Ok(self.command.output()?)
        } else {
            Ok(self.spawn()?.output()?)
        }
    }

    /// Executes the command and waits for the output.
    pub fn status(mut self) -> Result<ExitStatus, Error> {
        if let Verbosity::Verbose = &self.verbosity {
            debug!("[EXEC] {}", &self);
        }
        if self.pipeline.is_empty() {
            Ok(self.command.status()?)
        } else {
            Ok(self.spawn()?.status()?)
        }
    }

    /// Configuration for the child process's standard input (stdin) handle.
    pub fn stdin<T: Into<Stdio>>(mut self, cfg: T) -> Self {
        if let Some(cmd) = self.pipeline.last_mut() {
            cmd.stdin(cfg);
        } else {
            self.command.stdin(cfg);
        }
        self
    }

    /// Pipe another command.
    pub fn pipe_command(mut self, mut command: Self) -> Self {
        let (reader, writer) = pipe().expect("Failed opening a pipe");
        if let Some(cmd) = self.pipeline.last_mut() {
            cmd.stdout(writer);
        } else {
            self.command.stdout(writer);
        }
        command = command.stdin(reader);
        let Self {
            command: first_cmd,
            pipeline: rest,
            ..
        } = command;
        self.pipeline.push(first_cmd);
        for cmd in rest {
            self.pipeline.push(cmd);
        }
        self
    }

    /// Pipe another command.
    pub fn pipe_new<S: AsRef<OsStr>>(mut self, program: S) -> Self {
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
    pub fn mute(mut self) -> Self {
        self.verbosity = Verbosity::Silent;
        self
    }
}

impl From<Command> for ExtCommand {
    fn from(command: Command) -> Self {
        Self {
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
