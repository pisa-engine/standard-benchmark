//! Objects and functions dealing with retrieving and preparing PISA executables.

extern crate boolinator;
extern crate downcast_rs;
extern crate failure;

use super::command::ExtCommand;
use super::config::*;
use super::error::Error;
use super::executor::*;
use super::{execute, Stage};
use crate::config::FromYaml;
use boolinator::Boolinator;
use downcast_rs::{impl_downcast, Downcast};
use failure::ResultExt;
use log::warn;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use yaml_rust::Yaml;

/// Defines how to acquire PISA programs for later execution.
pub trait PisaSource: Debug + Downcast {
    /// Initially, a source only holds information. At this stage, it runs
    /// any processes required to acquire PISA executables, and returns an
    /// executor object.
    ///
    /// # Example
    ///
    /// In the following example, the code in the last line will clone
    /// the repository and build the source code (unless `config` suppresses
    /// this stage).
    /// ```
    /// # extern crate stdbench;
    /// # use stdbench::source::*;
    /// # use stdbench::config::*;
    /// # use std::path::PathBuf;
    /// let source = GitSource::new(
    ///     "https://github.com/pisa-engine/pisa.git",
    ///     "master"
    /// );
    /// let config = Config::new(PathBuf::from("/workdir"), Box::new(source.clone()));
    /// let executor = source.executor(&config);
    /// ```
    ///
    /// Typically, however, you would directly run `executor()` method of `config`,
    /// which will internally run the function of `source`:
    /// ```
    /// # extern crate stdbench;
    /// # use stdbench::source::*;
    /// # use stdbench::config::*;
    /// # use std::path::PathBuf;
    /// # let source = GitSource::new(
    /// #     "https://github.com/pisa-engine/pisa.git",
    /// #     "master"
    /// # );
    /// # let config = Config::new(PathBuf::from("/workdir"), Box::new(source.clone()));
    /// let executor = config.executor();
    /// ```
    fn executor(&self, config: &Config) -> Result<Box<dyn PisaExecutor>, Error>;
}
impl_downcast!(PisaSource);
impl dyn PisaSource {
    fn parse_git_source(yaml: &Yaml) -> Result<GitSource, Error> {
        match (yaml["url"].as_str(), yaml["branch"].as_str()) {
            (None, _) => Err("missing source.url".into()),
            (_, None) => Err("missing source.branch".into()),
            (Some(url), Some(branch)) => Ok(GitSource::new(url, branch)),
        }
    }

    fn parse_path_source(yaml: &Yaml) -> Result<CustomPathSource, Error> {
        match yaml["path"].as_str() {
            None => Err("missing source.path".into()),
            Some(path) => Ok(CustomPathSource::from(path)),
        }
    }

    fn parse_docker_source(yaml: &Yaml) -> Result<DockerSource, Error> {
        match yaml["tag"].as_str() {
            None => Err("missing source.tag".into()),
            Some(tag) => Ok(DockerSource {
                tag: String::from(tag),
            }),
        }
    }

    /// Constructs `PisaSource` object from a YAML object.
    ///
    /// ```
    /// extern crate yaml_rust;
    /// # extern crate stdbench;
    /// # use stdbench::source::*;
    /// let yaml = yaml_rust::YamlLoader::load_from_str("
    /// type: git
    /// url: http://git.url
    /// branch: master").unwrap();
    /// let source = PisaSource::parse(&yaml[0]);
    /// assert_eq!(
    ///     source.unwrap().downcast_ref::<GitSource>(),
    ///     Some(&GitSource::new("http://git.url", "master"))
    /// );
    /// ```
    pub fn parse(yaml: &Yaml) -> Result<Box<dyn PisaSource>, Error> {
        match yaml["type"].as_str() {
            Some(typ) => match typ {
                "git" => Ok(Box::new(PisaSource::parse_git_source(&yaml)?)),
                "path" => Ok(Box::new(PisaSource::parse_path_source(&yaml)?)),
                "docker" => Ok(Box::new(PisaSource::parse_docker_source(&yaml)?)),
                typ => Err(format!("unknown source type: {}", typ).into()),
            },
            None => Err("missing or corrupted source.type".into()),
        }
    }
}

#[allow(clippy::use_self)]
impl FromYaml for Box<dyn PisaSource> {
    fn from_yaml(yaml: &Yaml) -> Result<Self, Error> {
        match yaml.parse_field::<String>("type")?.as_ref() {
            "git" => Ok(Box::new(PisaSource::parse_git_source(&yaml)?)),
            "path" => Ok(Box::new(PisaSource::parse_path_source(&yaml)?)),
            "docker" => Ok(Box::new(PisaSource::parse_docker_source(&yaml)?)),
            typ => Err(format!("unknown source type: {}", typ).into()),
        }
    }
}

/// Defines a path where the PISA executables already exist.
///
/// # Example
///
/// Assuming you cloned PISA repository in `/home/user/`, then you might
/// define the following source:
/// ```
/// # extern crate stdbench;
/// # use stdbench::source::*;
/// let source = CustomPathSource::from("/home/user/pisa/build/bin");
/// ```
#[derive(Debug, PartialEq, Clone)]
pub struct CustomPathSource {
    bin: PathBuf,
}
impl<P> From<P> for CustomPathSource
where
    P: AsRef<Path>,
{
    fn from(bin: P) -> Self {
        Self {
            bin: bin.as_ref().to_path_buf(),
        }
    }
}
impl PisaSource for CustomPathSource {
    fn executor(&self, config: &Config) -> Result<Box<dyn PisaExecutor>, Error> {
        let bin = if self.bin.is_absolute() {
            self.bin.clone()
        } else {
            config.workdir.join(&self.bin)
        };
        Ok(Box::new(CustomPathExecutor::try_from(bin)?))
    }
}

/// Git-based source. The produced executor will (unless suppressed) clone, configure,
/// and build the code.
#[derive(Debug, PartialEq, Clone)]
pub struct GitSource {
    url: String,
    branch: String,
}
impl GitSource {
    /// Defines a git repository cloned from `url` on branch `branch`.
    pub fn new(url: &str, branch: &str) -> Self {
        Self {
            url: String::from(url),
            branch: String::from(branch),
        }
    }
}
impl PisaSource for GitSource {
    fn executor(&self, config: &Config) -> Result<Box<dyn PisaExecutor>, Error> {
        init_git(config, self.url.as_ref(), self.branch.as_ref())
    }
}

/// **Unimplemented**: A Docker-based source.
#[derive(Debug, PartialEq)]
pub struct DockerSource {
    tag: String,
}
impl PisaSource for DockerSource {
    #[cfg_attr(tarpaulin, skip)]
    fn executor(&self, _config: &Config) -> Result<Box<dyn PisaExecutor>, Error> {
        unimplemented!();
    }
}

fn process(args: &'static str) -> ExtCommand {
    let mut args = args.split(' ');
    ExtCommand::new(args.next().unwrap()).args(args.collect::<Vec<&str>>())
}

fn init_git(config: &Config, url: &str, branch: &str) -> Result<Box<dyn PisaExecutor>, Error> {
    let dir = config.workdir.join("pisa");
    if !dir.exists() {
        let clone = ExtCommand::new("git").args(&["clone", &url, dir.to_str().unwrap()]);
        execute!(clone; "cloning failed");
    };
    let build_dir = dir.join("build");
    create_dir_all(&build_dir).context("Could not create build directory")?;

    if config.is_suppressed(Stage::Compile) {
        warn!("Compilation has been suppressed");
    } else {
        let checkout = ExtCommand::new("git").args(&["checkout", branch]);
        execute!(checkout.current_dir(&dir); "checkout failed");
        let cmake = process("cmake -DCMAKE_BUILD_TYPE=Release ..");
        execute!(cmake.current_dir(&build_dir); "cmake failed");
        let build = process("cmake --build .");
        execute!(build.current_dir(&build_dir); "build failed");
    }
    let executor = CustomPathExecutor::try_from(build_dir.join("bin"))?;
    Ok(Box::new(executor))
}

#[cfg(test)]
mod tests;
