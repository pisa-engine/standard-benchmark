extern crate boolinator;
extern crate downcast_rs;

use super::config::*;
use super::executor::*;
use super::*;
use boolinator::Boolinator;
use downcast_rs::Downcast;
use log::warn;
use std::fmt::Debug;
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
    fn executor(&self, config: &Config) -> Result<Box<PisaExecutor>, Error>;
}
impl_downcast!(PisaSource);
impl PisaSource {
    fn parse_git_source(yaml: &Yaml) -> Result<GitSource, Error> {
        match (yaml["url"].as_str(), yaml["branch"].as_str()) {
            (None, _) => fail!("missing source.url"),
            (_, None) => fail!("missing source.branch"),
            (Some(url), Some(branch)) => Ok(GitSource {
                url: String::from(url),
                branch: String::from(branch),
            }),
        }
    }

    fn parse_path_source(yaml: &Yaml) -> Result<CustomPathSource, Error> {
        match yaml["path"].as_str() {
            None => fail!("missing source.path"),
            Some(path) => Ok(CustomPathSource::new(path)),
        }
    }

    fn parse_docker_source(yaml: &Yaml) -> Result<DockerSource, Error> {
        match yaml["tag"].as_str() {
            None => fail!("missing source.tag"),
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
    pub fn parse(yaml: &Yaml) -> Result<Box<PisaSource>, Error> {
        match yaml["type"].as_str() {
            Some(typ) => match typ {
                "git" => Ok(Box::new(PisaSource::parse_git_source(&yaml)?)),
                "path" => Ok(Box::new(PisaSource::parse_path_source(&yaml)?)),
                "docker" => Ok(Box::new(PisaSource::parse_docker_source(&yaml)?)),
                typ => fail!("unknown source type: {}", typ),
            },
            None => fail!("missing or corrupted source.type"),
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
/// let source = CustomPathSource::new("/home/user/pisa/build/bin");
/// ```
#[derive(Debug, PartialEq, Clone)]
pub struct CustomPathSource {
    pub bin: PathBuf,
}
impl CustomPathSource {
    pub fn new<P: AsRef<Path>>(bin: P) -> CustomPathSource {
        CustomPathSource {
            bin: bin.as_ref().to_path_buf(),
        }
    }
}
impl PisaSource for CustomPathSource {
    fn executor(&self, config: &Config) -> Result<Box<PisaExecutor>, Error> {
        let bin = if self.bin.is_absolute() {
            self.bin.clone()
        } else {
            config.workdir.join(&self.bin)
        };
        Ok(Box::new(CustomPathExecutor::new(bin)?))
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct GitSource {
    pub url: String,
    pub branch: String,
}
impl GitSource {
    pub fn new(url: &str, branch: &str) -> GitSource {
        GitSource {
            url: String::from(url),
            branch: String::from(branch),
        }
    }
}
impl PisaSource for GitSource {
    fn executor(&self, config: &Config) -> Result<Box<PisaExecutor>, Error> {
        init_git(config, self.url.as_ref(), self.branch.as_ref())
    }
}

#[derive(Debug, PartialEq)]
pub struct DockerSource {
    tag: String,
}
impl PisaSource for DockerSource {
    fn executor(&self, _config: &Config) -> Result<Box<PisaExecutor>, Error> {
        unimplemented!();
    }
}

fn process(args: &'static str) -> Process {
    let mut args = args.split(' ');
    Process::new(args.next().unwrap(), args.collect::<Vec<&str>>())
}

fn init_git(config: &Config, url: &str, branch: &str) -> Result<Box<PisaExecutor>, Error> {
    let dir = config.workdir.join("pisa");
    if !dir.exists() {
        let clone = Process::new("git", &["clone", &url, dir.to_str().unwrap()]);
        execute!(printed(clone).command(); "cloning failed");
    };
    let build_dir = dir.join("build");
    create_dir_all(&build_dir).map_err(|e| Error(format!("{}", e)))?;

    if config.is_suppressed(Stage::Compile) {
        warn!("Compilation has been suppressed");
    } else {
        let checkout = Process::new("git", &["checkout", branch]);
        execute!(printed(checkout).command().current_dir(&dir); "checkout failed");
        let cmake = process("cmake -DCMAKE_BUILD_TYPE=Release ..");
        execute!(printed(cmake).command().current_dir(&build_dir); "cmake failed");
        let build = process("cmake --build .");
        execute!(printed(build).command().current_dir(&build_dir); "build failed");
    }
    let executor = CustomPathExecutor::new(build_dir.join("bin"))?;
    Ok(Box::new(executor))
}

#[cfg(test)]
mod tests {
    extern crate downcast_rs;
    extern crate tempdir;

    use super::*;
    use tempdir::TempDir;
    use yaml_rust::YamlLoader;

    #[test]
    fn test_parse_git_source() {
        assert_eq!(
            PisaSource::parse_git_source(
                &YamlLoader::load_from_str(
                    r#"
                        type: git
                        branch: dev
                        url: https://github.com/pisa-engine/pisa.git
                    "#
                )
                .unwrap()[0]
            ),
            Ok(GitSource {
                url: String::from("https://github.com/pisa-engine/pisa.git"),
                branch: String::from("dev")
            })
        );
        assert_eq!(
            PisaSource::parse_git_source(
                &YamlLoader::load_from_str(
                    r#"
                        type: git
                        url: https://github.com/pisa-engine/pisa.git
                    "#
                )
                .unwrap()[0]
            ),
            fail!("missing source.branch")
        );
        assert_eq!(
            PisaSource::parse_git_source(
                &YamlLoader::load_from_str(
                    r#"
                        type: git
                    "#
                )
                .unwrap()[0]
            ),
            fail!("missing source.url")
        );
    }

    #[test]
    fn test_parse_docker_source() {
        assert_eq!(
            PisaSource::parse_docker_source(
                &YamlLoader::load_from_str(
                    r#"
                        type: docker
                        tag: latest
                    "#
                )
                .unwrap()[0]
            )
            .unwrap(),
            DockerSource {
                tag: String::from("latest")
            }
        );
        assert_eq!(
            PisaSource::parse_docker_source(
                &YamlLoader::load_from_str(
                    r#"
                        type: docker
                    "#
                )
                .unwrap()[0]
            ),
            fail!("missing source.tag")
        );
    }

    #[test]
    fn test_parse_source() {
        assert_eq!(
            PisaSource::parse(
                &YamlLoader::load_from_str(
                    r#"
                        type: docker
                        tag: latest
                    "#
                )
                .unwrap()[0]
            )
            .unwrap()
            .downcast_ref::<DockerSource>(),
            Some(&DockerSource {
                tag: String::from("latest")
            })
        );
        assert_eq!(
            PisaSource::parse(
                &YamlLoader::load_from_str(
                    r#"
                        type: git
                        branch: dev
                        url: https://github.com/pisa-engine/pisa.git
                    "#
                )
                .unwrap()[0]
            )
            .unwrap()
            .downcast_ref::<GitSource>(),
            Some(&GitSource {
                url: String::from("https://github.com/pisa-engine/pisa.git"),
                branch: String::from("dev")
            })
        );
        assert_eq!(
            PisaSource::parse(
                &YamlLoader::load_from_str(
                    r#"
                        type: 112
                    "#
                )
                .unwrap()[0]
            )
            .err(),
            Some(Error::new("missing or corrupted source.type"))
        );
        assert_eq!(
            PisaSource::parse(
                &YamlLoader::load_from_str(
                    r#"
                        type: "foo"
                    "#
                )
                .unwrap()[0]
            )
            .err(),
            Some(Error::new("unknown source type: foo"))
        );
        assert_eq!(
            PisaSource::parse(
                &YamlLoader::load_from_str(
                    r#"
                        type: path
                        path: "pisa/build/bin"
                    "#
                )
                .unwrap()[0]
            )
            .unwrap()
            .downcast_ref::<CustomPathSource>(),
            Some(&CustomPathSource::new("pisa/build/bin"))
        );
        assert_eq!(
            PisaSource::parse_path_source(
                &YamlLoader::load_from_str(
                    r#"
                        type: path
                    "#
                )
                .unwrap()[0]
            )
            .err(),
            Some(Error::new("missing source.path"))
        );
        assert_eq!(
            PisaSource::parse_path_source(
                &YamlLoader::load_from_str(
                    r#"
                        type: path
                        path: {}
                    "#
                )
                .unwrap()[0]
            )
            .err(),
            Some(Error::new("missing source.path"))
        );
    }

    #[test]
    fn test_custom_path_source_executor() {
        let tmp = TempDir::new("test_custom_path_source_executor").unwrap();
        let bin = tmp.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        assert_eq!(
            Config::new(tmp.path(), Box::new(CustomPathSource::new("bin")))
                .executor()
                .unwrap()
                .downcast_ref::<CustomPathExecutor>()
                .unwrap()
                .path(),
            bin.as_path()
        );
    }

    #[test]
    fn test_custom_path_source_fail() {
        let source = CustomPathSource::new("/nonexistent-path");
        let config = Config::new("workdir", Box::new(source));
        let executor = config.executor().err();
        assert_eq!(
            executor,
            Some(Error::new(
                "Failed to construct executor: not a directory: /nonexistent-path"
            ))
        );
    }
}
