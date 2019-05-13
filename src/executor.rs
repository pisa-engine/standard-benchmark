extern crate boolinator;
extern crate downcast_rs;
extern crate experiment;

use super::config::*;
use super::*;
use boolinator::Boolinator;
use downcast_rs::Downcast;
use experiment::process::Process;
use log::warn;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use yaml_rust::Yaml;

pub trait PisaSource: Debug + Downcast {
    fn executor(&self, config: &Config) -> Result<Box<PisaExecutor>, Error>;
}
impl_downcast!(PisaSource);
impl PisaSource {
    fn parse_git_source(yaml: &Yaml) -> Result<GitSource, Error> {
        match (yaml["url"].as_str(), yaml["branch"].as_str()) {
            (None, _) => Err(Error::new("missing source.url")),
            (_, None) => Err(Error::new("missing source.branch")),
            (Some(url), Some(branch)) => Ok(GitSource {
                url: String::from(url),
                branch: String::from(branch),
            }),
        }
    }

    fn parse_docker_source(yaml: &Yaml) -> Result<DockerSource, Error> {
        match yaml["tag"].as_str() {
            None => Err(Error::new("missing source.tag")),
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
    /// # use stdbench::executor::*;
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
                "docker" => Ok(Box::new(PisaSource::parse_docker_source(&yaml)?)),
                typ => Err(Error(format!("unknown source type: {}", typ))),
            },
            None => Err(Error::new("missing or corrupted source.type")),
        }
    }
}

#[derive(Debug, PartialEq)]
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

/// Implementations of this trait execute PISA tools.
pub trait PisaExecutor: Debug + Downcast {
    /// Builds a process object for a program with given arguments.
    fn command(&self, program: &str, args: &[&str]) -> Process;
}
impl_downcast!(PisaExecutor);

/// This executor simply executes the commands as passed,
/// as if they were on the system path.
#[derive(Default, Debug)]
pub struct SystemPathExecutor {}
impl SystemPathExecutor {
    pub fn new() -> SystemPathExecutor {
        SystemPathExecutor {}
    }
}
impl PisaExecutor for SystemPathExecutor {
    fn command(&self, program: &str, args: &[&str]) -> Process {
        Process::new(program, args)
    }
}

/// An executor using compiled code from git repository.
#[derive(Debug, PartialEq)]
pub struct GitPisaExecutor {
    bin: PathBuf,
}
impl GitPisaExecutor {
    pub fn new<P>(bin_path: P) -> Result<GitPisaExecutor, Error>
    where
        P: AsRef<Path>,
    {
        if bin_path.as_ref().is_dir() {
            Ok(GitPisaExecutor {
                bin: bin_path.as_ref().to_path_buf(),
            })
        } else {
            Err(Error(format!(
                "Failed to construct git executor: not a directory: {}",
                bin_path.as_ref().display()
            )))
        }
    }
}
impl PisaExecutor for GitPisaExecutor {
    fn command(&self, program: &str, args: &[&str]) -> Process {
        Process::new(&self.bin.join(program).to_str().unwrap().to_string(), args)
    }
}

fn init_git(config: &Config, url: &str, branch: &str) -> Result<Box<PisaExecutor>, Error> {
    let dir = config.workdir.join("pisa");
    if !dir.exists() {
        let clone = Process::new("git", &["clone", &url, dir.to_str().unwrap()]);
        checked_execute!(printed(clone));
        dir.join("CMakeLists.txt")
            .exists()
            .ok_or(Error::new("cloning failed"))?;
    };
    let build_dir = dir.join("build");

    if config.is_suppressed(Stage::Compile) {
        warn!("Compilation has been suppressed");
    } else {
        let checkout = Process::new("git", &["-C", &dir.to_str().unwrap(), "checkout", branch]);
        checked_execute!(printed(checkout));
        create_dir_all(&build_dir).map_err(|e| Error(format!("{}", e)))?;
        let cmake = Process::new(
            "cmake",
            &[
                "-DCMAKE_BUILD_TYPE=Release",
                "-S",
                &dir.to_str().unwrap(),
                "-B",
                &build_dir.to_str().unwrap(),
            ],
        );
        checked_execute!(printed(cmake));
        let build = Process::new("cmake", &["--build", &build_dir.to_str().unwrap()]);
        checked_execute!(printed(build));
    }
    let executor = GitPisaExecutor::new(build_dir.join("bin"))?;
    Ok(Box::new(executor))
}

#[cfg(test)]
#[cfg_attr(tarpaulin, skip)]
mod tests {
    extern crate downcast_rs;
    extern crate tempdir;

    use super::*;
    use std::fs::create_dir_all;
    use std::process::Command;
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
            Err(Error::new("missing source.branch"))
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
            Err(Error::new("missing source.url"))
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
            Err(Error::new("missing source.tag"))
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
    }

    #[test]
    fn test_init_git_failed_clone() {
        let tmp = TempDir::new("tmp").unwrap();
        let workdir = tmp.path().join("work");
        create_dir_all(&workdir).unwrap();

        let conf = Config::new(&workdir, Box::new(GitSource::new("http://url", "master")));
        assert_eq!(
            conf.source.executor(&conf).err(),
            Some(Error::new("cloning failed"))
        );
    }

    fn run_from(dir: PathBuf) -> impl Fn(&'static str) -> () {
        move |args: &'static str| {
            let mut args = args.split(" ").into_iter();
            Command::new(args.next().unwrap())
                .current_dir(&dir)
                .args(args.collect::<Vec<&str>>())
                .status()
                .expect("failed git command");
            ()
        }
    }

    fn set_up_git() -> (TempDir, PathBuf, PathBuf) {
        let tmp = TempDir::new("tmp").unwrap();
        let workdir = tmp.path().join("work");
        let origin_dir = tmp.path().join("origin");
        create_dir_all(&origin_dir).unwrap();
        let run = run_from(origin_dir.clone());
        run("git init");
        let cmakelists = "cmake_minimum_required(VERSION 3.14)
             add_custom_target(build-time-make-directory ALL
             COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/bin)";
        std::fs::write(origin_dir.join("CMakeLists.txt"), &cmakelists)
            .expect("Unable to write file");
        run("git add CMakeLists.txt");
        run("git commit -m \"c\"");
        (tmp, workdir, origin_dir)
    }

    #[test]
    fn test_init_git() {
        let (_tmp, workdir, origin_dir) = set_up_git();
        let conf = Config::new(
            &workdir,
            Box::new(GitSource::new(&origin_dir.to_str().unwrap(), "master")),
        );
        assert_eq!(
            conf.source
                .executor(&conf)
                .unwrap()
                .downcast_ref::<GitPisaExecutor>(),
            GitPisaExecutor::new(
                workdir
                    .join("pisa")
                    .join("build")
                    .join("bin")
                    .to_str()
                    .unwrap()
            )
            .ok()
            .as_ref()
        );
    }
}
