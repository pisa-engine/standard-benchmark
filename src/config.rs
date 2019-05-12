extern crate yaml_rust;

use super::{Error, Stage};
//use log::error;
use std::collections::HashSet;
use std::convert::From;
use std::fs::read_to_string;
use std::path::{Path, PathBuf};
use yaml_rust::{Yaml, YamlLoader};

/// Defines where the code comes from.
#[derive(Debug, PartialEq)]
pub enum CodeSource {
    Git { url: String, branch: String },
    Docker { tag: String },
}

impl CodeSource {
    fn parse_git_source(yaml: &Yaml) -> Result<CodeSource, Error> {
        match (yaml["url"].as_str(), yaml["branch"].as_str()) {
            (None, _) => Err(Error::new("missing source.url")),
            (_, None) => Err(Error::new("missing source.branch")),
            (Some(url), Some(branch)) => Ok(CodeSource::Git {
                url: String::from(url),
                branch: String::from(branch),
            }),
        }
    }
    fn parse_docker_source(yaml: &Yaml) -> Result<CodeSource, Error> {
        match yaml["tag"].as_str() {
            None => Err(Error::new("missing source.tag")),
            Some(tag) => Ok(CodeSource::Docker {
                tag: String::from(tag),
            }),
        }
    }
    /// Constructs `CodeSource` object from a YAML object.
    ///
    /// ```
    /// extern crate yaml_rust;
    /// # extern crate stdbench;
    /// # use stdbench::config::*;
    /// let yaml = yaml_rust::YamlLoader::load_from_str("
    /// type: git
    /// url: http://git.url
    /// branch: master").unwrap();
    /// let source = CodeSource::parse(&yaml[0]);
    /// assert_eq!(
    ///     source,
    ///     Ok(CodeSource::Git {
    ///         url: String::from("http://git.url"),
    ///         branch: String::from("master")
    ///     }
    /// ));
    /// ```
    pub fn parse(yaml: &Yaml) -> Result<CodeSource, Error> {
        match yaml["type"].as_str() {
            Some(typ) => match typ {
                "git" => Ok(CodeSource::parse_git_source(&yaml)?),
                "docker" => Ok(CodeSource::parse_docker_source(&yaml)?),
                typ => Err(Error(format!("unknown source type: {}", typ))),
            },
            None => Err(Error::new("missing or corrupted source.type")),
        }
    }
}

/// Configuration of a tested collection.
#[derive(Debug, PartialEq)]
pub struct CollectionConfig {
    pub name: String,
    pub description: Option<String>,
    pub collection_dir: PathBuf,
    pub forward_index: PathBuf,
    pub inverted_index: PathBuf,
}
impl CollectionConfig {
    /// Constructs a collection config from a YAML object.
    ///
    /// # Example
    ///
    /// ```
    /// extern crate yaml_rust;
    /// # extern crate stdbench;
    /// # use stdbench::config::*;
    /// # use std::path::PathBuf;
    /// let yaml = yaml_rust::YamlLoader::load_from_str("
    /// name: wapo
    /// description: WashingtonPost.v2
    /// collection_dir: /path/to/wapo
    /// forward_index: fwd/wapo
    /// inverted_index: /absolute/path/to/inv/wapo").unwrap();
    /// let conf = CollectionConfig::from_yaml(&yaml[0]);
    /// assert_eq!(
    ///     conf,
    ///     Ok(CollectionConfig {
    ///         name: String::from("wapo"),
    ///         description: Some(String::from("WashingtonPost.v2")),
    ///         collection_dir: PathBuf::from("/path/to/wapo"),
    ///         forward_index: PathBuf::from("fwd/wapo"),
    ///         inverted_index: PathBuf::from("/absolute/path/to/inv/wapo")
    ///     }
    /// ));
    /// ```
    pub fn from_yaml(yaml: &Yaml) -> Result<CollectionConfig, Error> {
        match (
            yaml["name"].as_str(),
            yaml["collection_dir"].as_str(),
            yaml["forward_index"].as_str(),
            yaml["inverted_index"].as_str(),
        ) {
            (None, _, _, _) => Err(Error::new("undefined name")),
            (_, None, _, _) => Err(Error::new("undefined collection_dir")),
            (Some(name), Some(collection_dir), fwd, inv) => Ok(CollectionConfig {
                name: name.to_string(),
                description: yaml["description"].as_str().map(String::from),
                collection_dir: PathBuf::from(collection_dir),
                forward_index: PathBuf::from(fwd.unwrap_or("fwd/wapo")),
                inverted_index: PathBuf::from(inv.unwrap_or("inv/wapo")),
            }),
        }
    }
}

/// Stores a full config for benchmark run.
pub struct Config {
    pub workdir: PathBuf,
    pub source: CodeSource,
    suppressed: HashSet<Stage>,
    pub collections: Vec<CollectionConfig>,
}
impl Config {
    fn new<P>(workdir: P, source: CodeSource) -> Config
    where
        P: AsRef<Path>,
    {
        Config {
            workdir: workdir.as_ref().to_path_buf(),
            source,
            suppressed: HashSet::new(),
            collections: Vec::new(),
        }
    }

    /// Load a config from a YAML file.
    ///
    /// # Example
    /// ```
    /// # extern crate stdbench;
    /// # use stdbench::config::*;
    /// # use std::path::PathBuf;
    /// match Config::from_file(PathBuf::from("config.yml")) {
    ///     Ok(config) => {}
    ///     Err(err) => {
    ///         println!("Couldn't read config");
    ///     }
    /// }
    /// ```
    pub fn from_file<P>(file: P) -> Result<Config, Error>
    where
        P: AsRef<Path>,
    {
        let content = read_to_string(&file)?;
        match YamlLoader::load_from_str(&content) {
            Ok(yaml) => match (yaml[0]["workdir"].as_str(), &yaml[0]["source"]) {
                (None, _) => Err(Error::new("missing or corrupted workdir")),
                (Some(workdir), source) => {
                    let source = CodeSource::parse(source)?;
                    let mut conf = Config::new(PathBuf::from(workdir), source);
                    match &yaml[0]["collections"] {
                        Yaml::Array(collections) => {
                            for collection in collections {
                                match conf.parse_collection(&collection) {
                                    Ok(coll_config) => {
                                        conf.collections.push(coll_config);
                                    }
                                    // TODO: Err(err) => error!("Unable to parse collection config: {}", err),
                                    Err(err) => println!(
                                        "ERROR - Unable to parse collection config: {}",
                                        err
                                    ),
                                }
                            }
                            if conf.collections.is_empty() {
                                Err(Error::new("no correct collection configurations found"))
                            } else {
                                Ok(conf)
                            }
                        }
                        _ => Err(Error::new("missing or corrupted collections config")),
                    }
                }
            },
            Err(_) => Err(Error::new("could not parse YAML file")),
        }
    }

    /// Adds a stage to be suppressed during experiment.
    pub fn suppress_stage(&mut self, stage: Stage) {
        self.suppressed.insert(stage);
    }

    /// Returns `true` if the given stage was suppressed in the config.
    pub fn is_suppressed(&self, stage: Stage) -> bool {
        self.suppressed.contains(&stage)
    }

    fn parse_collection(&self, yaml: &Yaml) -> Result<CollectionConfig, Error> {
        let mut collconf = CollectionConfig::from_yaml(yaml)?;
        if !collconf.forward_index.is_absolute() {
            collconf.forward_index = self.workdir.join(collconf.forward_index);
        }
        if !collconf.inverted_index.is_absolute() {
            collconf.inverted_index = self.workdir.join(collconf.inverted_index);
        }
        Ok(collconf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_git_source() {
        assert_eq!(
            CodeSource::parse_git_source(
                &YamlLoader::load_from_str(
                    r#"
                        type: git
                        branch: dev
                        url: https://github.com/pisa-engine/pisa.git
                    "#
                )
                .unwrap()[0]
            ),
            Ok(CodeSource::Git {
                url: String::from("https://github.com/pisa-engine/pisa.git"),
                branch: String::from("dev")
            })
        );
        assert_eq!(
            CodeSource::parse_git_source(
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
            CodeSource::parse_git_source(
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
            CodeSource::parse_docker_source(
                &YamlLoader::load_from_str(
                    r#"
                        type: docker
                        tag: latest
                    "#
                )
                .unwrap()[0]
            ),
            Ok(CodeSource::Docker {
                tag: String::from("latest")
            })
        );
        assert_eq!(
            CodeSource::parse_docker_source(
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
    fn test_parse() {
        assert_eq!(
            CodeSource::parse(
                &YamlLoader::load_from_str(
                    r#"
                        type: docker
                        tag: latest
                    "#
                )
                .unwrap()[0]
            ),
            Ok(CodeSource::Docker {
                tag: String::from("latest")
            })
        );
        assert_eq!(
            CodeSource::parse(
                &YamlLoader::load_from_str(
                    r#"
                        type: git
                        branch: dev
                        url: https://github.com/pisa-engine/pisa.git
                    "#
                )
                .unwrap()[0]
            ),
            Ok(CodeSource::Git {
                url: String::from("https://github.com/pisa-engine/pisa.git"),
                branch: String::from("dev")
            })
        );
        assert_eq!(
            CodeSource::parse(
                &YamlLoader::load_from_str(
                    r#"
                        type: 112
                    "#
                )
                .unwrap()[0]
            ),
            Err(Error::new("missing or corrupted source.type"))
        );
        assert_eq!(
            CodeSource::parse(
                &YamlLoader::load_from_str(
                    r#"
                        type: "foo"
                    "#
                )
                .unwrap()[0]
            ),
            Err(Error::new("unknown source type: foo"))
        );
    }

    #[test]
    fn test_parse_collection() {
        let config = Config {
            workdir: PathBuf::from("/work"),
            source: CodeSource::Git {
                url: String::from(""),
                branch: String::from(""),
            },
            suppressed: HashSet::new(),
            collections: Vec::new(),
        };
        let yaml = yaml_rust::YamlLoader::load_from_str(
            "
        name: wapo
        description: WashingtonPost.v2
        collection_dir: /path/to/wapo
        forward_index: fwd/wapo
        inverted_index: /absolute/path/to/inv/wapo",
        )
        .unwrap();
        let coll = config.parse_collection(&yaml[0]).unwrap();
        assert_eq!(coll.forward_index, PathBuf::from("/work/fwd/wapo"));
        assert_eq!(
            coll.inverted_index,
            PathBuf::from("/absolute/path/to/inv/wapo")
        );
    }
}
