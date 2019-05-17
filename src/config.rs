//! Experiment configuration, which is used throughout a run, and mostly
//! defined in an external YAML configuration file (with several exceptions).

extern crate yaml_rust;

use super::error::Error;
use super::executor::*;
use super::source::*;
use super::*;
use failure::ResultExt;
use log::error;
use std::collections::HashSet;
use std::convert::From;
use std::fs::read_to_string;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use yaml_rust::{Yaml, YamlLoader};

/// Index encoding type.
///
/// Intentionally implemented as a string to keep up with any PISA changes.
/// Assuming that any PISA tool using this will report an invalid encoding for us.
#[derive(Debug, PartialEq, Clone)]
pub struct Encoding(String);
impl FromStr for Encoding {
    type Err = Error;
    fn from_str(name: &str) -> Result<Self, Error> {
        Ok(Self(name.to_string()))
    }
}
impl From<&str> for Encoding {
    fn from(name: &str) -> Self {
        Self(name.to_string())
    }
}

/// Configuration of a tested collection.
#[derive(Debug, PartialEq)]
pub struct Collection {
    /// A collection name used also as a type when deciding on how to parse it.
    pub name: String,
    /// The root directory of the collection. Depending on a type, it could
    /// contain one or many files or directories. Must use `name` to determine
    /// how to find relevant data.
    pub collection_dir: PathBuf,
    /// The basename of the forward index.
    pub forward_index: PathBuf,
    /// The basename of the inverted index.
    pub inverted_index: PathBuf,
    /// The list of index encoding techniques to be tested.
    /// The compression step will be therefore run `encodings.len()` times,
    /// one for each technique.
    pub encodings: Vec<Encoding>,
}
impl Collection {
    /// Constructs a collection config from a YAML object.
    ///
    /// # Example
    ///
    /// ```
    /// extern crate yaml_rust;
    /// # extern crate stdbench;
    /// # use stdbench::config;
    /// # use stdbench::config::{Collection, Encoding};
    /// # use std::path::PathBuf;
    /// //# include!("src/doctest_helper.rs");
    /// let yaml = yaml_rust::YamlLoader::load_from_str("
    /// name: wapo
    /// collection_dir: /path/to/wapo
    /// forward_index: fwd/wapo
    /// inverted_index: /absolute/path/to/inv/wapo
    /// encodings:
    ///   - block_simdbp").unwrap();
    /// let conf = config::Collection::from_yaml(&yaml[0]);
    /// assert_eq!(
    ///     conf,
    ///     Ok(Collection {
    ///         name: String::from("wapo"),
    ///         collection_dir: PathBuf::from("/path/to/wapo"),
    ///         forward_index: PathBuf::from("fwd/wapo"),
    ///         inverted_index: PathBuf::from("/absolute/path/to/inv/wapo"),
    ///         encodings: vec![Encoding::from("block_simdbp")]
    ///     }
    /// ));
    /// ```
    pub fn from_yaml(yaml: &Yaml) -> Result<Self, Error> {
        match (
            yaml["name"].as_str(),
            yaml["collection_dir"].as_str(),
            yaml["forward_index"].as_str(),
            yaml["inverted_index"].as_str(),
            &yaml["encodings"],
        ) {
            (None, _, _, _, _) => Err("undefined name".into()),
            (_, None, _, _, _) => Err("undefined collection_dir".into()),
            (Some(name), Some(collection_dir), fwd, inv, encodings) => {
                let encodings = Self::parse_encodings(&encodings)
                    .context(format!("failed to parse collection {}", name))?;
                Ok(Self {
                    name: name.to_string(),
                    collection_dir: PathBuf::from(collection_dir),
                    forward_index: PathBuf::from(fwd.unwrap_or("fwd/wapo")),
                    inverted_index: PathBuf::from(inv.unwrap_or("inv/wapo")),
                    encodings,
                })
            }
        }
    }

    fn parse_encodings(yaml: &Yaml) -> Result<Vec<Encoding>, Error> {
        let encodings = yaml.as_vec().ok_or("missing or corrupted encoding list")?;
        let encodings: Vec<Encoding> = encodings
            .iter()
            .filter_map(|enc| {
                if let Some(enc) = enc.as_str() {
                    Some(Encoding::from(enc))
                } else {
                    error!("could not parse encoding: {:?}", enc);
                    None
                }
            })
            .collect();
        if encodings.is_empty() {
            Err("no valid encoding entries".into())
        } else {
            Ok(encodings)
        }
    }
}

/// Stores a full config for benchmark run.
#[derive(Debug)]
pub struct Config {
    /// This is the default directory of the experiment. Any paths that are not
    /// absolute will be rooted at this directory.
    pub workdir: PathBuf,
    /// Configuration of where the tools come from.
    pub source: Box<PisaSource>,
    suppressed: HashSet<Stage>,
    /// Configuration of all collections to be tested.
    pub collections: Vec<Collection>,
}
impl Config {
    /// Constructs a new configuration with `workdir` and a source.
    /// It is recommended that `workdir` is an absolute path to avoid any misunderstandings.
    pub fn new<P>(workdir: P, source: Box<PisaSource>) -> Self
    where
        P: AsRef<Path>,
    {
        Self {
            workdir: workdir.as_ref().to_path_buf(),
            source,
            suppressed: HashSet::new(),
            collections: Vec::new(),
        }
    }

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
    /// // Declare `config` as `mut` and execute the following line to skip
    /// // the compilation stage:
    /// // config.suppress_stage(Stage::Compile);
    /// let executor = config.executor();
    /// ```
    pub fn executor(&self) -> Result<Box<PisaExecutor>, super::error::Error> {
        self.source.executor(&self)
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
    pub fn from_file<P>(file: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let content = read_to_string(&file).context("Failed to read config file")?;
        match YamlLoader::load_from_str(&content) {
            Ok(yaml) => match (yaml[0]["workdir"].as_str(), &yaml[0]["source"]) {
                (None, _) => Err("missing or corrupted workdir".into()),
                (Some(workdir), source) => {
                    let source = PisaSource::parse(source)?;
                    let mut conf = Self::new(PathBuf::from(workdir), source);
                    match &yaml[0]["collections"] {
                        Yaml::Array(collections) => {
                            for collection in collections {
                                match conf.parse_collection(&collection) {
                                    Ok(coll_config) => {
                                        conf.collections.push(coll_config);
                                    }
                                    Err(err) => {
                                        error!("Unable to parse collection config: {}", err)
                                    }
                                }
                            }
                            if conf.collections.is_empty() {
                                Err("no correct collection configurations found".into())
                            } else {
                                Ok(conf)
                            }
                        }
                        _ => Err("missing or corrupted collections config".into()),
                    }
                }
            },
            Err(_) => Err("could not parse YAML file".into()),
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

    fn parse_collection(&self, yaml: &Yaml) -> Result<Collection, Error> {
        let mut collconf = Collection::from_yaml(yaml)?;
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
    extern crate tempdir;
    extern crate yaml_rust;

    use super::*;
    use tempdir::TempDir;
    use yaml_rust::YamlLoader;

    fn test_conf() -> Config {
        Config::new(PathBuf::from("/work"), Box::new(GitSource::new("", "")))
    }

    #[test]
    fn test_suppress() {
        let mut conf = test_conf();
        conf.suppress_stage(Stage::BuildIndex);
        assert!(conf.is_suppressed(Stage::BuildIndex));
    }

    #[test]
    fn test_parse_encodings() {
        assert_eq!(
            Collection::parse_encodings(&YamlLoader::load_from_str("- block_simdbp").unwrap()[0]),
            Ok(vec![Encoding::from("block_simdbp")])
        );
        assert_eq!(
            Collection::parse_encodings(
                &YamlLoader::load_from_str(
                    "- block_simdbp\n- complex: {}\n  object: x\n- block_qmx"
                )
                .unwrap()[0]
            ),
            Ok(vec![
                Encoding::from("block_simdbp"),
                Encoding::from("block_qmx")
            ])
        );
        assert_eq!(
            Collection::parse_encodings(&YamlLoader::load_from_str("some string").unwrap()[0]),
            Err(Error::from("missing or corrupted encoding list"))
        );
        assert_eq!(
            Collection::parse_encodings(&YamlLoader::load_from_str("- complex: x").unwrap()[0]),
            Err(Error::from("no valid encoding entries"))
        );
    }

    #[test]
    fn test_parse_collection() {
        let yaml = yaml_rust::YamlLoader::load_from_str(
            "
        name: wapo
        collection_dir: /path/to/wapo
        forward_index: fwd/wapo
        inverted_index: /absolute/path/to/inv/wapo
        encodings:
          - block_simdbp",
        )
        .unwrap();
        let coll = test_conf().parse_collection(&yaml[0]).unwrap();
        assert_eq!(coll.forward_index, PathBuf::from("/work/fwd/wapo"));
        assert_eq!(
            coll.inverted_index,
            PathBuf::from("/absolute/path/to/inv/wapo")
        );
    }

    #[test]
    fn test_parse_collection_missing_coll_dir() {
        let yaml = yaml_rust::YamlLoader::load_from_str(
            "
        name: wapo
        forward_index: fwd/wapo
        inverted_index: /absolute/path/to/inv/wapo",
        )
        .unwrap();
        assert_eq!(
            test_conf().parse_collection(&yaml[0]),
            Err("undefined collection_dir".into())
        );
    }

    #[test]
    fn test_parse_collection_missing_encodings() {
        let yaml = yaml_rust::YamlLoader::load_from_str(
            "
        name: wapo
        collection_dir: dir
        forward_index: fwd/wapo
        inverted_index: /absolute/path/to/inv/wapo",
        )
        .unwrap();
        assert_eq!(
            test_conf().parse_collection(&yaml[0]),
            Err("failed to parse collection wapo: missing or corrupted encoding list".into())
        );
    }

    #[test]
    fn test_config_from_file() -> std::io::Result<()> {
        let tmp = TempDir::new("tmp")?;
        let config_file = tmp.path().join("conf.yml");
        let yml = "
workdir: /tmp
source:
    type: git
    branch: dev
    url: https://github.com/pisa-engine/pisa.git
collections:
    - name: wapo
      collection_dir: /collections/wapo
      forward_index: fwd/wapo
      inverted_index: inv/wapo
      encodings:
        - block_simdbp";
        std::fs::write(&config_file, yml)?;
        let conf = Config::from_file(config_file).unwrap();
        assert_eq!(conf.workdir, PathBuf::from("/tmp"));
        assert_eq!(
            format!("{:?}", conf.source),
            format!(
                "{:?}",
                GitSource::new("https://github.com/pisa-engine/pisa.git", "dev")
            )
        );
        assert_eq!(
            conf.collections[0],
            Collection {
                name: "wapo".to_string(),
                collection_dir: PathBuf::from("/collections/wapo"),
                forward_index: PathBuf::from("/tmp/fwd/wapo"),
                inverted_index: PathBuf::from("/tmp/inv/wapo"),
                encodings: vec!["block_simdbp".parse().unwrap()]
            }
        );
        Ok(())
    }

    #[test]
    fn test_config_from_file_missing_collections() -> std::io::Result<()> {
        let tmp = TempDir::new("tmp")?;
        let config_file = tmp.path().join("conf.yml");
        let yml = "
workdir: /tmp
source:
    type: git
    branch: dev
    url: https://github.com/pisa-engine/pisa.git";
        std::fs::write(&config_file, yml)?;
        let conf = Config::from_file(config_file).err().unwrap();
        assert_eq!(conf.to_string(), "missing or corrupted collections config");
        Ok(())
    }

    #[test]
    fn test_config_from_file_corrupted_collection() -> std::io::Result<()> {
        let tmp = TempDir::new("tmp")?;
        let config_file = tmp.path().join("conf.yml");
        let yml = "
workdir: /tmp
source:
    type: git
    branch: dev
    url: https://github.com/pisa-engine/pisa.git
collections:
    - forward_index: fwd/wapo
      inverted_index: inv/wapo";
        std::fs::write(&config_file, yml)?;
        let conf = Config::from_file(config_file).err().unwrap();
        assert_eq!(
            conf.to_string(),
            "no correct collection configurations found"
        );
        Ok(())
    }

    #[test]
    fn test_config_from_file_yaml_error() -> std::io::Result<()> {
        let tmp = TempDir::new("tmp")?;
        let config_file = tmp.path().join("conf.yml");
        let yml = "*%%#";
        std::fs::write(&config_file, yml)?;
        let conf = Config::from_file(config_file).err().unwrap();
        assert_eq!(conf.to_string(), "could not parse YAML file");
        Ok(())
    }
}
