extern crate yaml_rust;

use super::executor::*;
use super::source::*;
use super::*;
use log::error;
use std::collections::HashSet;
use std::convert::From;
use std::fs::read_to_string;
use std::path::{Path, PathBuf};
use yaml_rust::{Yaml, YamlLoader};

#[derive(Debug, PartialEq)]
pub struct Encoding(String);
impl Encoding {
    pub fn new(enc: &str) -> Encoding {
        Encoding(String::from(enc))
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
    pub encodings: Vec<Encoding>,
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
    /// inverted_index: /absolute/path/to/inv/wapo
    /// encodings:
    ///   - block_simdbp").unwrap();
    /// let conf = CollectionConfig::from_yaml(&yaml[0]);
    /// assert_eq!(
    ///     conf,
    ///     Ok(CollectionConfig {
    ///         name: String::from("wapo"),
    ///         description: Some(String::from("WashingtonPost.v2")),
    ///         collection_dir: PathBuf::from("/path/to/wapo"),
    ///         forward_index: PathBuf::from("fwd/wapo"),
    ///         inverted_index: PathBuf::from("/absolute/path/to/inv/wapo"),
    ///         encodings: vec![Encoding::new("block_simdbp")]
    ///     }
    /// ));
    /// ```
    pub fn from_yaml(yaml: &Yaml) -> Result<CollectionConfig, Error> {
        match (
            yaml["name"].as_str(),
            yaml["collection_dir"].as_str(),
            yaml["forward_index"].as_str(),
            yaml["inverted_index"].as_str(),
            &yaml["encodings"],
        ) {
            (None, _, _, _, _) => fail!("undefined name"),
            (_, None, _, _, _) => fail!("undefined collection_dir"),
            (Some(name), Some(collection_dir), fwd, inv, encodings) => {
                let encodings = Self::parse_encodings(&encodings).map_err(Error::prepend(
                    &format!("failed to parse collection {}", name),
                ))?;
                Ok(CollectionConfig {
                    name: name.to_string(),
                    description: yaml["description"].as_str().map(String::from),
                    collection_dir: PathBuf::from(collection_dir),
                    forward_index: PathBuf::from(fwd.unwrap_or("fwd/wapo")),
                    inverted_index: PathBuf::from(inv.unwrap_or("inv/wapo")),
                    encodings,
                })
            }
        }
    }

    fn parse_encodings(yaml: &Yaml) -> Result<Vec<Encoding>, Error> {
        let encodings = yaml
            .as_vec()
            .ok_or_else(|| Error::new("missing or corrupted encoding list"))?;
        let encodings: Vec<Encoding> = encodings
            .into_iter()
            .filter_map(|enc| match enc.as_str() {
                Some(enc) => Some(Encoding::new(enc)),
                None => {
                    error!("could not parse encoding: {:?}", enc);
                    None
                }
            })
            .collect();
        if encodings.is_empty() {
            fail!("no valid encoding entries")
        } else {
            Ok(encodings)
        }
    }
}

/// Stores a full config for benchmark run.
#[derive(Debug)]
pub struct Config {
    pub workdir: PathBuf,
    pub source: Box<PisaSource>,
    suppressed: HashSet<Stage>,
    pub collections: Vec<CollectionConfig>,
}
impl Config {
    pub fn new<P>(workdir: P, source: Box<PisaSource>) -> Config
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
    pub fn executor(&self) -> Result<Box<PisaExecutor>, Error> {
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
    pub fn from_file<P>(file: P) -> Result<Config, Error>
    where
        P: AsRef<Path>,
    {
        let content = read_to_string(&file)?;
        match YamlLoader::load_from_str(&content) {
            Ok(yaml) => match (yaml[0]["workdir"].as_str(), &yaml[0]["source"]) {
                (None, _) => fail!("missing or corrupted workdir"),
                (Some(workdir), source) => {
                    let source = PisaSource::parse(source)?;
                    let mut conf = Config::new(PathBuf::from(workdir), source);
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
                                fail!("no correct collection configurations found")
                            } else {
                                Ok(conf)
                            }
                        }
                        _ => fail!("missing or corrupted collections config"),
                    }
                }
            },
            Err(_) => fail!("could not parse YAML file"),
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
            CollectionConfig::parse_encodings(
                &YamlLoader::load_from_str("- block_simdbp").unwrap()[0]
            ),
            Ok(vec![Encoding::new("block_simdbp")])
        );
        assert_eq!(
            CollectionConfig::parse_encodings(
                &YamlLoader::load_from_str(
                    "- block_simdbp\n- complex: y\n  object: x\n- block_qmx"
                )
                .unwrap()[0]
            ),
            Ok(vec![
                Encoding::new("block_simdbp"),
                Encoding::new("block_qmx")
            ])
        );
        assert_eq!(
            CollectionConfig::parse_encodings(
                &YamlLoader::load_from_str("some string").unwrap()[0]
            ),
            Err(Error::new("missing or corrupted encoding list"))
        );
        assert_eq!(
            CollectionConfig::parse_encodings(
                &YamlLoader::load_from_str("- complex: x").unwrap()[0]
            ),
            Err(Error::new("no valid encoding entries"))
        );
    }

    #[test]
    fn test_parse_collection() {
        let yaml = yaml_rust::YamlLoader::load_from_str(
            "
        name: wapo
        description: WashingtonPost.v2
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
        description: WashingtonPost.v2
        forward_index: fwd/wapo
        inverted_index: /absolute/path/to/inv/wapo",
        )
        .unwrap();
        assert_eq!(
            test_conf().parse_collection(&yaml[0]),
            fail!("undefined collection_dir")
        );
    }

    #[test]
    fn test_parse_collection_missing_encodings() {
        let yaml = yaml_rust::YamlLoader::load_from_str(
            "
        name: wapo
        description: WashingtonPost.v2
        collection_dir: dir
        forward_index: fwd/wapo
        inverted_index: /absolute/path/to/inv/wapo",
        )
        .unwrap();
        assert_eq!(
            test_conf().parse_collection(&yaml[0]),
            fail!("failed to parse collection wapo: missing or corrupted encoding list")
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
      description: WashingtonPost.v2
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
            CollectionConfig {
                name: "wapo".to_string(),
                description: Some(String::from("WashingtonPost.v2")),
                collection_dir: PathBuf::from("/collections/wapo"),
                forward_index: PathBuf::from("/tmp/fwd/wapo"),
                inverted_index: PathBuf::from("/tmp/inv/wapo"),
                encodings: vec![Encoding::new("block_simdbp")]
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
        let conf = Config::from_file(config_file).err();
        assert_eq!(
            conf,
            Some(Error::new("missing or corrupted collections config"))
        );
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
    - description: WashingtonPost.v2
      forward_index: fwd/wapo
      inverted_index: inv/wapo";
        std::fs::write(&config_file, yml)?;
        let conf = Config::from_file(config_file).err();
        assert_eq!(
            conf,
            Some(Error::new("no correct collection configurations found"))
        );
        Ok(())
    }

    #[test]
    fn test_config_from_file_yaml_error() -> std::io::Result<()> {
        let tmp = TempDir::new("tmp")?;
        let config_file = tmp.path().join("conf.yml");
        let yml = "*%%#";
        std::fs::write(&config_file, yml)?;
        let conf = Config::from_file(config_file).err();
        assert_eq!(conf, Some(Error::new("could not parse YAML file")));
        Ok(())
    }
}
