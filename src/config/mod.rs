//! Experiment configuration, which is used throughout a run, and mostly
//! defined in an external YAML configuration file (with several exceptions).

extern crate boolinator;
extern crate downcast_rs;
extern crate glob;
extern crate yaml_rust;

use crate::command::ExtCommand;
use crate::error::Error;
use crate::executor::*;
use crate::run::Run;
use crate::source::*;
use crate::*;
use boolinator::Boolinator;
use downcast_rs::Downcast;
use failure::ResultExt;
use glob::glob;
use log::error;
use std::collections::{HashMap, HashSet};
use std::convert::{From, Into};
use std::fmt::Debug;
use std::fs::read_to_string;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::str::FromStr;
use yaml_rust::{Yaml, YamlLoader};

/// Mapping from collection name to a (reference counted pointer to) collection object.
pub type CollectionMap = HashMap<String, Rc<Collection>>;

/// Extension for `yaml_rust::Yaml` object providing useful methods.
pub trait YamlExt {
    /// Returns string slice to the value of a field if it is a string,
    /// or an error otherwise.
    fn require_string(&self, field: &str) -> Result<&str, Error>;
}
impl YamlExt for Yaml {
    fn require_string(&self, field: &str) -> Result<&str, Error> {
        self[field]
            .as_str()
            .ok_or_else(|| Error::from(format!("field {} missing or not string", field)))
    }
}

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
impl AsRef<str> for Encoding {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}
impl std::fmt::Display for Encoding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

/// Collection type defining parsing command.
///
/// Building an index is identical for any collection, but how the files
/// are accessed for parsing differs.
/// This trait is defined in order to enable enhancing this libary with
/// new collection types in the future.
pub trait CollectionType: Debug + Downcast + fmt::Display {
    /// Returns a command object: its execution will parse the collection
    /// and build a forward index.
    fn parse_command(
        &self,
        executor: &dyn PisaExecutor,
        collection: &Collection,
    ) -> Result<ExtCommand, Error>;
}
impl_downcast!(CollectionType);

impl CollectionType {
    /// Parses a string and returns a requested collection type object,
    /// or an error if the name is invalid.
    ///
    /// # Examples
    ///
    /// ```
    /// # extern crate stdbench;
    /// extern crate downcast_rs;
    /// # use stdbench::config::*;
    /// use downcast_rs::Downcast;
    /// assert!(CollectionType::from("wapo").is_ok());
    /// assert!(CollectionType::from("trecweb").is_ok());
    /// assert!(CollectionType::from("unknown").is_err());
    /// ```
    pub fn from<S>(name: S) -> Result<Box<dyn CollectionType>, Error>
    where
        S: AsRef<str>,
    {
        match name.as_ref() {
            "wapo" => Ok(WashingtonPostCollection::boxed()),
            "trecweb" => Ok(TrecWebCollection::boxed()),
            _ => Err(Error::from(format!(
                "Unknown collection type: {}",
                name.as_ref()
            ))),
        }
    }
}

fn resolve_files<P: AsRef<Path>>(path: P) -> Result<Vec<PathBuf>, Error> {
    let pattern = path.as_ref().to_str().unwrap();
    let files: Vec<_> = glob(pattern).unwrap().filter_map(Result::ok).collect();
    (!files.is_empty()).ok_or(format!(
        "could not resolve any files for pattern: {}",
        pattern
    ))?;
    Ok(files)
}

/// This is a collection such as Gov2.
#[derive(Debug)]
pub struct TrecWebCollection;
impl TrecWebCollection {
    /// Returns the object wrapped in `Box`.
    pub fn boxed() -> Box<Self> {
        Box::new(Self {})
    }
}
impl fmt::Display for TrecWebCollection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "trecweb")
    }
}
impl CollectionType for TrecWebCollection {
    fn parse_command(
        &self,
        executor: &dyn PisaExecutor,
        collection: &Collection,
    ) -> Result<ExtCommand, Error> {
        let input_files = resolve_files(collection.collection_dir.join("GX*/*.gz"))?;
        Ok(ExtCommand::new("zcat")
            .args(&input_files)
            .pipe_command(executor.command("parse_collection"))
            .args(&[
                "-o",
                collection.forward_index.to_str().unwrap(),
                "-f",
                "trecweb",
                "--stemmer",
                "porter2",
                "--content-parser",
                "html",
                "--batch-size",
                "1000",
            ]))
    }
}

/// WashingtonPost.v2 collection type: [](https://trec.nist.gov/data/wapost)
#[derive(Debug)]
pub struct WashingtonPostCollection;
impl WashingtonPostCollection {
    /// Returns the object wrapped in `Box`.
    pub fn boxed() -> Box<Self> {
        Box::new(Self {})
    }
}
impl fmt::Display for WashingtonPostCollection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "wapo")
    }
}
impl CollectionType for WashingtonPostCollection {
    fn parse_command(
        &self,
        executor: &dyn PisaExecutor,
        collection: &Collection,
    ) -> Result<ExtCommand, Error> {
        let input_files = resolve_files(collection.collection_dir.join("data/*.jl"))?;
        Ok(ExtCommand::new("cat")
            .args(&input_files)
            .pipe_command(executor.command("parse_collection"))
            .args(&[
                "-o",
                collection.forward_index.to_str().unwrap(),
                "-f",
                "wapo",
                "--stemmer",
                "porter2",
                "--content-parser",
                "html",
                "--batch-size",
                "1000",
            ]))
    }
}

/// Configuration of a tested collection.
#[derive(Debug)]
pub struct Collection {
    /// A collection name.
    pub name: String,
    /// The colleciton's type used when deciding on how to parse it.
    pub kind: Box<dyn CollectionType>,
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
impl PartialEq for Collection {
    fn eq(&self, other: &Self) -> bool {
        (
            self.kind.to_string(),
            &self.collection_dir,
            &self.forward_index,
            &self.inverted_index,
            &self.encodings,
        ) == (
            other.kind.to_string(),
            &other.collection_dir,
            &other.forward_index,
            &other.inverted_index,
            &other.encodings,
        )
    }
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
    /// # use stdbench::config::*;
    /// # use std::path::PathBuf;
    /// let yaml = yaml_rust::YamlLoader::load_from_str("
    /// name: wapo
    /// kind: wapo
    /// collection_dir: /path/to/wapo
    /// forward_index: fwd/wapo
    /// inverted_index: /absolute/path/to/inv/wapo
    /// encodings:
    ///   - block_simdbp").unwrap();
    /// let conf = config::Collection::from_yaml(&yaml[0]);
    /// assert_eq!(
    ///     conf,
    ///     Ok(Collection {
    ///         name: "wapo".to_string(),
    ///         kind: WashingtonPostCollection::boxed(),
    ///         collection_dir: PathBuf::from("/path/to/wapo"),
    ///         forward_index: PathBuf::from("fwd/wapo"),
    ///         inverted_index: PathBuf::from("/absolute/path/to/inv/wapo"),
    ///         encodings: vec![Encoding::from("block_simdbp")]
    ///     }
    /// ));
    /// ```
    pub fn from_yaml(yaml: &Yaml) -> Result<Self, Error> {
        let name = yaml.require_string("name")?;
        let kind = yaml.require_string("kind")?;
        let collection_dir = yaml.require_string("collection_dir")?;
        let fwd = yaml["forward_index"].as_str();
        let inv = yaml["inverted_index"].as_str();
        let encodings = Self::parse_encodings(&yaml["encodings"])
            .context(format!("failed to parse collection {}", name))?;
        Ok(Self {
            name: name.to_string(),
            kind: CollectionType::from(kind)?,
            collection_dir: PathBuf::from(collection_dir),
            forward_index: PathBuf::from(
                fwd.map_or_else(|| format!("fwd/{}", &name), String::from),
            ),
            inverted_index: PathBuf::from(
                inv.map_or_else(|| format!("inv/{}", &name), String::from),
            ),
            encodings,
        })
    }

    /// Returns a string representing forward index path.
    #[cfg_attr(tarpaulin, skip)] // Due to so many false positives
    pub fn fwd(&self) -> Result<&str, Error> {
        let fwd = self
            .forward_index
            .to_str()
            .ok_or("Failed to parse forward index path")?;
        Ok(fwd)
    }

    /// Returns a string representing inverted index path.
    #[cfg_attr(tarpaulin, skip)] // Due to so many false positives
    pub fn inv(&self) -> Result<&str, Error> {
        let inv = self
            .inverted_index
            .to_str()
            .ok_or("Failed to parse inverted index path")?;
        Ok(inv)
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
    pub source: Box<dyn PisaSource>,
    suppressed: HashSet<Stage>,
    /// Configuration of all collections to be tested.
    pub collections: Vec<Rc<Collection>>,
    /// Experimental runs
    pub runs: Vec<Run>,
}
impl Config {
    /// Constructs a new configuration with `workdir` and a source.
    /// It is recommended that `workdir` is an absolute path to avoid any misunderstandings.
    pub fn new<P>(workdir: P, source: Box<dyn PisaSource>) -> Self
    where
        P: AsRef<Path>,
    {
        Self {
            workdir: workdir.as_ref().to_path_buf(),
            source,
            suppressed: HashSet::new(),
            collections: Vec::new(),
            runs: Vec::new(),
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
    pub fn executor(&self) -> Result<Box<dyn PisaExecutor>, Error> {
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
                    let collections = conf.parse_collections(&yaml[0]["collections"])?;
                    conf.parse_runs(&yaml[0]["runs"], &collections)?;
                    Ok(conf)
                }
            },
            Err(_) => Err("could not parse YAML file".into()),
        }
    }

    fn parse_runs(&mut self, runs: &Yaml, collections: &CollectionMap) -> Result<(), Error> {
        match runs {
            Yaml::Array(runs) => {
                for run in runs {
                    self.runs.push(Run::parse(&run, collections)?);
                }
                Ok(())
            }

            _ => Ok(()),
        }
    }

    fn parse_collections(&mut self, collections: &Yaml) -> Result<CollectionMap, Error> {
        match collections {
            Yaml::Array(collections) => {
                let mut collection_map: CollectionMap = HashMap::new();
                for collection in collections {
                    match self.parse_collection(&collection) {
                        Ok(coll_config) => {
                            let name = coll_config.name.clone();
                            let collrc = Rc::new(coll_config);
                            self.collections.push(Rc::clone(&collrc));
                            collection_map.insert(name, collrc);
                        }
                        Err(err) => error!("Unable to parse collection config: {}", err),
                    }
                }
                if self.collections.is_empty() {
                    Err("no correct collection configurations found".into())
                } else {
                    Ok(collection_map)
                }
            }
            _ => Err("missing or corrupted collections config".into()),
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
mod tests;
