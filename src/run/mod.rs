//! All things related to experimental runs, including efficiency and precision runs.
extern crate yaml_rust;

use crate::{
    config::{Collection, CollectionMap, Encoding, YamlExt},
    error::Error,
    executor::PisaExecutor,
};
use std::{path::PathBuf, rc::Rc};
use yaml_rust::Yaml;

/// An experimental run.
#[derive(PartialEq, Debug)]
pub enum Run {
    /// Report selected precision metrics.
    Evaluate {
        /// Pointer to evalated collection
        collection: Rc<Collection>,
        /// Path to topics in TREC format
        topics: PathBuf,
        /// Path to a [TREC qrels
        /// file](https://www-nlpir.nist.gov/projects/trecvid/trecvid.tools/trec_eval_video/A.README)
        qrels: PathBuf,
    },
    /// Report query times
    Benchmark,
}
impl Run {
    fn parse_evaluate(yaml: &Yaml, collection: Rc<Collection>) -> Result<Self, Error> {
        let topics = yaml.require_string("topics")?;
        let qrels = yaml.require_string("qrels")?;
        Ok(Run::Evaluate {
            collection,
            topics: PathBuf::from(topics),
            qrels: PathBuf::from(qrels),
        })
    }

    /// Constructs from a YAML object, given a collection map.
    ///
    /// Fails if failed to parse, or when the referenced collection is missing form
    /// the mapping.
    ///
    /// # Example
    /// ```
    /// # extern crate yaml_rust;
    /// # extern crate stdbench;
    /// # use stdbench::run::Run;
    /// # use stdbench::config::{Collection, CollectionMap, Encoding};
    /// # use std::collections::HashMap;
    /// # use std::path::PathBuf;
    /// # use std::rc::Rc;
    /// let yaml = yaml_rust::YamlLoader::load_from_str("
    /// collection: wapo
    /// type: evaluate
    /// topics: /path/to/query/topics
    /// qrels: /path/to/query/relevance").unwrap();
    ///
    /// let mut collections: CollectionMap = HashMap::new();
    /// let run = Run::parse(&yaml[0], &collections);
    /// assert!(run.is_err());
    ///
    /// collections.insert(String::from("wapo"), Rc::new(Collection {
    ///     name: String::from("wapo"),
    ///     collection_dir: PathBuf::from("/coll/dir"),
    ///     forward_index: PathBuf::from("fwd"),
    ///     inverted_index: PathBuf::from("inv"),
    ///     encodings: vec![Encoding::from("block_simdbp")]
    /// }));
    /// let run = Run::parse(&yaml[0], &collections);
    /// match run {
    ///     Ok(Run::Evaluate { collection, .. }) => {
    ///         assert_eq!(collection.name, "wapo");
    ///     }
    ///     _ => panic!(),
    /// }
    /// ```
    pub fn parse(yaml: &Yaml, collections: &CollectionMap) -> Result<Self, Error> {
        let collection_name = yaml.require_string("collection")?;
        let collection = collections
            .get(collection_name)
            .ok_or_else(|| format!("collection {} not found in config", collection_name))?;
        let typ = yaml.require_string("type")?;
        match typ {
            "evaluate" => Self::parse_evaluate(yaml, Rc::clone(collection)),
            unknown => Err(Error::from(format!("unknown collection type: {}", unknown))),
        }
    }

    /// Returns the type of this run.
    ///
    /// # Example
    ///
    /// ```
    /// # extern crate yaml_rust;
    /// # extern crate stdbench;
    /// # use stdbench::run::Run;
    /// # use stdbench::config::{Collection, CollectionMap, Encoding};
    /// # use std::collections::HashMap;
    /// # use std::path::PathBuf;
    /// # use std::rc::Rc;
    /// let collection = Rc::new(Collection {
    ///     name: String::from("wapo"),
    ///     collection_dir: PathBuf::from("/coll/dir"),
    ///     forward_index: PathBuf::from("fwd"),
    ///     inverted_index: PathBuf::from("inv"),
    ///     encodings: vec![Encoding::from("block_simdbp")]
    /// });
    /// assert_eq!(
    ///     Run::Evaluate {
    ///         collection,
    ///         topics: PathBuf::new(),
    ///         qrels: PathBuf::new()
    ///     }.run_type(),
    ///     "evaluate"
    /// );
    /// assert_eq!(
    ///     Run::Benchmark.run_type(),
    ///     "benchmark"
    /// );
    /// ```
    pub fn run_type(&self) -> String {
        match self {
            Run::Evaluate { .. } => String::from("evaluate"),
            Run::Benchmark => String::from("benchmark"),
        }
    }
}

/// Runs query evaluation for on a given executor, for a given run.
///
/// Fails if the run is not of type `Evaluate`.
pub fn evaluate(executor: &PisaExecutor, run: &Run, encoding: &Encoding) -> Result<String, Error> {
    if let Run::Evaluate {
        collection, topics, ..
    } = run
    {
        executor.evaluate_queries(
            &collection.inverted_index,
            &collection.forward_index,
            encoding,
            format!("{}.title", topics.display()),
        )
    } else {
        Err(Error::from(format!(
            "Run of type {} cannot be evaluated",
            run.run_type()
        )))
    }
}

#[cfg(test)]
mod tests;
