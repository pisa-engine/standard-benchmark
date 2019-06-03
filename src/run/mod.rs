//! All things related to experimental runs, including efficiency and precision runs.
extern crate failure;
extern crate strum;
extern crate strum_macros;
extern crate tempdir;
extern crate yaml_rust;

use crate::{
    command::ExtCommand,
    config::{Collection, CollectionMap, YamlExt},
    error::Error,
    executor::PisaExecutor,
};
use failure::ResultExt;
use std::{
    fs,
    path::{Path, PathBuf},
    rc::Rc,
};
use strum_macros::{Display, EnumIter, EnumString};
use tempdir::TempDir;
use yaml_rust::Yaml;
use RunData::{Benchmark, Evaluate};

/// Represents one of the three available fields in a TREC topic file
/// to be used as an input for query processing.
#[derive(Debug, Clone, PartialEq, EnumString, Display, EnumIter)]
pub enum TrecTopicField {
    /// Short query from `<title>`
    #[strum(serialize = "title")]
    Title,
    /// Mid-length query from `<desc>`
    #[strum(serialize = "desc")]
    Description,
    /// Long query from `<narr>`
    #[strum(serialize = "narr")]
    Narrative,
}

/// Format in which query topics are provided.
#[derive(Debug, Clone, PartialEq)]
pub enum TopicsFormat {
    /// Each query is in format: `qid:query terms`; one query per line.
    Simple,
    /// TREC format; example: [](https://trec.nist.gov/data/terabyte/04/04topics.701-750.txt)
    Trec(TrecTopicField),
}

/// Data for evaluation run.
#[derive(Debug, Clone)]
pub struct EvaluateData {
    /// Path to topics in TREC format
    pub topics: PathBuf,
    /// Format of the file with topics (queries)
    pub topics_format: TopicsFormat,
    /// Path to a [TREC qrels
    /// file](https://www-nlpir.nist.gov/projects/trecvid/trecvid.tools/trec_eval_video/A.README)
    pub qrels: PathBuf,
    /// Where the output of `trec_eval` will be written.
    pub output_file: PathBuf,
}

/// An experimental run
#[derive(Debug, Clone)]
pub struct Run {
    /// Pointer to evalated collection
    pub collection: Rc<Collection>,
    /// Data specific to type of run
    pub data: RunData,
}

/// An experimental run.
#[derive(Debug, Clone)]
pub enum RunData {
    /// Report selected precision metrics.
    Evaluate(EvaluateData),
    /// Report query times
    Benchmark,
}
impl RunData {
    /// Cast to `EvaluateData` if run is `Evaluate`, or return `None`.
    pub fn as_evaluate(&self) -> Option<&EvaluateData> {
        match self {
            Evaluate(eval_data) => Some(eval_data),
            _ => None,
        }
    }
}
impl Run {
    fn parse_topics_format(yaml: &Yaml) -> Result<Option<TopicsFormat>, Error> {
        let topics_format = &yaml["topics_format"];
        if let Yaml::BadValue = topics_format {
            Ok(None)
        } else if let Yaml::String(topics_format) = topics_format {
            match topics_format.as_ref() {
                "simple" => Ok(Some(TopicsFormat::Simple)),
                "trec" => {
                    let field = yaml.require_string("trec_topic_field")?;
                    Ok(Some(TopicsFormat::Trec(
                        field
                            .parse::<TrecTopicField>()
                            .context("failed to parse trec topic field")?,
                    )))
                }
                invalid => Err(Error::from(format!("invalid topics format: {}", invalid))),
            }
        } else {
            Err(Error::from("topics_format is not a string value"))
        }
    }

    fn parse_evaluate<P>(yaml: &Yaml, collection: Rc<Collection>, workdir: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let topics = yaml.require_string("topics")?;
        let qrels = yaml.require_string("qrels")?;
        let output_file = yaml.require_string("output")?;
        Ok(Self {
            collection,
            data: Evaluate(EvaluateData {
                topics: PathBuf::from(topics),
                topics_format: Self::parse_topics_format(yaml)?
                    .unwrap_or(TopicsFormat::Trec(TrecTopicField::Title)),
                qrels: PathBuf::from(qrels),
                output_file: match PathBuf::from(output_file) {
                    ref abs if abs.is_absolute() => abs.clone(),
                    ref rel => workdir.as_ref().join(rel),
                },
            }),
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
    /// # use stdbench::config::*;
    /// # use std::collections::HashMap;
    /// # use std::path::PathBuf;
    /// # use std::rc::Rc;
    /// let yaml = yaml_rust::YamlLoader::load_from_str("
    /// collection: wapo
    /// type: evaluate
    /// topics: /path/to/query/topics
    /// qrels: /path/to/query/relevance
    /// output: /output").unwrap();
    ///
    /// let mut collections: CollectionMap = HashMap::new();
    /// let run = Run::parse(&yaml[0], &collections, PathBuf::from("work"));
    /// assert!(run.is_err());
    ///
    /// collections.insert(String::from("wapo"), Rc::new(Collection {
    ///     name: "wapo".to_string(),
    ///     kind: WashingtonPostCollection::boxed(),
    ///     collection_dir: PathBuf::from("/coll/dir"),
    ///     forward_index: PathBuf::from("fwd"),
    ///     inverted_index: PathBuf::from("inv"),
    ///     encodings: vec![Encoding::from("block_simdbp")]
    /// }));
    /// let run = Run::parse(&yaml[0], &collections, PathBuf::from("work")).unwrap();
    /// assert_eq!(run.collection.name, "wapo");
    /// ```
    pub fn parse<P>(yaml: &Yaml, collections: &CollectionMap, workdir: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let collection_name = yaml.require_string("collection")?;
        let collection = collections
            .get(collection_name)
            .ok_or_else(|| format!("collection {} not found in config", collection_name))?;
        let typ = yaml.require_string("type")?;
        match typ {
            "evaluate" => Self::parse_evaluate(yaml, Rc::clone(collection), workdir),
            unknown => Err(Error::from(format!("unknown run type: {}", unknown))),
        }
    }

    /// Returns the type of this run.
    ///
    /// # Example
    ///
    /// ```
    /// # extern crate yaml_rust;
    /// # extern crate stdbench;
    /// # use stdbench::run::*;
    /// # use stdbench::config::*;
    /// # use std::collections::HashMap;
    /// # use std::path::PathBuf;
    /// # use std::rc::Rc;
    /// let collection = Rc::new(Collection {
    ///     name: "wapo".to_string(),
    ///     kind: WashingtonPostCollection::boxed(),
    ///     collection_dir: PathBuf::from("/coll/dir"),
    ///     forward_index: PathBuf::from("fwd"),
    ///     inverted_index: PathBuf::from("inv"),
    ///     encodings: vec![Encoding::from("block_simdbp")]
    /// });
    /// assert_eq!(
    ///     Run {
    ///         collection: Rc::clone(&collection),
    ///         data: RunData::Evaluate(EvaluateData {
    ///             topics: PathBuf::new(),
    ///             topics_format: TopicsFormat::Simple,
    ///             qrels: PathBuf::new(),
    ///             output_file: PathBuf::from("output")
    ///         })
    ///     }.run_type(),
    ///     "evaluate"
    /// );
    /// assert_eq!(
    ///     Run {
    ///         collection,
    ///         data: RunData::Benchmark
    ///     }.run_type(),
    ///     "benchmark"
    /// );
    /// ```
    pub fn run_type(&self) -> String {
        match &self.data {
            Evaluate(_) => String::from("evaluate"),
            Benchmark => String::from("benchmark"),
        }
    }
}

/// Runs query evaluation for on a given executor, for a given run.
///
/// Fails if the run is not of type `Evaluate`.
pub fn evaluate(executor: &dyn PisaExecutor, run: &Run) -> Result<String, Error> {
    if let Evaluate(data) = &run.data {
        let queries = if let TopicsFormat::Trec(field) = &data.topics_format {
            executor.extract_topics(&data.topics, &data.topics)?;
            format!("{}.{}", &data.topics.display(), field)
        } else {
            data.topics.to_str().unwrap().to_string()
        };
        executor.evaluate_queries(&run.collection, &data, &queries)
    } else {
        Err(Error::from(format!(
            "Run of type {} cannot be evaluated",
            run.run_type()
        )))
    }
}

/// Process a run (e.g., single precision evaluation or benchmark).
pub fn process_run(executor: &dyn PisaExecutor, run: &Run) -> Result<(), Error> {
    match &run.data {
        Evaluate(eval) => {
            let output = evaluate(executor, &run)?;
            let tmp = TempDir::new("evaluate_queries").expect("Failed to create temp directory");
            let results_path = tmp.path().join("results.trec");
            std::fs::write(&results_path, &output)?;
            let output = ExtCommand::new("trec_eval")
                .arg("-q")
                .arg("-a")
                .arg(eval.qrels.to_str().unwrap())
                .arg(results_path.to_str().unwrap())
                .output()?;
            let eval_result =
                String::from_utf8(output.stdout).context("unable to parse result of trec_eval")?;
            //fs::write();
            Ok(())
        }
        Benchmark => {
            unimplemented!("Benchmark runs are currently unimplemented");
        }
    }
}

#[cfg(test)]
mod tests;
