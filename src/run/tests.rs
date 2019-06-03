extern crate tempdir;
extern crate yaml_rust;

use super::{evaluate, Run, RunData, TopicsFormat, TrecTopicField};
use crate::config::{Collection, CollectionMap, Encoding, WashingtonPostCollection};
use crate::error::Error;
use crate::tests::{mock_set_up, MockSetup};
use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;
use tempdir::TempDir;
use yaml_rust::YamlLoader;

#[test]
#[cfg_attr(target_family, unix)]
fn test_evaluate() {
    let tmp = TempDir::new("build").unwrap();
    let MockSetup {
        config,
        executor,
        programs,
        outputs,
        term_count: _,
    } = mock_set_up(&tmp);
    evaluate(executor.as_ref(), config.runs.first().unwrap()).unwrap();
    assert_eq!(
        std::fs::read_to_string(outputs.get("evaluate_queries").unwrap()).unwrap(),
        format!(
            "{0} -t block_simdbp -i {2}.block_simdbp -w {2}.wand -a wand \
             -q topics.title --terms {1}.termmap --documents {1}.docmap \
             --stemmer porter2 -k 1000",
            programs.get("evaluate_queries").unwrap().display(),
            tmp.path().join("fwd").display(),
            tmp.path().join("inv").display(),
        )
    );
}

#[test]
#[cfg_attr(target_family, unix)]
fn test_evaluate_wrong_type() {
    let tmp = TempDir::new("build").unwrap();
    let MockSetup {
        config,
        executor,
        programs: _,
        outputs: _,
        term_count: _,
    } = mock_set_up(&tmp);
    assert!(evaluate(
        executor.as_ref(),
        &Run {
            collection: Rc::clone(&config.collections[0]),
            data: RunData::Benchmark
        },
    )
    .is_err());
}

#[test]
fn test_unknown_run_type() {
    let yaml = YamlLoader::load_from_str("collection: wapo\ntype: unknown").unwrap();
    let mut collections: CollectionMap = HashMap::new();
    collections.insert(
        String::from("wapo"),
        Rc::new(Collection {
            name: "wapo".to_string(),
            kind: WashingtonPostCollection::boxed(),
            collection_dir: PathBuf::from("/coll/dir"),
            forward_index: PathBuf::from("fwd"),
            inverted_index: PathBuf::from("inv"),
            encodings: vec![Encoding::from("block_simdbp")],
        }),
    );
    assert_eq!(
        Run::parse(&yaml[0], &collections, PathBuf::from("workdir")).err(),
        Some(Error::from("unknown run type: unknown"))
    );
}

#[test]
fn test_parse_topics_format() -> Result<(), Error> {
    let yaml = YamlLoader::load_from_str("topics: /topics").unwrap();
    assert_eq!(Run::parse_topics_format(&yaml[0])?, None);

    let yaml = YamlLoader::load_from_str("topics_format: simple").unwrap();
    assert_eq!(
        Run::parse_topics_format(&yaml[0])?,
        Some(TopicsFormat::Simple)
    );

    let yaml = YamlLoader::load_from_str("topics_format: trec").unwrap();
    assert_eq!(
        Run::parse_topics_format(&yaml[0]).err(),
        Some(Error::from("field trec_topic_field missing or not string"))
    );

    let yaml = YamlLoader::load_from_str(
        "topics_format: trec
trec_topic_field: xxx",
    )
    .unwrap();
    assert!(Run::parse_topics_format(&yaml[0]).is_err());

    let yaml = YamlLoader::load_from_str(
        "topics_format: trec
trec_topic_field: title",
    )
    .unwrap();
    assert_eq!(
        Run::parse_topics_format(&yaml[0])?,
        Some(TopicsFormat::Trec(TrecTopicField::Title))
    );

    let yaml = YamlLoader::load_from_str(
        "topics_format: trec
trec_topic_field: desc",
    )
    .unwrap();
    assert_eq!(
        Run::parse_topics_format(&yaml[0])?,
        Some(TopicsFormat::Trec(TrecTopicField::Description))
    );

    let yaml = YamlLoader::load_from_str(
        "topics_format: trec
trec_topic_field: narr",
    )
    .unwrap();
    assert_eq!(
        Run::parse_topics_format(&yaml[0])?,
        Some(TopicsFormat::Trec(TrecTopicField::Narrative))
    );
    Ok(())
}