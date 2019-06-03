extern crate tempdir;
extern crate yaml_rust;

use super::*;
use crate::run::{EvaluateData, RunData, TopicsFormat, TrecTopicField};
use crate::tests::*;
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
            &YamlLoader::load_from_str("- block_simdbp\n- complex: {}\n  object: x\n- block_qmx")
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
        kind: wapo
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
    assert_eq!(coll.fwd().unwrap(), "/work/fwd/wapo");
    assert_eq!(coll.inv().unwrap(), "/absolute/path/to/inv/wapo");
}

#[test]
fn test_parse_collection_missing_coll_dir() {
    let yaml = yaml_rust::YamlLoader::load_from_str(
        "
        name: wapo
        kind: wapo
        forward_index: fwd/wapo
        inverted_index: /absolute/path/to/inv/wapo",
    )
    .unwrap();
    assert_eq!(
        test_conf().parse_collection(&yaml[0]).err(),
        Some("field collection_dir missing or not string".into())
    );
}

#[test]
fn test_parse_collection_missing_encodings() {
    let yaml = yaml_rust::YamlLoader::load_from_str(
        "
        name: wapo
        kind: wapo
        collection_dir: dir
        forward_index: fwd/wapo
        inverted_index: /absolute/path/to/inv/wapo",
    )
    .unwrap();
    assert_eq!(
        test_conf().parse_collection(&yaml[0]).err(),
        Some("failed to parse collection wapo: missing or corrupted encoding list".into())
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
      kind: wapo
      collection_dir: /collections/wapo
      forward_index: fwd/wapo
      inverted_index: inv/wapo
      encodings:
        - block_simdbp
runs:
    - collection: wapo
      type: evaluate
      topics: /topics
      qrels: /qrels
      output: r1.out";
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
        conf.collections[0].as_ref(),
        &Collection {
            name: "wapo".to_string(),
            kind: WashingtonPostCollection::boxed(),
            collection_dir: PathBuf::from("/collections/wapo"),
            forward_index: PathBuf::from("/tmp/fwd/wapo"),
            inverted_index: PathBuf::from("/tmp/inv/wapo"),
            encodings: vec!["block_simdbp".parse().unwrap()]
        }
    );
    assert_eq!(conf.runs[0].collection.name, "wapo");
    match &conf.runs[0].data {
        RunData::Evaluate(EvaluateData {
            topics,
            topics_format,
            qrels,
            output_file,
        }) => {
            assert_eq!(topics, &PathBuf::from("/topics"));
            assert_eq!(qrels, &PathBuf::from("/qrels"));
            assert_eq!(topics_format, &TopicsFormat::Trec(TrecTopicField::Title));
            assert_eq!(output_file, &PathBuf::from("/tmp/r1.out"));
        }
        _ => panic!(),
    }
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

#[test]
fn test_yaml_ext() {
    let yaml = YamlLoader::load_from_str("name: wapo").unwrap();
    assert_eq!(yaml[0].require_string("name"), Ok("wapo"));
    assert!(yaml[0].require_string("unknown").is_err());
}

#[test]
fn test_parse_command_trecweb() -> Result<(), Error> {
    let tmp = TempDir::new("config")?;
    let setup = mock_set_up(&tmp);
    let cmd = TrecWebCollection::boxed()
        .parse_command(setup.executor.as_ref(), &setup.config.collections[1])?;
    assert_eq!(
        cmd.to_string(),
        format!(
            "zcat \
             {0}/gov2/GX000/00.gz {0}/gov2/GX000/01.gz \
             {0}/gov2/GX001/02.gz {0}/gov2/GX001/03.gz\
             \n    | {0}/parse_collection -o {}/gov2/fwd -f trecweb \
             --stemmer porter2 --content-parser html --batch-size 1000",
            tmp.path().display()
        )
    );
    Ok(())
}

#[test]
fn test_parse_command_warc() -> Result<(), Error> {
    let tmp = TempDir::new("config")?;
    let setup = mock_set_up(&tmp);
    let cmd = WarcCollection::boxed()
        .parse_command(setup.executor.as_ref(), &setup.config.collections[2])?;
    assert_eq!(
        cmd.to_string(),
        format!(
            "zcat \
             {0}/cw09b/en0000/00.warc.gz {0}/cw09b/en0000/01.warc.gz \
             {0}/cw09b/en0001/02.warc.gz {0}/cw09b/en0001/03.warc.gz\
             \n    | {0}/parse_collection -o {}/cw09b/fwd -f warc \
             --stemmer porter2 --content-parser html --batch-size 1000",
            tmp.path().display()
        )
    );
    Ok(())
}

#[test]
fn test_colection_type_from_str() {
    assert!(CollectionType::from("wapo")
        .unwrap()
        .downcast_ref::<WashingtonPostCollection>()
        .is_some(),);
    assert!(CollectionType::from("trecweb")
        .unwrap()
        .downcast_ref::<TrecWebCollection>()
        .is_some(),);
    assert!(CollectionType::from("unknown").is_err());
}

#[test]
fn test_colection_type_to_str() {
    assert_eq!(WashingtonPostCollection::boxed().to_string(), "wapo");
    assert_eq!(TrecWebCollection::boxed().to_string(), "trecweb");
}
