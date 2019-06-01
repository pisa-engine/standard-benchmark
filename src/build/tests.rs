extern crate tempdir;
extern crate yaml_rust;

use super::*;
use crate::tests::{mock_set_up, MockSetup};
use std::fs::{create_dir, File};
use std::path::PathBuf;
use tempdir::TempDir;

#[test]
#[cfg_attr(target_family, unix)]
fn test_term_count() {
    {
        let tmp = TempDir::new("build").unwrap();
        let setup = mock_set_up(&tmp);
        assert_eq!(term_count(&setup.config.collections[0]), Ok(3));
    }
    {
        let tmp = TempDir::new("build").unwrap();
        let setup = mock_set_up(&tmp);
        std::fs::remove_file(tmp.path().join("fwd.terms")).unwrap();
        assert_eq!(
            term_count(&setup.config.collections[0]).err(),
            Some(Error::from("Failed to count terms"))
        );
    }
}

#[test]
fn test_collection() {
    let tmp = TempDir::new("build").unwrap();
    let MockSetup {
        config,
        executor,
        programs,
        outputs,
        term_count,
    } = mock_set_up(&tmp);
    collection(executor.as_ref(), &config.collections[0], &config).unwrap();
    assert_eq!(
        std::fs::read_to_string(outputs.get("parse_collection").unwrap()).unwrap(),
        format!(
            "{} -o {} \
             -f wapo --stemmer porter2 --content-parser html --batch-size 1000",
            programs.get("parse_collection").unwrap().display(),
            tmp.path().join("fwd").display()
        )
    );
    assert_eq!(
        std::fs::read_to_string(outputs.get("invert").unwrap()).unwrap(),
        format!(
            "{} -i {} -o {} --term-count {}",
            programs.get("invert").unwrap().display(),
            tmp.path().join("fwd").display(),
            tmp.path().join("inv").display(),
            term_count
        )
    );
    assert_eq!(
        std::fs::read_to_string(outputs.get("create_freq_index").unwrap()).unwrap(),
        format!(
            "{0} -t block_simdbp -c {1} -o {1}.block_simdbp --check\
             {0} -t block_qmx -c {1} -o {1}.block_qmx --check",
            programs.get("create_freq_index").unwrap().display(),
            tmp.path().join("inv").display(),
        )
    );
    assert_eq!(
        std::fs::read_to_string(outputs.get("create_wand_data").unwrap()).unwrap(),
        format!(
            "{0} -c {1} -o {1}.wand",
            programs.get("create_wand_data").unwrap().display(),
            tmp.path().join("inv").display(),
        )
    );
    assert_eq!(
        std::fs::read_to_string(outputs.get("lexicon").unwrap()).unwrap(),
        format!(
            "{0} build {1}.terms {1}.termmap\
             {0} build {1}.documents {1}.docmap",
            programs.get("lexicon").unwrap().display(),
            tmp.path().join("fwd").display(),
        )
    );
}

#[test]
fn test_suppressed_build() {
    let tmp = TempDir::new("build").unwrap();
    let MockSetup {
        mut config,
        executor,
        programs: _,
        outputs: _,
        term_count: _,
    } = mock_set_up(&tmp);
    config.suppress_stage(Stage::BuildIndex);
    let stages = collection(executor.as_ref(), &config.collections[0], &config).unwrap();
    assert_eq!(stages, vec![]);
}

#[test]
fn test_suppressed_parse_and_invert() {
    let tmp = TempDir::new("build").unwrap();
    let MockSetup {
        mut config,
        executor,
        programs: _,
        outputs: _,
        term_count: _,
    } = mock_set_up(&tmp);
    config.suppress_stage(Stage::ParseCollection);
    config.suppress_stage(Stage::Invert);
    let stages = collection(executor.as_ref(), &config.collections[0], &config).unwrap();
    assert_eq!(stages, vec![Stage::BuildIndex]);
}

#[test]
fn test_parse_wapo_command() {
    let tmp = TempDir::new("tmp").unwrap();
    let data_dir = tmp.path().join("data");
    create_dir(&data_dir).unwrap();
    let data_file = data_dir.join("TREC_Washington_Post_collection.v2.jl");
    File::create(&data_file).unwrap();
    let executor = SystemPathExecutor::new();
    let cconf = Collection {
        name: String::from("wapo"),
        collection_dir: tmp.path().to_path_buf(),
        forward_index: PathBuf::from("fwd"),
        inverted_index: PathBuf::from("inv"),
        encodings: vec![],
    };
    let expected = format!(
        "cat {}\n\t| parse_collection -o fwd \
         -f wapo --stemmer porter2 --content-parser html --batch-size 1000",
        data_file.to_str().unwrap()
    );
    assert_eq!(
        format!(
            "{}",
            parse_wapo_command(&executor, &cconf)
                .unwrap()
                .display(Verbosity::Verbose)
        ),
        expected
    );
    assert_eq!(
        format!(
            "{}",
            parse_command(&executor, &cconf)
                .unwrap()
                .display(Verbosity::Verbose)
        ),
        expected
    );
}
