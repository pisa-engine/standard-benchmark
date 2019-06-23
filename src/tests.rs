extern crate tempdir;

use super::config::*;
use super::executor::PisaExecutor;
use super::run::{EvaluateData, Run, RunData, TopicsFormat, TrecTopicField};
use super::source::*;
use super::*;
use boolinator::Boolinator;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::env::{set_var, var};
use std::fs::Permissions;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use tempdir::TempDir;

pub(crate) struct MockSetup {
    pub config: Config,
    pub executor: Box<dyn PisaExecutor>,
    pub programs: HashMap<&'static str, PathBuf>,
    pub outputs: HashMap<&'static str, PathBuf>,
    pub term_count: usize,
}

fn mock_program(tmp: &TempDir, setup: &mut MockSetup, program: &'static str) {
    let path = tmp.path().join(format!("{}.out", program));
    let prog = tmp.path().join(program);
    make_echo(&prog, &path).unwrap();
    setup.outputs.insert(program, path);
    setup.programs.insert(program, prog);
}

pub(crate) fn mock_set_up(tmp: &TempDir) -> MockSetup {
    let mut config = Config::new(tmp.path(), Box::new(CustomPathSource::from(tmp.path())));
    config.collections.push(Rc::new(Collection {
        name: "wapo".to_string(),
        kind: WashingtonPostCollection::boxed(),
        collection_dir: tmp.path().join("coll"),
        forward_index: tmp.path().join("fwd"),
        inverted_index: tmp.path().join("inv"),
        encodings: vec!["block_simdbp".into(), "block_qmx".into()],
    }));
    config.collections.push(Rc::new(Collection {
        name: "gov2".to_string(),
        kind: TrecWebCollection::boxed(),
        collection_dir: tmp.path().join("gov2"),
        forward_index: tmp.path().join("gov2/fwd"),
        inverted_index: tmp.path().join("gov2/inv"),
        encodings: vec!["block_simdbp".into(), "block_qmx".into()],
    }));
    config.collections.push(Rc::new(Collection {
        name: "cw09b".to_string(),
        kind: WarcCollection::boxed(),
        collection_dir: tmp.path().join("cw09b"),
        forward_index: tmp.path().join("cw09b/fwd"),
        inverted_index: tmp.path().join("cw09b/inv"),
        encodings: vec!["block_simdbp".into(), "block_qmx".into()],
    }));
    config.runs.push(Run {
        collection: Rc::clone(&config.collections[0]),
        data: RunData::Evaluate(EvaluateData {
            topics: PathBuf::from("topics"),
            topics_format: TopicsFormat::Trec(TrecTopicField::Title),
            qrels: PathBuf::from("qrels"),
            output_file: PathBuf::from("output.trec"),
        }),
    });
    config.runs.push(Run {
        collection: Rc::clone(&config.collections[0]),
        data: RunData::Evaluate(EvaluateData {
            topics: PathBuf::from("topics"),
            topics_format: TopicsFormat::Simple,
            qrels: PathBuf::from("qrels"),
            output_file: PathBuf::from("output.trec"),
        }),
    });
    config.runs.push(Run {
        collection: Rc::clone(&config.collections[0]),
        data: RunData::Benchmark,
    });

    let data_dir = tmp.path().join("coll").join("data");
    create_dir_all(&data_dir).unwrap();
    std::fs::File::create(data_dir.join("f.jl")).unwrap();
    let executor = config.executor().unwrap();

    let gov2_dir = tmp.path().join("gov2");
    let gov2_0_dir = gov2_dir.join("GX000");
    let gov2_1_dir = gov2_dir.join("GX001");
    create_dir_all(&gov2_0_dir).unwrap();
    create_dir_all(&gov2_1_dir).unwrap();
    std::fs::File::create(gov2_0_dir.join("00.gz")).unwrap();
    std::fs::File::create(gov2_0_dir.join("01.gz")).unwrap();
    std::fs::File::create(gov2_1_dir.join("02.gz")).unwrap();
    std::fs::File::create(gov2_1_dir.join("03.gz")).unwrap();

    let cw_dir = tmp.path().join("cw09b");
    let cw_0_dir = cw_dir.join("en0000");
    let cw_1_dir = cw_dir.join("en0001");
    create_dir_all(&cw_0_dir).unwrap();
    create_dir_all(&cw_1_dir).unwrap();
    std::fs::File::create(cw_0_dir.join("00.warc.gz")).unwrap();
    std::fs::File::create(cw_0_dir.join("01.warc.gz")).unwrap();
    std::fs::File::create(cw_1_dir.join("02.warc.gz")).unwrap();
    std::fs::File::create(cw_1_dir.join("03.warc.gz")).unwrap();

    let mut mock_setup = MockSetup {
        config,
        executor,
        programs: HashMap::new(),
        outputs: HashMap::new(),
        term_count: 3,
    };

    mock_program(&tmp, &mut mock_setup, "parse_collection");
    mock_program(&tmp, &mut mock_setup, "invert");
    mock_program(&tmp, &mut mock_setup, "create_freq_index");
    mock_program(&tmp, &mut mock_setup, "create_wand_data");
    mock_program(&tmp, &mut mock_setup, "lexicon");
    mock_program(&tmp, &mut mock_setup, "evaluate_queries");
    mock_program(&tmp, &mut mock_setup, "extract_topics");
    mock_program(&tmp, &mut mock_setup, "trec_eval");
    set_var(
        "PATH",
        format!(
            "{}:{}",
            tmp.path().display(),
            var("PATH").unwrap_or_else(|_| String::from(""))
        ),
    );
    std::fs::write(tmp.path().join("fwd.terms"), "term1\nterm2\nterm3\n").unwrap();

    mock_setup
}

pub(crate) fn make_echo<P, Q>(program: P, output: Q) -> Result<(), Error>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    if cfg!(unix) {
        let code = format!(
            "#!/bin/bash\necho -n \"$0 $@\" >> {}",
            output.as_ref().display()
        );
        std::fs::write(&program, &code)?;
        std::fs::set_permissions(&program, Permissions::from_mode(0o744)).unwrap();
        Ok(())
    } else {
        Err("this function is only supported on UNIX systems".into())
    }
}

#[derive(Debug, Default)]
pub(crate) struct TestLogger {
    pub(crate) sinks: Arc<Option<usize>>, //pub(crate) messages: Option<Arc<RwLock<Vec<String>>>>
}

#[test]
fn test_make_echo() {
    let tmp = TempDir::new("echo").unwrap();
    let echo = tmp.path().join("e");
    let output = tmp.path().join("output");
    make_echo(&echo, &output).unwrap();
    let executor = super::executor::CustomPathExecutor::try_from(tmp.path()).unwrap();
    executor
        .command("e")
        .args(&["arg1", "--a", "arg2"])
        .status()
        .unwrap();
    let output_text = std::fs::read_to_string(&output).unwrap();
    assert_eq!(output_text, format!("{} arg1 --a arg2", echo.display()));
}

#[test]
fn test_execute_failed_to_start() {
    struct MockCommand {};
    impl MockCommand {
        fn status(&self) -> Result<std::process::ExitStatus, &'static str> {
            Err("Oops")
        }
    }
    let f = || -> Result<(), Error> {
        execute!(MockCommand{}; "err");
        Ok(())
    };
    assert_eq!(f().err(), Some(Error::from("Oops")));
}
