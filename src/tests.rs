extern crate tempdir;

use super::config::*;
use super::executor::PisaExecutor;
use super::run::Run;
use super::source::*;
use super::*;
use boolinator::Boolinator;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs::Permissions;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use tempdir::TempDir;

pub(crate) struct MockSetup {
    pub config: Config,
    pub executor: Box<PisaExecutor>,
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
        name: String::from("wapo"),
        collection_dir: tmp.path().join("coll"),
        forward_index: tmp.path().join("fwd"),
        inverted_index: tmp.path().join("inv"),
        encodings: vec!["block_simdbp".into(), "block_qmx".into()],
    }));
    config.runs.push(Run::Evaluate {
        collection: Rc::clone(config.collections.last().unwrap()),
        topics: PathBuf::from("topics"),
        qrels: PathBuf::from("qrels"),
    });

    let data_dir = tmp.path().join("coll").join("data");
    create_dir_all(&data_dir).unwrap();
    std::fs::File::create(data_dir.join("f.jl")).unwrap();
    let executor = config.executor().unwrap();

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
        .command("e", &["arg1", "--a", "arg2"])
        .command()
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
