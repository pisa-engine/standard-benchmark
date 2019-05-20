extern crate tempdir;

use super::config::*;
use super::executor::PisaExecutor;
use super::source::*;
use super::*;
use boolinator::Boolinator;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs::Permissions;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use tempdir::TempDir;

pub(crate) struct MockSetup {
    pub config: Config,
    pub executor: Box<PisaExecutor>,
    pub programs: HashMap<&'static str, PathBuf>,
    pub outputs: HashMap<&'static str, PathBuf>,
    pub term_count: usize,
}

pub(crate) fn mock_set_up(tmp: &TempDir) -> MockSetup {
    let mut output_paths: HashMap<&'static str, PathBuf> = HashMap::new();
    let mut programs: HashMap<&'static str, PathBuf> = HashMap::new();

    let parse_path = tmp.path().join("parse_collection.out");
    let parse_prog = tmp.path().join("parse_collection");
    make_echo(&parse_prog, &parse_path).unwrap();
    output_paths.insert("parse", parse_path);
    programs.insert("parse", parse_prog);

    let invert_path = tmp.path().join("invert.out");
    let invert_prog = tmp.path().join("invert");
    make_echo(&invert_prog, &invert_path).unwrap();
    output_paths.insert("invert", invert_path);
    programs.insert("invert", invert_prog);
    std::fs::write(tmp.path().join("fwd.terms"), "term1\nterm2\nterm3\n").unwrap();

    let compress_path = tmp.path().join("create_freq_index.out");
    let compress_prog = tmp.path().join("create_freq_index");
    make_echo(&compress_prog, &compress_path).unwrap();
    output_paths.insert("compress", compress_path);
    programs.insert("compress", compress_prog);

    let wand_path = tmp.path().join("create_wand_data.out");
    let wand_prog = tmp.path().join("create_wand_data");
    make_echo(&wand_prog, &wand_path).unwrap();
    output_paths.insert("wand", wand_path);
    programs.insert("wand", wand_prog);

    let mut config = Config::new(tmp.path(), Box::new(CustomPathSource::from(tmp.path())));
    config.collections.push(Collection {
        name: String::from("wapo"),
        collection_dir: tmp.path().join("coll"),
        forward_index: tmp.path().join("fwd"),
        inverted_index: tmp.path().join("inv"),
        encodings: vec!["block_simdbp".into(), "block_qmx".into()],
    });

    let data_dir = tmp.path().join("coll").join("data");
    create_dir_all(&data_dir).unwrap();
    std::fs::File::create(data_dir.join("f.jl")).unwrap();
    let executor = config.executor().unwrap();
    MockSetup {
        config,
        executor,
        programs,
        outputs: output_paths,
        term_count: 3,
    }
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
