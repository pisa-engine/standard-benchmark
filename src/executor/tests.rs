extern crate downcast_rs;
extern crate tempdir;

use super::super::tests::{mock_set_up, MockSetup};
use super::config::*;
use super::source::*;
use super::*;
use std::fs::create_dir_all;
use std::fs::Permissions;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;
use tempdir::TempDir;

fn test_exec<F>(prog: &str, err: &'static str, exec: F)
where
    F: Fn(&MockSetup) -> Result<(), Error>,
{
    {
        let tmp = TempDir::new("executor").unwrap();
        let setup: MockSetup = mock_set_up(&tmp);
        assert!(exec(&setup).is_ok());
    }
    {
        let tmp = TempDir::new("executor").unwrap();
        let setup: MockSetup = mock_set_up(&tmp);
        std::fs::write(setup.programs.get(prog).unwrap(), "#!/bin/bash\nexit 1").unwrap();
        assert_eq!(exec(&setup), Err(Error::from(err)));
    }
    {
        let tmp = TempDir::new("executor").unwrap();
        let setup: MockSetup = mock_set_up(&tmp);
        std::fs::remove_file(setup.programs.get(prog).unwrap()).unwrap();
        assert!(exec(&setup).is_err());
    }
}

#[test]
#[cfg_attr(target_family, unix)]
fn test_invert() {
    test_exec("invert", "Failed to invert index", |setup: &MockSetup| {
        setup.executor.invert(
            &setup.config.collections[0].forward_index,
            &setup.config.collections[0].inverted_index,
            setup.term_count,
        )
    });
}

#[test]
#[cfg_attr(target_family, unix)]
fn test_compress() {
    test_exec(
        "create_freq_index",
        "Failed to compress index",
        |setup: &MockSetup| {
            setup.executor.compress(
                &setup.config.collections[0].forward_index,
                &Encoding::from("block_simdbp"),
            )
        },
    );
}

#[test]
#[cfg_attr(target_family, unix)]
fn test_create_wand_data() {
    test_exec(
        "create_wand_data",
        "Failed to create WAND data",
        |setup: &MockSetup| {
            setup
                .executor
                .create_wand_data(&setup.config.collections[0].inverted_index)
        },
    );
}

#[test]
#[cfg_attr(target_family, unix)]
fn test_custom_path_source_executor() {
    let tmp = TempDir::new("tmp").unwrap();
    let program = "#!/bin/bash
echo ok";
    let program_path = tmp.path().join("program");
    std::fs::write(&program_path, &program).unwrap();
    let permissions = Permissions::from_mode(0o744);
    std::fs::set_permissions(&program_path, permissions).unwrap();

    let source = CustomPathSource::from(tmp.path());
    let config = Config::new("workdir", Box::new(source));
    let executor = config.executor().unwrap();
    let output = executor.command("program", &[]).command().output().unwrap();
    assert_eq!(std::str::from_utf8(&output.stdout).unwrap(), "ok\n");
}

#[test]
fn test_git_executor_wrong_bin() {
    assert_eq!(
        CustomPathExecutor::try_from(PathBuf::from("/nonexistent/path")),
        Err("Failed to construct executor: not a directory: /nonexistent/path".into())
    );
}

#[test]
fn test_init_git_failed_clone() {
    let tmp = TempDir::new("tmp").unwrap();
    let workdir = tmp.path().join("work");
    create_dir_all(&workdir).unwrap();

    let conf = Config::new(&workdir, Box::new(GitSource::new("xxx", "master")));
    assert_eq!(
        conf.source.executor(&conf).err(),
        Some(Error::from("cloning failed"))
    );
}

fn run_from(dir: PathBuf) -> impl Fn(&'static str) -> () {
    move |args: &'static str| {
        let mut args = args.split(" ").into_iter();
        Command::new(args.next().unwrap())
            .current_dir(&dir)
            .args(args.collect::<Vec<&str>>())
            .status()
            .expect("failed git command");
        ()
    }
}

fn set_up_git() -> (TempDir, PathBuf, PathBuf) {
    let tmp = TempDir::new("tmp").unwrap();
    let workdir = tmp.path().join("work");
    let origin_dir = tmp.path().join("origin");
    create_dir_all(&workdir).unwrap();
    create_dir_all(&origin_dir).unwrap();
    let run = run_from(origin_dir.clone());
    run("git init");
    let cmakelists = "cmake_minimum_required(VERSION 3.0)
             add_custom_target(build-time-make-directory ALL
             COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/bin)";
    std::fs::write(origin_dir.join("CMakeLists.txt"), &cmakelists).expect("Unable to write file");
    run("git add CMakeLists.txt");
    run("git commit -m \"c\"");
    (tmp, workdir, origin_dir)
}

#[test]
fn test_init_git() {
    let (_tmp, workdir, origin_dir) = set_up_git();
    let conf = Config::new(
        &workdir,
        Box::new(GitSource::new(&origin_dir.to_str().unwrap(), "master")),
    );
    assert_eq!(
        conf.source
            .executor(&conf)
            .unwrap()
            .downcast_ref::<CustomPathExecutor>(),
        CustomPathExecutor::try_from(
            workdir
                .join("pisa")
                .join("build")
                .join("bin")
                .to_str()
                .unwrap()
        )
        .ok()
        .as_ref()
    );
}

#[test]
fn test_init_git_suppress_compilation() {
    let (_tmp, workdir, origin_dir) = set_up_git();
    let mut conf = Config::new(
        &workdir,
        Box::new(GitSource::new(&origin_dir.to_str().unwrap(), "master")),
    );
    conf.suppress_stage(Stage::Compile);
    assert_eq!(
        conf.source.executor(&conf).err(),
        Some(Error::from(format!(
            "Failed to construct executor: not a directory: {}",
            workdir
                .join("pisa")
                .join("build")
                .join("bin")
                .to_str()
                .unwrap()
        )))
    );
}
