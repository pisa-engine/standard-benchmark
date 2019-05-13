extern crate clap;
#[macro_use]
extern crate experiment;
extern crate boolinator;
extern crate git2;
extern crate glob;
extern crate json;
extern crate stdbench;
extern crate stderrlog;

use boolinator::Boolinator;
use clap::{App, Arg};
use experiment::process::{Process, ProcessPipeline};
use experiment::Verbosity;
use glob::glob;
use log::{error, info, warn};
use std::env;
use std::fmt::Display;
use std::path::PathBuf;
use std::process;
use stdbench::config::{CollectionConfig, Config};
use stdbench::executor::PisaExecutor;
use stdbench::{Error, Stage};

#[cfg_attr(tarpaulin, skip)]
pub fn app<'a, 'b>() -> App<'a, 'b> {
    App::new("PISA standard benchmark for regression tests.")
        .version("1.0")
        .author("Michal Siedlaczek <michal.siedlaczek@gmail.com>")
        .arg(
            Arg::with_name("config-file")
                .long("config-file")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("suppress")
                .long("suppress")
                .multiple(true)
                .takes_value(true),
        )
}

fn parse_config(args: Vec<String>) -> Result<stdbench::config::Config, Error> {
    info!("Parsing config");
    let matches = app().get_matches_from(args);
    let config_file = matches
        .value_of("config-file")
        .expect("failed to read required argument");
    let mut config = Config::from_file(PathBuf::from(config_file))?;
    if let Some(stages) = matches.values_of("suppress") {
        for name in stages {
            if let Some(stage) = Stage::from_name(name) {
                config.suppress_stage(stage);
            } else {
                warn!("Requested suppression of stage `{}` that is invalid", name);
            }
        }
    }
    Ok(config)
}

fn parse_wapo_command(
    executor: &PisaExecutor,
    collection: &CollectionConfig,
) -> Result<ProcessPipeline, Error> {
    let input_path = collection.collection_dir.join("data/*.jl");
    let input = input_path.to_str().unwrap();
    let input_files: Vec<_> = glob(input).unwrap().filter_map(Result::ok).collect();
    (!input_files.is_empty()).ok_or(Error(format!(
        "could not resolve any files for pattern: {}",
        input
    )))?;
    Ok(pipeline!(
        Process::new("cat", &input_files),
        executor.command(
            "parse_collection",
            &[
                "-o",
                collection.forward_index.to_str().unwrap(),
                "-f",
                "wapo",
                "--stemmer",
                "porter2",
                "--content-parser",
                "html"
            ]
        )
    ))
}

fn parse_command(
    executor: &PisaExecutor,
    collection: &CollectionConfig,
) -> Result<ProcessPipeline, Error> {
    match collection.name.as_ref() {
        "wapo" => parse_wapo_command(executor, collection),
        _ => unimplemented!(""),
    }
}

/// Prints out the error with the logger and exits the program.
/// ```
/// # extern crate stdbench;
/// # use::stdbench::*;
/// let x: Result<i32, &str> = Ok(-3);
/// let y = x.unwrap_or_else(exit_gracefully);
/// ```
#[cfg_attr(tarpaulin, skip)]
pub fn exit_gracefully<E: Display, R>(e: E) -> R {
    error!("{}", e);
    // TODO: why is error not working?
    println!("ERROR - {}", e);
    process::exit(1);
}

#[macro_export]
macro_rules! must_succeed {
    ($cmd:expr) => {{
        let status = $cmd;
        if !status.success() {
            std::process::exit(status.code().unwrap_or(1));
        }
    }};
}

#[cfg_attr(tarpaulin, skip)]
fn main() {
    stderrlog::new()
        .verbosity(100)
        .module(module_path!())
        .init()
        .unwrap();
    let config = parse_config(env::args().collect()).unwrap_or_else(exit_gracefully);

    info!("Code source: {:?}", &config.source);
    let executor = config.source.executor(&config).unwrap_or_else(|e| {
        println!("Failed to construct executor: {}", e);
        process::exit(1);
    });
    info!("Executor ready");

    for collection in &config.collections {
        info!("Processing collection: {}", collection.name);
        if config.is_suppressed(Stage::BuildIndex) {
            warn!("Suppressed index building");
        } else {
            let name = &collection.name;
            info!("[{}] [build] Building index", name);
            if config.is_suppressed(Stage::ParseCollection) {
                warn!("[{}] [build] [parse] Suppressed", name);
            } else {
                info!("[{}] [build] [parse] Parsing collection", name);
                let pipeline =
                    parse_command(&*executor, &collection).unwrap_or_else(exit_gracefully);
                println!("EXEC - {}", pipeline.display(Verbosity::Verbose));
                must_succeed!(pipeline.pipe().status().unwrap_or_else(exit_gracefully));
            }
            info!("[{}] [build] [invert] Inverting index", name);
            unimplemented!();
            //info!("[{}] [build] [compress] Compressing index", name);
            //unimplemented!();
        }
    }
}

#[cfg(test)]
mod test {
    extern crate tempdir;

    use super::*;
    use std::fs;
    use tempdir::TempDir;

    #[test]
    fn test_parse_config_missing_file() {
        assert!(parse_config(
            ["exe", "--config-file", "file"]
                .into_iter()
                .map(|&s| String::from(s))
                .collect(),
        )
        .is_err());
    }

    #[test]
    fn test_parse_config() {
        let tmp = TempDir::new("tmp").unwrap();
        let config_file = tmp.path().join("conf.yml");
        let yml = "
workdir: /tmp
source:
    type: git
    branch: dev
    url: https://github.com/pisa-engine/pisa.git
collections:
    - description: WashingtonPost.v2
      forward_index: fwd/wapo
      inverted_index: inv/wapo";
        fs::write(config_file.to_str().unwrap(), &yml).unwrap();
        let conf = parse_config(
            [
                "exe",
                "--config-file",
                "conf.yml",
                "--suppress",
                "compile",
                "invalid",
            ]
            .into_iter()
            .map(|&s| String::from(s))
            .collect(),
        )
        .unwrap();
        assert!(conf.is_suppressed(Stage::Compile));
    }

    #[test]
    fn test_parse_wapo_command() {
        let tmp = TempDir::new("tmp").unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir(&data_dir).unwrap();
        let data_file = data_dir.join("TREC_Washington_Post_collection.v2.jl");
        fs::File::create(&data_file).unwrap();
        let executor = stdbench::executor::SystemPathExecutor::new();
        let cconf = CollectionConfig {
            name: String::from("wapo"),
            description: None,
            collection_dir: tmp.path().to_path_buf(),
            forward_index: PathBuf::from("fwd"),
            inverted_index: PathBuf::from("inv"),
        };
        let expected = format!(
            "cat {}\n\t| parse_collection -o fwd \
             -f wapo --stemmer porter2 --content-parser html",
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
}
