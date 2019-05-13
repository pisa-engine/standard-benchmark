extern crate clap;
#[macro_use]
extern crate experiment;
extern crate git2;
extern crate glob;
extern crate json;
#[macro_use]
extern crate stdbench;
extern crate stderrlog;

use clap::{App, Arg};
use experiment::process::{Process, ProcessPipeline};
use experiment::Verbosity;
use glob::glob;
use log::{info, warn};
use std::path::PathBuf;
use std::process;
use stdbench::config::{CollectionConfig, Config};
use stdbench::executor::PisaExecutor;
use stdbench::{exit_gracefully, Error, Stage};

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

fn parse_config() -> Result<stdbench::config::Config, Error> {
    info!("Parsing config");
    let matches = app().get_matches();
    let config_file = matches
        .value_of("config-file")
        .ok_or_else(|| Error::new("cannot read --config"))?;
    let mut config = Config::from_file(PathBuf::from(config_file))?;
    if let Some(stages) = matches.values_of("suppress") {
        for name in stages {
            if let Some(stage) = Stage::from_name(name) {
                config.suppress_stage(stage);
            } else {
                warn!("Requested suppression of stage `{}` that is invalid", name);
            }
        }
    } else {
        panic!("");
    }
    Ok(config)
}

fn parse_wapo_command(
    executor: &PisaExecutor,
    collection: &CollectionConfig,
) -> Result<ProcessPipeline, Error> {
    let input_path = collection.collection_dir.join("data/*.jl");
    let input = input_path
        .to_str()
        .ok_or_else(|| Error::new("unable to parse path"))?;
    let input_files: Vec<_> = glob(input)
        .or_else(|e| Err(Error(format!("{}", e))))?
        .filter_map(Result::ok)
        .collect();
    if input_files.is_empty() {
        Err(Error(format!(
            "could not resolve any files for pattern: {}",
            input
        )))
    } else {
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
}

fn parse_command(
    executor: &PisaExecutor,
    collection: &CollectionConfig,
) -> Result<ProcessPipeline, Error> {
    match collection.name.as_ref() {
        "wapo" => parse_wapo_command(executor, collection),
        _ => panic!(""),
    }
}

#[cfg_attr(tarpaulin, skip)]
fn main() {
    stderrlog::new()
        .verbosity(100)
        .module(module_path!())
        .init()
        .unwrap();
    let config = parse_config().unwrap_or_else(exit_gracefully);

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
    fn test_parse_wapo_command() {
        let tmp = TempDir::new("tmp").unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir(&data_dir).unwrap();
        let data_file = data_dir.join("TREC_Washington_Post_collection.v2.jl");
        fs::File::create(&data_file).unwrap();
        assert_eq!(
            format!(
                "{}",
                parse_wapo_command(
                    &stdbench::executor::SystemPathExecutor::new(),
                    &CollectionConfig {
                        name: String::from("name"),
                        description: None,
                        collection_dir: tmp.path().to_path_buf(),
                        forward_index: PathBuf::from("fwd"),
                        inverted_index: PathBuf::from("inv"),
                    }
                )
                .unwrap()
                .display(Verbosity::Verbose)
            ),
            format!(
                "cat {}\n\t| parse_collection -o fwd \
                 -f wapo --stemmer porter2 --content-parser html",
                data_file.to_str().unwrap()
            )
        );
    }
}
