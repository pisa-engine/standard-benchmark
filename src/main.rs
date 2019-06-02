extern crate clap;
extern crate stdbench;
extern crate stderrlog;
extern crate tempdir;

use clap::{App, Arg};
use log::{error, info, warn};
use std::env;
use std::path::PathBuf;
use std::process;
use stdbench::build;
use stdbench::config::Config;
use stdbench::error::Error;
use stdbench::run::process_run;
use strum::IntoEnumIterator;

pub fn app<'a, 'b>() -> App<'a, 'b> {
    App::new("PISA standard benchmark for regression tests.")
        .version("1.0")
        .author("Michal Siedlaczek <michal.siedlaczek@gmail.com>")
        .arg(
            Arg::with_name("config-file")
                .help("Configuration file path")
                .long("config-file")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("print-stages")
                .help("Prints all available stages")
                .long("print-stages"),
        )
        .arg(
            Arg::with_name("suppress")
                .help("A list of stages to suppress")
                .long("suppress")
                .multiple(true)
                .takes_value(true),
        )
}

fn parse_config(args: Vec<String>) -> Result<Config, Error> {
    let matches = app().get_matches_from(args);
    if matches.is_present("print-stages") {
        for stage in stdbench::Stage::iter() {
            println!("{}", stage);
        }
        process::exit(0);
    }
    info!("Parsing config");
    let config_file = matches
        .value_of("config-file")
        .ok_or("failed to read required argument")?;
    let mut config = Config::from_file(PathBuf::from(config_file))?;
    if let Some(stages) = matches.values_of("suppress") {
        for name in stages {
            if let Ok(stage) = name.parse() {
                config.suppress_stage(stage);
            } else {
                warn!("Requested suppression of stage `{}` that is invalid", name);
            }
        }
    }
    Ok(config)
}

#[cfg_attr(tarpaulin, skip)]
fn run() -> Result<(), Error> {
    stderrlog::new().verbosity(100).init().unwrap();
    let config = parse_config(env::args().collect())?;
    info!("Code source: {:?}", &config.source);
    let executor = config.executor()?;
    info!("Executor ready");

    for collection in &config.collections {
        build::collection(executor.as_ref(), collection, &config)?;
    }
    for run in &config.runs {
        info!("{:?}", run);
        process_run(executor.as_ref(), run)?;
    }
    Ok(())
}

#[cfg_attr(tarpaulin, skip)]
fn main() {
    if let Err(err) = run() {
        error!("{}", err);
        process::exit(1);
    }
}

#[cfg(test)]
mod test {
    extern crate tempdir;

    use super::stdbench::Stage;
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
    - name: wapo
      description: WashingtonPost.v2
      collection_dir: coll
      forward_index: fwd/wapo
      inverted_index: inv/wapo
      encodings:
        - block_simdbp";
        fs::write(config_file.to_str().unwrap(), &yml).unwrap();
        let conf = parse_config(
            [
                "exe",
                "--config-file",
                config_file.to_str().unwrap(),
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
}
