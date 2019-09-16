use clap::{App, Arg};
use failure::ResultExt;
use log::{error, info, warn};
use std::collections::{HashMap, HashSet};
use std::{env, fs, process};
use stdbench::build;
use stdbench::config::{Collection, Config, Stage};
use stdbench::run::process_run;
use stdbench::Error;
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
                .required_unless("print-stages"),
        )
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .arg(
            Arg::with_name("log")
                .long("log")
                .help("Store logs in a file (PISA output excluded)"),
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
        .arg(
            Arg::with_name("collections")
                .help("Filter out collections you want to run")
                .long("collections")
                .multiple(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("clean")
                .help("Remove entire work dir first")
                .long("clean"),
        )
        .arg(
            Arg::with_name("no-scorer")
                .help("No --scorer in runs (for backwards compatibility)")
                .long("no-scorer"),
        )
}

fn filter_collections<'a, I>(mut config: &mut Config, collections: I)
where
    I: IntoIterator<Item = &'a str>,
{
    let colset = collections.into_iter().collect::<HashSet<&str>>();
    config.collections = std::mem::replace(&mut config.collections, vec![])
        .into_iter()
        .filter(|c| {
            let name = &c.name;
            colset.contains(&name.as_ref())
        })
        .collect();
    config.runs = std::mem::replace(&mut config.runs, vec![])
        .into_iter()
        .filter(|r| colset.contains(&r.collection.as_ref()))
        .collect();
    // TODO(michal): Replace the above with drain_filter once it stabilizes:
    //               https://github.com/rust-lang/rust/issues/43244
    //
    // config.collections.drain_filter(|c| {
    //     let name = &c.name;
    //     !colset.contains(&name.as_ref())
    // });
    // config
    //     .runs
    //     .drain_filter(|r| colset.contains(&r.collection.as_ref()));
}

fn parse_config(args: Vec<String>, init_log: bool) -> Result<Option<Config>, Error> {
    let matches = app().get_matches_from(args);
    if init_log {
        let log_level = match matches.occurrences_of("v") {
            0 => "info",
            1 => "debug",
            _ => "trace",
        };
        let logger = flexi_logger::Logger::with_env_or_str(log_level);
        if matches.is_present("log") {
            logger
                .log_to_file()
                .duplicate_to_stderr(flexi_logger::Duplicate::All)
                .start()
                .unwrap();
        } else {
            logger.start().unwrap();
        }
    }
    if matches.is_present("print-stages") {
        for stage in Stage::iter() {
            println!("{}", stage);
        }
        return Ok(None);
    }
    info!("Parsing config");
    let config_file = matches
        .value_of("config-file")
        .ok_or("failed to read required argument")?;
    let mut config: Config =
        serde_yaml::from_reader(fs::File::open(config_file)?).context("Failed to parse config")?;
    if let Some(stages) = matches.values_of("suppress") {
        for name in stages {
            if let Ok(stage) = serde_yaml::from_str(name) {
                config.disable(stage);
            } else {
                warn!("Requested suppression of stage `{}` that is invalid", name);
            }
        }
    }
    if let Some(collections) = matches.values_of("collections") {
        filter_collections(&mut config, collections);
    }
    if matches.is_present("no-scorer") {
        config.use_scorer = false;
    }
    if matches.is_present("clean") {
        config.clean = true;
    }
    Ok(Some(config))
}

#[cfg_attr(tarpaulin, skip)]
fn run() -> Result<(), Error> {
    let config = parse_config(env::args().collect(), true)?;
    if config.is_none() {
        return Ok(());
    }
    let config = config.unwrap();
    info!("Config: {:?}", &config);

    if config.clean {
        std::fs::remove_dir_all(&config.workdir)?;
    }

    let executor = config.executor()?;
    info!("Executor ready");

    for collection in &config.collections {
        build::collection(&executor, collection, &config)?;
    }
    let collections: HashMap<String, &Collection> = config
        .collections
        .iter()
        .map(|c| (c.name.to_string(), c))
        .collect();
    for run in &config.runs {
        if let Some(collection) = &collections.get(&run.collection) {
            info!("{:?}", run);
            process_run(&executor, run, collection, config.use_scorer)?;
        } else {
            // TODO
            error!("{:?}", run);
        }
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

    use super::*;
    use std::fs;
    use tempdir::TempDir;

    #[test]
    fn test_parse_config_missing_file() {
        env::set_var("RUST_LOG", "off");
        assert!(parse_config(
            ["exe", "--config-file", "file"]
                .into_iter()
                .map(|&s| String::from(s))
                .collect(),
            false
        )
        .is_err());
    }

    #[test]
    fn test_parse_config() -> Result<(), Error> {
        let tmp = TempDir::new("tmp").unwrap();
        let config_file = tmp.path().join("conf.yml");
        let yml = "
workdir: /tmp
source:
    git:
        branch: dev
        url: https://github.com/pisa-engine/pisa.git
collections:
    - name: wapo
      kind: washington-post
      input_dir: coll
      fwd_index: fwd/wapo
      inv_index: inv/wapo
      encodings:
        - block_simdbp
    - name: wapo2
      kind: washington-post
      input_dir: coll
      fwd_index: fwd/wapo
      inv_index: inv/wapo
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
            false,
        )?
        .unwrap();
        assert!(!conf.stages[&Stage::Compile]);
        assert_eq!(conf.use_scorer, true);

        let conf = parse_config(
            [
                "exe",
                "--config-file",
                config_file.to_str().unwrap(),
                "--collections",
                "wapo2",
                "--no-scorer",
            ]
            .into_iter()
            .map(|&s| String::from(s))
            .collect(),
            false,
        )?
        .unwrap();
        let colnames: Vec<_> = conf.collections.iter().map(|c| c.name.clone()).collect();
        assert_eq!(colnames, vec!["wapo2".to_string()]);
        assert_eq!(conf.use_scorer, false);

        assert!(parse_config(
            ["exe", "--print-stages"]
                .into_iter()
                .map(|&s| String::from(s))
                .collect(),
            false
        )?
        .is_none());
        Ok(())
    }
}
