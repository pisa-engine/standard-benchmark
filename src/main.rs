use failure::ResultExt;
use log::{error, info};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::{env, fs, process};
use stdbench::config::{Collection, Config, Stage};
use stdbench::run::process_run;
use stdbench::Error;
use structopt::StructOpt;
use strum::IntoEnumIterator;

#[derive(StructOpt, Debug)]
#[structopt(name = "PISA Regression Benchmark Suite")]
struct Opt {
    /// Prints all available stages
    #[structopt(long)]
    print_stages: bool,

    /// Configuration file path
    #[structopt(long, parse(from_os_str), required_unless = "print-stages")]
    config_file: Option<PathBuf>,

    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[structopt(short, long, parse(from_occurrences))]
    verbose: u8,

    /// Store logs in a file (PISA output excluded)
    #[structopt(long)]
    log: bool,

    /// A list of stages to suppress
    #[structopt(long)]
    suppress: Vec<Stage>,

    /// Filter out collections you want to run
    #[structopt(long)]
    collections: Vec<String>,

    /// Remove entire work dir first
    #[structopt(long)]
    clean: bool,

    /// No --scorer in runs (for backwards compatibility)
    #[structopt(long)]
    no_scorer: bool,
}

fn filter_collections(mut config: &mut Config, collections: &[String]) {
    let colset = collections.iter().collect::<HashSet<&String>>();
    config.collections = std::mem::replace(&mut config.collections, vec![])
        .into_iter()
        .filter(|c| {
            let name = &c.name;
            colset.contains(&name)
        })
        .collect();
    config.runs = std::mem::replace(&mut config.runs, vec![])
        .into_iter()
        .filter(|r| colset.contains(&r.collection))
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
    let Opt {
        config_file,
        verbose,
        log,
        print_stages,
        suppress,
        collections,
        clean,
        no_scorer,
    } = Opt::from_iter_safe(&args).unwrap_or_else(|err| err.exit());
    if init_log {
        let log_level = match verbose {
            0 => "info",
            1 => "debug",
            _ => "trace",
        };
        let logger = flexi_logger::Logger::with_env_or_str(log_level);
        if log {
            logger
                .log_to_file()
                .duplicate_to_stderr(flexi_logger::Duplicate::All)
                .start()
                .unwrap();
        } else {
            logger.start().unwrap();
        }
    }
    if print_stages {
        for stage in Stage::iter() {
            println!("{}", stage);
        }
        return Ok(None);
    }
    info!("Parsing config");
    let mut config: Config = serde_yaml::from_reader(fs::File::open(config_file.unwrap())?)
        .context("Failed to parse config")?;
    for stage in suppress {
        config.disable(stage);
    }
    if !collections.is_empty() {
        filter_collections(&mut config, &collections);
    }
    if no_scorer {
        config.use_scorer = false;
    }
    if clean {
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
        stdbench::build::collection(&executor, collection, &config)?;
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
