use failure::ResultExt;
use log::{error, info};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::{env, fs, mem, process};
use stdbench::run::{compare_with_baseline, process_run, RunStatus};
use stdbench::{
    CMakeVar, Collection, Config, Encoding, Error, RawConfig, ResolvedPathsConfig, Source, Stage,
};
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

    /// Filter out collections you want to run
    #[structopt(long)]
    encodings: Vec<Encoding>,

    /// Remove entire work dir first
    #[structopt(long)]
    clean: bool,

    /// No --scorer in runs (for backwards compatibility)
    #[structopt(long)]
    no_scorer: bool,

    /// CMake flags, e.g., `PISA_ENABLE_TESTING=OFF`.
    /// Only for git source.
    #[structopt(long = "cmake-vars")]
    cmake_vars: Vec<CMakeVar>,
}

fn filter_collections(mut config: &mut RawConfig, collections: Vec<String>) {
    if collections.is_empty() {
        return;
    }
    let colset: HashSet<String> = collections.into_iter().collect();
    config.collections = mem::replace(&mut config.collections, vec![])
        .into_iter()
        .filter(|c| {
            let name = &c.name;
            colset.contains(name)
        })
        .collect();
    config.runs = mem::replace(&mut config.runs, vec![])
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

fn filter_encodings(config: &mut RawConfig, encodings: Vec<Encoding>) {
    if !encodings.is_empty() {
        let encoding_filter: HashSet<Encoding> = encodings.into_iter().collect();
        for collection in &mut config.collections {
            collection.encodings = mem::replace(&mut collection.encodings, vec![])
                .into_iter()
                .filter(|e| encoding_filter.contains(e))
                .collect();
        }
        for run in &mut config.runs {
            run.encodings = mem::replace(&mut run.encodings, vec![])
                .into_iter()
                .filter(|e| encoding_filter.contains(e))
                .collect();
        }
    }
}

fn parse_config(args: Vec<String>, init_log: bool) -> Result<Option<ResolvedPathsConfig>, Error> {
    let Opt {
        config_file,
        verbose,
        log,
        print_stages,
        suppress,
        collections,
        encodings,
        clean,
        no_scorer,
        cmake_vars,
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
    let mut config: RawConfig = serde_yaml::from_reader(fs::File::open(config_file.unwrap())?)
        .context("Failed to parse config")?;
    for stage in suppress {
        config.disable(stage);
    }
    filter_collections(&mut config, collections);
    if let Source::Git {
        cmake_vars: inner_cmake_vars,
        ..
    } = &mut config.source
    {
        if !cmake_vars.is_empty() {
            inner_cmake_vars.clear();
            inner_cmake_vars.extend(cmake_vars);
        }
    }
    if no_scorer {
        config.use_scorer = false;
    }
    if clean {
        config.clean = true;
    }
    let mut config = ResolvedPathsConfig::from(config)?;
    filter_encodings(&mut config.0, encodings);
    Ok(Some(config))
}

enum FinalStatus {
    Success,
    FailedRuns {
        undefined_collections: Vec<String>,
        regressions: Vec<usize>,
    },
}

#[cfg_attr(tarpaulin, skip)]
fn run() -> Result<FinalStatus, Error> {
    let config = parse_config(env::args().collect(), true)?;
    if config.is_none() {
        return Ok(FinalStatus::Success);
    }
    let config = config.unwrap();
    info!("Config: {:?}", &config);

    if config.clean() {
        std::fs::remove_dir_all(&config.workdir())?;
    }

    let executor = config.executor()?;
    info!("Executor ready");

    for collection in config.collections() {
        stdbench::build::collection(&executor, collection, &config)?;
    }
    let collections: HashMap<String, &Collection> = config
        .collections()
        .iter()
        .map(|c| (c.name.to_string(), c))
        .collect();
    let undefined_collections = {
        let mut undefined_collections: Vec<String> = Vec::new();
        if config.enabled(Stage::Run) {
            for run in config.runs() {
                if let Some(collection) = &collections.get(&run.collection) {
                    info!("Processing run: {:?}", run);
                    process_run(&executor, run, collection, config.use_scorer())?;
                } else {
                    undefined_collections.push(run.collection.clone())
                }
            }
        }
        undefined_collections
    };
    let regressions = {
        let mut regressions: Vec<usize> = Vec::new();
        if config.enabled(Stage::Compare) {
            for run in config.runs() {
                if let Some(compare_with) = &run.compare_with {
                    match compare_with_baseline(&executor, run, compare_with, config.margin())? {
                        RunStatus::Success => {}
                        RunStatus::Regression(count) => {
                            regressions.push(count);
                        }
                    }
                }
            }
        }
        regressions
    };
    if undefined_collections.is_empty() && regressions.is_empty() {
        Ok(FinalStatus::Success)
    } else {
        Ok(FinalStatus::FailedRuns {
            undefined_collections,
            regressions,
        })
    }
}

#[cfg_attr(tarpaulin, skip)]
fn main() {
    match run() {
        Err(err) => {
            error!("{}", err);
            process::exit(1);
        }
        Ok(FinalStatus::Success) => {
            info!("Success!");
        }
        Ok(FinalStatus::FailedRuns {
            undefined_collections,
            regressions,
        }) => {
            for name in undefined_collections {
                error!("Undefined collection: {}", name)
            }
            if !regressions.is_empty() {
                error!(
                    "Found {} regressed runs with total of {} regressions",
                    regressions.len(),
                    regressions.into_iter().sum::<usize>()
                );
            }
            process::exit(1);
        }
    }
}

#[cfg(test)]
mod test {
    extern crate tempdir;

    use super::*;
    use std::fs;
    use tempdir::TempDir;

    fn mkfiles(root: &std::path::Path, paths: &[&str]) -> Result<(), Error> {
        for path in paths {
            if path.ends_with('/') {
                fs::create_dir(root.join(path))?;
            } else {
                std::fs::File::create(root.join(path))?;
            }
        }
        Ok(())
    }

    #[test]
    fn test_parse_config() -> Result<(), Error> {
        let tmp = TempDir::new("tmp").unwrap();
        mkfiles(tmp.path(), &["coll"])?;
        let config_file = tmp.path().join("conf.yml");
        let yml = format!(
            "
workdir: {0}
source:
    git:
        branch: dev
        url: https://github.com/pisa-engine/pisa.git
collections:
    - name: wapo
      kind: washington-post
      input_dir: {0}/coll
      fwd_index: fwd/wapo
      inv_index: inv/wapo
      encodings:
        - block_simdbp
    - name: wapo2
      kind: washington-post
      input_dir: {0}/coll
      fwd_index: fwd/wapo
      inv_index: inv/wapo
      encodings:
        - block_simdbp",
            tmp.path().display()
        );
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
        assert!(!conf.enabled(Stage::Compile));
        assert!(conf.use_scorer());

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
        let colnames: Vec<_> = conf.collections().iter().map(|c| c.name.clone()).collect();
        assert_eq!(colnames, vec!["wapo2".to_string()]);
        assert_eq!(conf.use_scorer(), false);

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
