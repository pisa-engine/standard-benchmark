//! Anything that has to do with building either forward or inverted index,
//! including parsing and compressing.

extern crate boolinator;
extern crate experiment;
extern crate failure;
extern crate glob;
extern crate log;

use super::config::*;
use super::error::Error;
use super::executor::*;
use super::*;
use boolinator::Boolinator;
use experiment::pipeline;
use experiment::process::*;
use failure::format_err;
use glob::glob;
use log::{info, warn};

fn parse_wapo_command(
    executor: &PisaExecutor,
    collection: &Collection,
) -> Result<ProcessPipeline, Error> {
    let input_path = collection.collection_dir.join("data/*.jl");
    let input = input_path.to_str().unwrap();
    let input_files: Vec<_> = glob(input).unwrap().filter_map(Result::ok).collect();
    (!input_files.is_empty()).ok_or(format_err!(
        "could not resolve any files for pattern: {}",
        input
    ))?;
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
                "html",
                "--batch-size",
                "1000"
            ]
        )
    ))
}

fn parse_command(
    executor: &PisaExecutor,
    collection: &Collection,
) -> Result<ProcessPipeline, Error> {
    match collection.name.as_ref() {
        "wapo" => parse_wapo_command(executor, collection),
        _ => unimplemented!(""),
    }
}

/// Builds a requeested collection, using a given executor.
///
/// **Note**: Some steps might be ignored if the `config` struct
/// has been instructed to suppress some stages.
/// ```
/// # extern crate stdbench;
/// # use stdbench::Stage;
/// let stage = Stage::BuildIndex; // suppresses the entire function
/// let stage = Stage::ParseCollection; // suppresses building forward index
/// let stage = Stage::Invert; // as ParseCollection + suppresses building inverted index
/// ```
pub fn collection(
    executor: &PisaExecutor,
    collection: &Collection,
    config: &Config,
) -> Result<(), Error> {
    info!("Processing collection: {}", collection.name);
    if config.is_suppressed(Stage::BuildIndex) {
        warn!("Suppressed index building");
    } else {
        let name = &collection.name;
        info!("[{}] [build] Building index", name);
        ensure_parent_exists(&collection.forward_index)?;
        ensure_parent_exists(&collection.inverted_index)?;
        if config.is_suppressed(Stage::ParseCollection) {
            warn!("[{}] [build] [parse] Suppressed", name);
        } else {
            info!("[{}] [build] [parse] Parsing collection", name);
            let pipeline = parse_command(&*executor, &collection)?;
            debug!("\n{}", pipeline.display(Verbosity::Verbose));
            execute!(pipeline.pipe(); "Failed to parse");
        }
        if config.is_suppressed(Stage::Invert) {
            warn!("[{}] [build] [invert] Suppressed", name);
        } else {
            info!("[{}] [build] [invert] Inverting index", name);
            let term_count = 1;
            execute!(executor.command("invert", &[
                "-i",
                collection.forward_index.to_str().unwrap(),
                "-o",
                collection.inverted_index.to_str().unwrap(),
                "--term-count",
                &term_count.to_string()
            ]).command(); "Failed to invert");
        }
        //unimplemented!();
        //info!("[{}] [build] [compress] Compressing index", name);
        //unimplemented!();
    }
    Ok(())
}

#[cfg(test)]
mod test {
    extern crate tempdir;

    use super::source::*;
    use super::tests::make_echo;
    use super::*;
    use std::collections::HashMap;
    use std::fs::{create_dir, File};
    use std::path::PathBuf;
    use tempdir::TempDir;

    fn set_up(
        tmp: &TempDir,
    ) -> (
        Config,
        Box<PisaExecutor>,
        HashMap<&'static str, PathBuf>,
        HashMap<&'static str, PathBuf>,
    ) {
        stderrlog::new().verbosity(100).init().unwrap();
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

        let mut config = Config::new(tmp.path(), Box::new(CustomPathSource::from(tmp.path())));
        config.collections.push(Collection {
            name: String::from("wapo"),
            collection_dir: tmp.path().join("coll"),
            forward_index: PathBuf::from("fwd/wapo"),
            inverted_index: PathBuf::from("inv/wapo"),
            encodings: vec![],
        });

        let data_dir = tmp.path().join("coll").join("data");
        create_dir_all(&data_dir).unwrap();
        std::fs::File::create(data_dir.join("f.jl")).unwrap();
        let executor = config.executor().unwrap();
        (config, executor, programs, output_paths)
    }

    #[test]
    fn test_collection() {
        let tmp = TempDir::new("build").unwrap();
        let (config, executor, programs, outputs) = set_up(&tmp);
        collection(executor.as_ref(), &config.collections[0], &config).unwrap();
        assert_eq!(
            std::fs::read_to_string(outputs.get("parse").unwrap()).unwrap(),
            format!(
                "{} -o fwd/wapo \
                 -f wapo --stemmer porter2 --content-parser html --batch-size 1000",
                programs.get("parse").unwrap().display()
            )
        );
        assert_eq!(
            std::fs::read_to_string(outputs.get("invert").unwrap()).unwrap(),
            format!(
                "{} -i fwd/wapo -o inv/wapo --term-count {}",
                programs.get("invert").unwrap().display(),
                1
            )
        );
    }

    #[test]
    fn test_parse_wapo_command() {
        let tmp = TempDir::new("tmp").unwrap();
        let data_dir = tmp.path().join("data");
        create_dir(&data_dir).unwrap();
        let data_file = data_dir.join("TREC_Washington_Post_collection.v2.jl");
        File::create(&data_file).unwrap();
        let executor = SystemPathExecutor::new();
        let cconf = Collection {
            name: String::from("wapo"),
            collection_dir: tmp.path().to_path_buf(),
            forward_index: PathBuf::from("fwd"),
            inverted_index: PathBuf::from("inv"),
            encodings: vec![],
        };
        let expected = format!(
            "cat {}\n\t| parse_collection -o fwd \
             -f wapo --stemmer porter2 --content-parser html --batch-size 1000",
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
