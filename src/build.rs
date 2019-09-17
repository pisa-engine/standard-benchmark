//! Anything that has to do with building either forward or inverted index,
//! including parsing and compressing.

extern crate boolinator;
extern crate failure;
extern crate log;

use crate::config::{resolve_files, Collection, CollectionKind, Config, Stage};
use crate::ensure_parent_exists;
use crate::error::Error;
use crate::executor::Executor;
use crate::CommandDebug;
use boolinator::Boolinator;
use failure::ResultExt;
use log::{info, warn};
use os_pipe::pipe;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    process::Command,
};

/// Retrieves the term count of an already built collection.
///
/// Internally, it counts lines of the terms file of the forward index.
/// If it's not yet built, this function will return an error.
fn term_count(collection: &Collection) -> Result<usize, Error> {
    let output = Command::new("wc")
        .args(&["-l", &format!("{}.terms", collection.fwd_index.display())])
        .output()
        .context("Failed to count terms")?;
    output.status.success().ok_or("Failed to count terms")?;
    let term_count_str = String::from_utf8(output.stdout).context("Failed to parse UTF-8")?;
    let parsing_error = "could not parse output of `wc -l`";
    let count = term_count_str[..]
        .split_whitespace()
        .find(|s| !s.is_empty())
        .expect(parsing_error)
        .parse::<usize>()
        .expect(parsing_error);
    Ok(count)
}

fn merge_parsed_batches(executor: &Executor, collection: &Collection) -> Result<(), Error> {
    let batch_pattern = format!("{}.batch.*documents", collection.fwd_index.display());
    let batch_doc_files = resolve_files(&batch_pattern)?;
    let batch_count = batch_doc_files.len();
    let document_count = batch_doc_files
        .iter()
        .map(|f| Ok(BufReader::new(File::open(f)?).lines().count()))
        .fold(
            Ok(0_usize),
            |acc: Result<usize, Error>, count: Result<usize, Error>| Ok(acc? + count?),
        )?;
    executor
        .command("parse_collection")
        .args(&["--output", collection.fwd_index.to_str().unwrap()])
        .arg("merge")
        .args(&["--batch-count", &batch_count.to_string()])
        .args(&["--document-count", &document_count.to_string()])
        .log()
        .status()?
        .success()
        .ok_or("Failed to merge collection batches")?;
    Ok(())
}

fn parsing_commands(
    executor: &Executor,
    collection: &Collection,
) -> Result<(Command, Command), Error> {
    match &collection.kind {
        CollectionKind::Warc => {
            let input_files = resolve_files(collection.input_dir.join("*/*.gz"))?;
            let mut cat = Command::new("zcat");
            cat.args(&input_files);
            let mut parse = executor.command("parse_collection");
            parse
                .args(&["-o", collection.fwd_index.to_str().unwrap()])
                .args(&["-f", "warc"])
                .args(&["--stemmer", "porter2"])
                .args(&["--content-parser", "html"])
                .args(&["--batch-size", "1000"]);
            Ok((cat, parse))
        }
        CollectionKind::TrecWeb => {
            let input_files = resolve_files(collection.input_dir.join("*/*.gz"))?;
            let mut cat = Command::new("zcat");
            cat.args(&input_files);
            let mut parse = executor.command("parse_collection");
            parse
                .args(&["-o", collection.fwd_index.to_str().unwrap()])
                .args(&["-f", "trecweb"])
                .args(&["--stemmer", "porter2"])
                .args(&["--content-parser", "html"])
                .args(&["--batch-size", "1000"]);
            Ok((cat, parse))
        }
        CollectionKind::WashingtonPost => {
            let input_files = resolve_files(collection.input_dir.join("data/*.jl"))?;
            let mut cat = Command::new("zcat");
            cat.args(&input_files);
            let mut parse = executor.command("parse_collection");
            parse
                .args(&["-o", collection.fwd_index.to_str().unwrap()])
                .args(&["-f", "wapo"])
                .args(&["--stemmer", "porter2"])
                .args(&["--content-parser", "html"])
                .args(&["--batch-size", "1000"]);
            Ok((cat, parse))
        }
    }
}

/// Builds a requeested collection, using a given executor.
pub fn collection(
    executor: &Executor,
    collection: &Collection,
    config: &Config,
) -> Result<(), Error> {
    info!(
        "Processing collection: {}/{:?}",
        collection.name, collection.kind
    );
    let name = &collection.name;
    if config.enabled(Stage::BuildIndex) {
        info!("[{}] [build] Building index", name);
        ensure_parent_exists(&collection.fwd_index)?;
        ensure_parent_exists(&collection.inv_index)?;
        if config.enabled(Stage::Parse) {
            if config.enabled(Stage::ParseBatches) {
                info!("[{}] [build] [parse] Parsing collection", name);
                let (mut cat, mut parse) = parsing_commands(&executor, &collection)?;
                let (reader, writer) = pipe().expect("Failed opening a pipe");
                cat.log().stdout(writer).spawn()?;
                parse.stdin(reader);
                parse.log().status()?.success().ok_or("Failed to parse")?;
            } else {
                warn!("[{}] [build] [parse] Only merging", name);
                merge_parsed_batches(executor, &collection)?;
            }
            let fwd = collection.fwd_index.display();
            executor.build_lexicon(format!("{}.terms", fwd), format!("{}.termmap", fwd))?;
            executor.build_lexicon(format!("{}.documents", fwd), format!("{}.docmap", fwd))?;
        } else {
            warn!("[{}] [build] [parse] Suppressed", name);
        }
        if config.enabled(Stage::Invert) {
            info!("[{}] [build] [invert] Inverting index", name);
            executor.invert(
                &collection.fwd_index,
                &collection.inv_index,
                term_count(&collection)?,
            )?;
        } else {
            warn!("[{}] [build] [invert] Suppressed", name);
        }
        if config.enabled(Stage::Compress) {
            info!("[{}] [build] [compress] Compressing index", name);
            for encoding in &collection.encodings {
                executor.compress(&collection.inv_index, encoding)?;
            }
        } else {
            warn!("[{}] [build] [compress] Suppressed", name);
        }
        if config.enabled(Stage::Wand) {
            info!("[{}] [build] [wand] Creating WAND data", name);
            executor.create_wand_data(&collection.inv_index, config.use_scorer)?;
        } else {
            warn!("[{}] [build] [wand] Suppressed", name);
        }
    } else {
        warn!("[{}] [build] Suppressed", name);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{mock_set_up, MockSetup};
    use crate::CommandDebug;
    use std::fs;
    use std::path::PathBuf;
    use tempdir::TempDir;

    #[test]
    #[cfg_attr(target_family, unix)]
    fn test_term_count() {
        {
            let tmp = TempDir::new("build").unwrap();
            let setup = mock_set_up(&tmp);
            assert_eq!(term_count(&setup.config.collections[0]), Ok(3));
        }
        {
            let tmp = TempDir::new("build").unwrap();
            let setup = mock_set_up(&tmp);
            std::fs::remove_file(tmp.path().join("fwd.terms")).unwrap();
            assert_eq!(
                term_count(&setup.config.collections[0]).err(),
                Some(Error::from("Failed to count terms"))
            );
        }
    }

    #[test]
    #[cfg_attr(target_family, unix)]
    fn test_merge_batches() -> Result<(), Error> {
        let tmp = TempDir::new("build").unwrap();
        let MockSetup {
            config,
            executor,
            programs,
            outputs,
            ..
        } = mock_set_up(&tmp);
        let coll = &config.collections[0];
        std::fs::write(
            format!("{}.batch.0.documents", coll.fwd_index.display()),
            "doc1\ndoc2\n",
        )?;
        std::fs::write(
            format!("{}.batch.1.documents", coll.fwd_index.display()),
            "doc3\ndoc4\n",
        )?;
        std::fs::write(
            format!("{}.batch.2.documents", coll.fwd_index.display()),
            "doc5\n",
        )?;
        assert!(merge_parsed_batches(&executor, coll).is_ok());
        assert_eq!(
            std::fs::read_to_string(outputs.get("parse_collection").unwrap()).unwrap(),
            format!(
                "{} --output {} merge --batch-count 3 --document-count 5\n",
                programs.get("parse_collection").unwrap().display(),
                coll.fwd_index.display()
            )
        );
        Ok(())
    }

    #[test]
    fn test_collection() {
        let tmp = TempDir::new("build").unwrap();
        let MockSetup {
            config,
            executor,
            programs,
            outputs,
            term_count,
        } = mock_set_up(&tmp);
        collection(&executor, &config.collections[0], &config).unwrap();
        assert_eq!(
            std::fs::read_to_string(outputs.get("parse_collection").unwrap()).unwrap(),
            format!(
                "{} -o {} \
                 -f wapo --stemmer porter2 --content-parser html --batch-size 1000\n",
                programs.get("parse_collection").unwrap().display(),
                tmp.path().join("fwd").display()
            )
        );
        assert_eq!(
            std::fs::read_to_string(outputs.get("invert").unwrap()).unwrap(),
            format!(
                "{} -i {} -o {} --term-count {}\n",
                programs.get("invert").unwrap().display(),
                tmp.path().join("fwd").display(),
                tmp.path().join("inv").display(),
                term_count
            )
        );
        assert_eq!(
            std::fs::read_to_string(outputs.get("create_freq_index").unwrap()).unwrap(),
            format!(
                "{0} -t block_simdbp -c {1} -o {1}.block_simdbp --check\n\
                 {0} -t block_qmx -c {1} -o {1}.block_qmx --check\n",
                programs.get("create_freq_index").unwrap().display(),
                tmp.path().join("inv").display(),
            )
        );
        assert_eq!(
            std::fs::read_to_string(outputs.get("create_wand_data").unwrap()).unwrap(),
            format!(
                "{0} -c {1} -o {1}.wand --scorer bm25\n",
                programs.get("create_wand_data").unwrap().display(),
                tmp.path().join("inv").display(),
            )
        );
        assert_eq!(
            std::fs::read_to_string(outputs.get("lexicon").unwrap()).unwrap(),
            format!(
                "{0} build {1}.terms {1}.termmap\n\
                 {0} build {1}.documents {1}.docmap\n",
                programs.get("lexicon").unwrap().display(),
                tmp.path().join("fwd").display(),
            )
        );
    }

    //     #[test]
    //     fn test_suppressed_build() {
    //         let tmp = TempDir::new("build").unwrap();
    //         let MockSetup {
    //             mut config,
    //             executor,
    //             ..
    //         } = mock_set_up(&tmp);
    //         config.disable(Stage::BuildIndex);
    //         let stages = collection(&executor, &config.collections[0], &config).unwrap();
    //         assert_eq!(stages, vec![]);
    //     }

    //     #[test]
    //     fn test_suppressed_parse_and_invert() {
    //         let tmp = TempDir::new("build").unwrap();
    //         let MockSetup {
    //             mut config,
    //             executor,
    //             programs: _,
    //             outputs: _,
    //             term_count: _,
    //         } = mock_set_up(&tmp);
    //         config.suppress_stage(Stage::ParseCollection);
    //         config.suppress_stage(Stage::Invert);
    //         let stages = collection(executor.as_ref(), &config.collections[0], &config).unwrap();
    //         assert_eq!(stages, vec![Stage::BuildIndex]);
    //     }

    //     #[test]
    //     fn test_suppressed_parse_batches() -> Result<(), Error> {
    //         let tmp = TempDir::new("build").unwrap();
    //         let MockSetup {
    //             mut config,
    //             executor,
    //             programs: _,
    //             outputs: _,
    //             term_count: _,
    //         } = mock_set_up(&tmp);
    //         std::fs::File::create(format!(
    //             "{}.batch.0.documents",
    //             &config.collections[0].fwd()?
    //         ))
    //         .unwrap();
    //         config.suppress_stage(Stage::ParseBatches);
    //         let stages = collection(executor.as_ref(), &config.collections[0], &config).unwrap();
    //         assert_eq!(stages, vec![Stage::BuildIndex, Stage::Invert]);
    //         Ok(())
    //     }

    #[test]
    fn test_parse_wapo_command() -> Result<(), Error> {
        let tmp = TempDir::new("tmp").unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir(&data_dir).unwrap();
        let data_file = data_dir.join("TREC_Washington_Post_collection.v2.jl");
        File::create(&data_file).unwrap();
        let executor = Executor::default();
        let cconf = Collection {
            name: "wapo".to_string(),
            kind: CollectionKind::WashingtonPost,
            input_dir: tmp.path().to_path_buf(),
            fwd_index: PathBuf::from("fwd"),
            inv_index: PathBuf::from("inv"),
            encodings: vec![],
        };
        let (cat, parse) = parsing_commands(&executor, &cconf)?;
        assert_eq!(cat.to_string(), format!("zcat {}", data_file.display()));
        assert_eq!(
            parse.to_string(),
            [
                "parse_collection -o fwd -f wapo --stemmer porter2",
                "--content-parser html --batch-size 1000"
            ]
            .join(" ")
        );
        Ok(())
    }
}
