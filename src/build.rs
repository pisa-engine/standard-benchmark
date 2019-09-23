//! Anything that has to do with building either forward or inverted index,
//! including parsing and compressing.

extern crate boolinator;
extern crate failure;
extern crate log;

use crate::config::{resolve_files, Collection, CollectionKind, Stage};
use crate::error::Error;
use crate::executor::Executor;
use crate::{ensure_parent_exists, CommandDebug, Config, Resolved};
use boolinator::Boolinator;
use failure::ResultExt;
use log::{info, warn};
use os_pipe::pipe;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
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

fn parse_collection_cmd(executor: &Executor, fwd_index: &Path, format: &str) -> Command {
    let mut cmd = executor.command("parse_collection");
    cmd.arg("-o")
        .arg(fwd_index)
        .args(&["-f", format])
        .args(&["--stemmer", "porter2"])
        .args(&["--content-parser", "html"])
        .args(&["--batch-size", "1000"]);
    cmd
}

fn parsing_commands(
    executor: &Executor,
    collection: &Collection,
) -> Result<(Command, Command), Error> {
    match &collection.kind {
        CollectionKind::NewYorkTimes => {
            let input_files = resolve_files(collection.input_dir.join("*.plain"))?;
            let mut cat = Command::new("cat");
            cat.args(&input_files);
            let parse = parse_collection_cmd(&executor, &collection.fwd_index, "plaintext");
            Ok((cat, parse))
        }
        CollectionKind::Robust => {
            let find_output = Command::new("find")
                .arg(&collection.input_dir)
                .args(&["-type", "f"])
                .args(&["-name", "*.*z"])
                .arg("(")
                .args(&["-path", "*/disk4/fr94/[0-9]*/*"])
                .args(&["-o", "-path", "*/disk4/ft/ft*"])
                .args(&["-o", "-path", "*/disk5/fbis/fb*"])
                .args(&["-o", "-path", "*/disk5/latimes/la*"])
                .arg(")")
                .log()
                .output()?;
            let find_output = String::from_utf8_lossy(&find_output.stdout);
            let input_files: Vec<_> = find_output.split('\n').collect();
            let mut cat = Command::new("zcat");
            cat.args(&input_files);
            let parse = parse_collection_cmd(&executor, &collection.fwd_index, "trectext");
            Ok((cat, parse))
        }
        CollectionKind::Warc => {
            let input_files = resolve_files(collection.input_dir.join("*/*.gz"))?;
            let mut cat = Command::new("zcat");
            cat.args(&input_files);
            let parse = parse_collection_cmd(&executor, &collection.fwd_index, "warc");
            Ok((cat, parse))
        }
        CollectionKind::TrecWeb => {
            let input_files = resolve_files(collection.input_dir.join("*/*.gz"))?;
            let mut cat = Command::new("zcat");
            cat.args(&input_files);
            let parse = parse_collection_cmd(&executor, &collection.fwd_index, "trecweb");
            Ok((cat, parse))
        }
        CollectionKind::WashingtonPost => {
            let input_files = resolve_files(collection.input_dir.join("data/*.jl"))?;
            let mut cat = Command::new("cat");
            cat.args(&input_files);
            let parse = parse_collection_cmd(&executor, &collection.fwd_index, "wapo");
            Ok((cat, parse))
        }
    }
}

/// Builds a requeested collection, using a given executor.
pub fn collection<C: Config + Resolved>(
    executor: &Executor,
    collection: &Collection,
    config: &C,
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
                drop(cat);
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
            for scorer in &collection.scorers {
                info!(
                    "[{}] [build] [wand] Creating WAND data for {}",
                    name, &scorer
                );
                executor.create_wand_data(
                    &collection.inv_index,
                    if config.use_scorer() {
                        Some(&scorer)
                    } else {
                        None
                    },
                )?;
            }
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
    use std::collections::HashSet;
    use std::fs;
    use std::path::PathBuf;
    use tempdir::TempDir;

    #[test]
    #[cfg_attr(target_family, unix)]
    fn test_term_count() {
        {
            let tmp = TempDir::new("build").unwrap();
            let setup = mock_set_up(&tmp);
            assert_eq!(term_count(&setup.config.collection(0)), Ok(3));
        }
        {
            let tmp = TempDir::new("build").unwrap();
            let setup = mock_set_up(&tmp);
            std::fs::remove_file(tmp.path().join("fwd.terms")).unwrap();
            assert_eq!(
                term_count(&setup.config.collection(0)).err(),
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
        let coll = &config.collection(0);
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
        collection(&executor, &config.collection(0), &config).unwrap();
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

    #[test]
    fn test_suppressed_build() {
        let tmp = TempDir::new("build").unwrap();
        let MockSetup {
            mut config,
            executor,
            outputs,
            ..
        } = mock_set_up(&tmp);
        config.disable(Stage::BuildIndex);
        collection(&executor, &config.collection(0), &config).unwrap();
        assert!(!outputs.get("parse_collection").unwrap().exists());
        assert!(!outputs.get("invert").unwrap().exists());
        assert!(!outputs.get("create_freq_index").unwrap().exists());
        assert!(!outputs.get("create_wand_data").unwrap().exists());
        assert!(!outputs.get("lexicon").unwrap().exists());
    }

    #[test]
    fn test_suppressed_parse_and_invert() {
        let tmp = TempDir::new("build").unwrap();
        let MockSetup {
            mut config,
            executor,
            outputs,
            ..
        } = mock_set_up(&tmp);
        config.disable(Stage::Parse);
        config.disable(Stage::Invert);
        collection(&executor, &config.collection(0), &config).unwrap();
        assert!(!outputs.get("parse_collection").unwrap().exists());
        assert!(!outputs.get("parse_collection").unwrap().exists());
        assert!(!outputs.get("invert").unwrap().exists());
        assert!(outputs.get("create_freq_index").unwrap().exists());
        assert!(outputs.get("create_wand_data").unwrap().exists());
        assert!(!outputs.get("lexicon").unwrap().exists());
    }

    #[test]
    fn test_suppressed_parse_batches() {
        let tmp = TempDir::new("build").unwrap();
        let MockSetup {
            mut config,
            executor,
            outputs,
            ..
        } = mock_set_up(&tmp);
        std::fs::File::create(format!(
            "{}.batch.0.documents",
            &config.collection(0).fwd_index.display()
        ))
        .unwrap();
        config.disable(Stage::ParseBatches);
        collection(&executor, &config.collection(0), &config).unwrap();
        let parse_out = std::fs::read_to_string(outputs.get("parse_collection").unwrap()).unwrap();
        assert!(parse_out.find("merge").is_some());
        assert!(outputs.get("invert").unwrap().exists());
        assert!(outputs.get("create_freq_index").unwrap().exists());
        assert!(outputs.get("create_wand_data").unwrap().exists());
        assert!(outputs.get("lexicon").unwrap().exists());
    }

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
            scorers: crate::config::default_scorers(),
        };
        let (cat, parse) = parsing_commands(&executor, &cconf)?;
        assert_eq!(cat.to_string(), format!("cat {}", data_file.display()));
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

    fn mkfiles(root: &Path, paths: &[&str]) -> Result<(), Error> {
        for path in paths {
            if path.ends_with('/') {
                fs::create_dir(root.join(path))?;
            } else {
                File::create(root.join(path))?;
            }
        }
        Ok(())
    }

    #[test]
    fn test_make_files() {
        let tmp = TempDir::new("tmp").unwrap();
        mkfiles(
            tmp.path(),
            &[
                "file1",
                "file2",
                "subdir/",
                "subdir/file3",
                "subdir/file4",
                "subdir/subdir/",
                "subdir/subdir/file5",
            ],
        )
        .unwrap();
        assert!(tmp.path().join("file1").exists());
        assert!(tmp.path().join("file2").exists());
        assert!(tmp.path().join("subdir").join("file3").exists());
        assert!(tmp.path().join("subdir").join("file4").exists());
        assert!(tmp
            .path()
            .join("subdir")
            .join("subdir")
            .join("file5")
            .exists());
    }

    #[test]
    fn test_parsing_command_robust() -> Result<(), Error> {
        let tmp = TempDir::new("tmp").unwrap();
        mkfiles(
            tmp.path(),
            &[
                "disk4/",
                "disk4/cr/",
                "disk4/cr/hfiles/",
                "disk4/cr/hfiles/cr93e1.z",
                "disk4/cr/hfiles/cr93e2.z",
                "disk4/dtds/",
                "disk4/dtds/credtd.z",
                "disk4/dtds/crhdtd.z",
                "disk4/dtds/fr94dtd.z",
                "disk4/dtds/ftdtd.z",
                "disk4/fr94/",
                "disk4/fr94/01/",
                "disk4/fr94/01/fr940104.0z",
                "disk4/fr94/01/fr940104.1z",
                "disk4/fr94/02/",
                "disk4/fr94/02/fr940202.0z",
                "disk4/fr94/02/fr940202.1z",
                "disk4/fr94/aux/",
                "disk4/fr94/aux/frcheck.c",
                "disk4/fr94/aux/frfoot.c",
                "disk4/fr94/readchg.z",
                "disk4/fr94/readmefr.z",
                "disk4/ft/",
                "disk4/ft/readmeft.z",
                "disk4/ft/readmeft.z",
                "disk4/ft/ft911/",
                "disk4/ft/ft911/ft911_1.z",
                "disk4/ft/ft911/ft911_2.z",
                "disk4/ft/ft921/",
                "disk4/ft/ft921/ft921_1.z",
                "disk4/ft/ft921/ft921_2.z",
                "disk5/",
                "disk5/dtds/",
                "disk5/dtds/credtd.z",
                "disk5/dtds/crhdtd.z",
                "disk5/dtds/fr94dtd.z",
                "disk5/dtds/ftdtd.z",
                "disk5/fbis/",
                "disk5/fbis/readchg.txt",
                "disk5/fbis/fb396001.z",
                "disk5/fbis/fb396002.z",
                "disk5/latimes/",
                "disk5/latimes/la123190.z",
                "disk5/latimes/readchg.txt",
                "disk5/latimes/readmela.txt",
            ],
        )
        .unwrap();

        let executor = Executor::default();
        let collection = Collection {
            name: "robust".to_string(),
            kind: CollectionKind::Robust,
            input_dir: tmp.path().to_path_buf(),
            fwd_index: PathBuf::from("fwd"),
            inv_index: PathBuf::from("inv"),
            encodings: vec![],
            scorers: crate::config::default_scorers(),
        };
        let (cat, parse) = parsing_commands(&executor, &collection)?;
        let actual_files: HashSet<String> = cat
            .to_string()
            .split(' ')
            .skip(1)
            .map(String::from)
            .collect();
        let expected_files: HashSet<_> = [
            "disk4/fr94/01/fr940104.0z",
            "disk4/fr94/01/fr940104.1z",
            "disk4/fr94/02/fr940202.0z",
            "disk4/fr94/02/fr940202.1z",
            "disk4/ft/ft911/ft911_1.z",
            "disk4/ft/ft911/ft911_2.z",
            "disk4/ft/ft921/ft921_1.z",
            "disk4/ft/ft921/ft921_2.z",
            "disk5/fbis/fb396001.z",
            "disk5/fbis/fb396002.z",
            "disk5/latimes/la123190.z",
        ]
        .iter()
        .map(|p| tmp.path().join(p).to_string_lossy().to_string())
        .collect();
        assert_eq!(actual_files, expected_files);
        assert_eq!(
            parse.to_string(),
            [
                "parse_collection -o fwd -f trectext --stemmer porter2",
                "--content-parser html --batch-size 1000"
            ]
            .join(" ")
        );
        Ok(())
    }

    #[test]
    fn test_parsing_command_nyt() -> Result<(), Error> {
        let tmp = TempDir::new("tmp").unwrap();
        mkfiles(tmp.path(), &["nyt.plain"]).unwrap();

        let executor = Executor::default();
        let collection = Collection {
            name: "robust".to_string(),
            kind: CollectionKind::NewYorkTimes,
            input_dir: tmp.path().to_path_buf(),
            fwd_index: PathBuf::from("fwd"),
            inv_index: PathBuf::from("inv"),
            encodings: vec![],
            scorers: crate::config::default_scorers(),
        };
        let (cat, parse) = parsing_commands(&executor, &collection)?;
        assert_eq!(
            cat.to_string(),
            format!("cat {}", tmp.path().join("nyt.plain").display())
        );
        assert_eq!(
            parse.to_string(),
            [
                "parse_collection -o fwd -f plaintext --stemmer porter2",
                "--content-parser html --batch-size 1000"
            ]
            .join(" ")
        );
        Ok(())
    }

    #[test]
    fn test_parsing_command_warc() -> Result<(), Error> {
        let tmp = TempDir::new("tmp").unwrap();
        mkfiles(
            tmp.path(),
            &[
                "00/",
                "00/00.gz",
                "00/01.gz",
                "00/xyz",
                "00/0.gz1",
                "01/",
                "01/00.gz",
                "xyz/",
                "xyz/00.gz",
                "00.gz",
                "xyz.txt",
            ],
        )
        .unwrap();

        let executor = Executor::default();
        let collection = Collection {
            name: "robust".to_string(),
            kind: CollectionKind::Warc,
            input_dir: tmp.path().to_path_buf(),
            fwd_index: PathBuf::from("fwd"),
            inv_index: PathBuf::from("inv"),
            encodings: vec![],
            scorers: crate::config::default_scorers(),
        };
        let (cat, parse) = parsing_commands(&executor, &collection)?;
        let actual_files: HashSet<String> = cat
            .to_string()
            .split(' ')
            .skip(1)
            .map(String::from)
            .collect();
        let expected_files: HashSet<_> = ["00/00.gz", "00/01.gz", "01/00.gz", "xyz/00.gz"]
            .iter()
            .map(|p| tmp.path().join(p).to_string_lossy().to_string())
            .collect();
        assert_eq!(actual_files, expected_files);
        assert_eq!(
            parse.to_string(),
            [
                "parse_collection -o fwd -f warc --stemmer porter2",
                "--content-parser html --batch-size 1000"
            ]
            .join(" ")
        );
        Ok(())
    }

    #[test]
    fn test_parsing_command_trecweb() -> Result<(), Error> {
        let tmp = TempDir::new("tmp").unwrap();
        mkfiles(
            tmp.path(),
            &[
                "00/",
                "00/00.gz",
                "00/01.gz",
                "00/xyz",
                "00/0.gz1",
                "01/",
                "01/00.gz",
                "xyz/",
                "xyz/00.gz",
                "00.gz",
                "xyz.txt",
            ],
        )
        .unwrap();

        let executor = Executor::default();
        let collection = Collection {
            name: "robust".to_string(),
            kind: CollectionKind::TrecWeb,
            input_dir: tmp.path().to_path_buf(),
            fwd_index: PathBuf::from("fwd"),
            inv_index: PathBuf::from("inv"),
            encodings: vec![],
            scorers: crate::config::default_scorers(),
        };
        let (cat, parse) = parsing_commands(&executor, &collection)?;
        let actual_files: HashSet<String> = cat
            .to_string()
            .split(' ')
            .skip(1)
            .map(String::from)
            .collect();
        let expected_files: HashSet<_> = ["00/00.gz", "00/01.gz", "01/00.gz", "xyz/00.gz"]
            .iter()
            .map(|p| tmp.path().join(p).to_string_lossy().to_string())
            .collect();
        assert_eq!(actual_files, expected_files);
        assert_eq!(
            parse.to_string(),
            [
                "parse_collection -o fwd -f trecweb --stemmer porter2",
                "--content-parser html --batch-size 1000"
            ]
            .join(" ")
        );
        Ok(())
    }
}
