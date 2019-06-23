//! Anything that has to do with building either forward or inverted index,
//! including parsing and compressing.

extern crate boolinator;
extern crate failure;
extern crate log;

use crate::command::ExtCommand;
use crate::config::*;
use crate::error::Error;
use crate::executor::*;
use crate::*;
use boolinator::Boolinator;
use failure::ResultExt;
use log::{info, warn};
use std::{fs::File, io::BufRead, io::BufReader};

/// Retrieves the term count of an already built collection.
///
/// Internally, it counts lines of the terms file of the forward index.
/// If it's not yet built, this function will return an error.
fn term_count(collection: &Collection) -> Result<usize, Error> {
    let output = ExtCommand::new("wc")
        .args(&["-l", &format!("{}.terms", collection.fwd()?)])
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

fn merge_parsed_batches(executor: &dyn PisaExecutor, collection: &Collection) -> Result<(), Error> {
    let batch_pattern = format!("{}.batch.*documents", collection.fwd()?);
    let batch_doc_files = resolve_files(&batch_pattern)?;
    let batch_count = batch_doc_files.len();
    let document_count = batch_doc_files
        .iter()
        .map(|f| Ok(BufReader::new(File::open(f)?).lines().count()))
        .fold(
            Ok(0_usize),
            |acc: Result<usize, Error>, count: Result<usize, Error>| Ok(acc? + count?),
        )?;
    ExtCommand::from(executor.command("parse_collection"))
        .args(&["--output", collection.fwd()?])
        .arg("merge")
        .args(&["--batch-count", &batch_count.to_string()])
        .args(&["--document-count", &document_count.to_string()])
        .status()?
        .success()
        .ok_or("Failed to merge collection batches")?;
    Ok(())
}

/// Builds a requeested collection, using a given executor.
pub fn collection(
    executor: &dyn PisaExecutor,
    collection: &Collection,
    config: &Config,
) -> Result<Vec<Stage>, Error> {
    let mut stages_run: Vec<Stage> = Vec::new();
    info!(
        "Processing collection: {}/{}",
        collection.name, collection.kind
    );
    let name = &collection.name;
    if config.is_suppressed(Stage::BuildIndex) {
        warn!("[{}] [build] Suppressed", name);
    } else {
        stages_run.push(Stage::BuildIndex);
        info!("[{}] [build] Building index", name);
        ensure_parent_exists(&collection.forward_index)?;
        ensure_parent_exists(&collection.inverted_index)?;
        if config.is_suppressed(Stage::ParseCollection) {
            warn!("[{}] [build] [parse] Suppressed", name);
        } else {
            if config.is_suppressed(Stage::ParseBatches) {
                warn!("[{}] [build] [parse] Only merging", name);
                merge_parsed_batches(executor, &collection)?;
            } else {
                stages_run.push(Stage::ParseCollection);
                info!("[{}] [build] [parse] Parsing collection", name);
                let parse = collection.kind.parse_command(&*executor, &collection)?;
                execute!(parse; "Failed to parse");
            }
            let fwd = collection.forward_index.display();
            executor.build_lexicon(format!("{}.terms", fwd), format!("{}.termmap", fwd))?;
            executor.build_lexicon(format!("{}.documents", fwd), format!("{}.docmap", fwd))?;
        }
        if config.is_suppressed(Stage::Invert) {
            warn!("[{}] [build] [invert] Suppressed", name);
        } else {
            stages_run.push(Stage::Invert);
            info!("[{}] [build] [invert] Inverting index", name);
            executor.invert(
                &collection.forward_index,
                &collection.inverted_index,
                term_count(&collection)?,
            )?;
        }
        info!("[{}] [build] [compress] Compressing index", name);
        for encoding in &collection.encodings {
            executor.compress(&collection.inverted_index, encoding)?;
        }
        executor.create_wand_data(&collection.inverted_index)?;
    }
    Ok(stages_run)
}

#[cfg(test)]
mod tests;
