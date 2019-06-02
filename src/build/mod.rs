//! Anything that has to do with building either forward or inverted index,
//! including parsing and compressing.

extern crate boolinator;
extern crate failure;
extern crate glob;
extern crate log;

use super::command::ExtCommand;
use super::config::*;
use super::error::Error;
use super::executor::*;
use super::*;
use boolinator::Boolinator;
use failure::ResultExt;
use log::{info, warn};

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

/// Builds a requeested collection, using a given executor.
///
/// **Note**: Some steps might be ignored if the `config` struct
/// has been instructed to suppress some stages.
/// ```
/// # extern crate stdbench;
/// # use stdbench::Stage;
/// let stage = Stage::BuildIndex; // suppresses the entire function
/// let stage = Stage::ParseCollection; // suppresses building forward index
/// let stage = Stage::Invert; // suppresses building inverted index
/// ```
pub fn collection(
    executor: &dyn PisaExecutor,
    collection: &Collection,
    config: &Config,
) -> Result<Vec<Stage>, Error> {
    let mut stages_run: Vec<Stage> = Vec::new();
    info!("Processing collection: {}", collection.kind);
    let name = &collection.kind.to_string();
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
            stages_run.push(Stage::ParseCollection);
            info!("[{}] [build] [parse] Parsing collection", name);
            let pipeline = collection.kind.parse_command(&*executor, &collection)?;
            execute!(pipeline; "Failed to parse");
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
