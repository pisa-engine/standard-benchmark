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
use failure::{format_err, ResultExt};
use glob::glob;
use log::{info, warn};

fn parse_wapo_command(
    executor: &dyn PisaExecutor,
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
    executor: &dyn PisaExecutor,
    collection: &Collection,
) -> Result<ProcessPipeline, Error> {
    match collection.name.as_ref() {
        "wapo" => parse_wapo_command(executor, collection),
        _ => unimplemented!(""),
    }
}

/// Retrieves the term count of an already built collection.
///
/// Internally, it counts lines of the terms file of the forward index.
/// If it's not yet built, this function will return an error.
fn term_count(collection: &Collection) -> Result<usize, Error> {
    let output = Process::new("wc", &["-l", &format!("{}.terms", collection.fwd()?)])
        .command()
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
    info!("Processing collection: {}", collection.name);
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
            stages_run.push(Stage::ParseCollection);
            info!("[{}] [build] [parse] Parsing collection", name);
            let pipeline = parse_command(&*executor, &collection)?;
            debug!("\n{}", pipeline.display(Verbosity::Verbose));
            execute!(pipeline.pipe(); "Failed to parse");
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
