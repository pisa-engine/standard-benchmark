//! All things related to experimental runs, including efficiency and precision runs.

use crate::{
    config::{Collection, Run, RunKind, Topics},
    error::Error,
    executor::Executor,
    CommandDebug, Scorer,
};
use failure::ResultExt;
use itertools::iproduct;
use std::{fs, path::PathBuf, process::Command};

#[cfg_attr(tarpaulin, skip)]
fn queries_path(topics: &Topics, executor: &Executor) -> Result<String, Error> {
    match topics {
        Topics::Trec { path, field } => {
            executor.extract_topics(&path, &path)?;
            Ok(format!("{}.{}", &path.display(), field))
        }
        Topics::Simple { path } => Ok(path.to_str().unwrap().to_string()),
    }
}

/// Runs query benchmark for on a given executor, for a given run.
///
/// Fails if the run is not of type `Benchmark`.
pub fn benchmark(
    executor: &Executor,
    run: &Run,
    collection: &Collection,
    scorer: Option<&Scorer>,
) -> Result<String, Error> {
    // TODO: loop
    let topics = &run.topics[0];
    let queries = queries_path(topics, executor)?;
    let results = iproduct!(&run.algorithms, &run.encodings)
        .map(|(algorithm, encoding)| {
            executor.benchmark(&collection, encoding, algorithm, &queries, scorer)
        })
        .collect::<Result<Vec<String>, Error>>();
    Ok(results?.iter().fold(String::new(), |mut acc, x| {
        acc.push_str(&x);
        acc
    }))
}

/// The result of checking against a gold standard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunStatus {
    /// Everything OK.
    Success,
    /// Failed due to missing collection in the configuration.
    CollectionUndefined(String),
    /// Regression with respect to the gold standard was detected.
    Regression(Vec<Diff>),
}

/// Two paths to files that are supposed to be equal but are not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diff(pub PathBuf, pub PathBuf);

/// Process a run (e.g., single precision evaluation or benchmark).
pub fn process_run(
    executor: &Executor,
    run: &Run,
    collection: &Collection,
    use_scorer: bool,
) -> Result<RunStatus, Error> {
    let scorer = if use_scorer { Some(&run.scorer) } else { None };
    let queries: Result<Vec<_>, Error> = run
        .topics
        .iter()
        .map(|t| queries_path(t, executor))
        .collect();
    let base_path = &run.output.display();
    match &run.kind {
        RunKind::Evaluate { qrels } => {
            let mut diffs: Vec<Diff> = Vec::new();
            for (algorithm, encoding, (tid, queries)) in
                iproduct!(&run.algorithms, &run.encodings, queries?.iter().enumerate())
            {
                let results =
                    executor.evaluate_queries(&collection, encoding, algorithm, queries, scorer)?;
                let results_path = format!("{}.{}.{}.results", base_path, algorithm, tid);
                let trec_eval_path = format!("{}.{}.{}.trec_eval", base_path, algorithm, tid);
                fs::write(&results_path, &results)?;
                let output = Command::new("trec_eval")
                    .arg("-q")
                    .arg("-a")
                    .arg(qrels.to_str().unwrap())
                    .arg(results_path)
                    .log()
                    .output()?;
                let eval_result = String::from_utf8(output.stdout)
                    .context("unable to parse result of trec_eval")?;
                fs::write(&trec_eval_path, &eval_result)?;
                if let Some(compare_with) = &run.compare_with {
                    let compare_path =
                        format!("{}.{}.{}.trec_eval", compare_with.display(), algorithm, tid);
                    if fs::read_to_string(&compare_path)? != eval_result {
                        diffs.push(Diff(
                            PathBuf::from(compare_path),
                            PathBuf::from(trec_eval_path),
                        ));
                    }
                }
            }
            if diffs.is_empty() {
                Ok(RunStatus::Success)
            } else {
                Ok(RunStatus::Regression(diffs))
            }
        }
        RunKind::Benchmark => {
            // let mut diffs: Vec<Diff> = Vec::new();
            for (algorithm, encoding, (tid, queries)) in
                iproduct!(&run.algorithms, &run.encodings, queries?.iter().enumerate())
            {
                let results =
                    executor.benchmark(&collection, encoding, algorithm, &queries, scorer)?;
                let path = format!("{}.{}.{}.results", base_path, algorithm, tid);
                fs::write(&path, results)?;
            }
            // TODO: Regression
            Ok(RunStatus::Success)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{mock_program, mock_set_up, EchoMode, EchoOutput, MockSetup};
    use crate::Config;
    use crate::Error;
    use tempdir::TempDir;

    #[test]
    #[cfg_attr(target_family, unix)]
    fn test_evaluate() {
        let tmp = TempDir::new("build").unwrap();
        let MockSetup {
            config,
            executor,
            programs,
            outputs,
            ..
        } = mock_set_up(&tmp);
        process_run(&executor, &config.run(0), &config.collection(0), true).unwrap();
        assert_eq!(
            std::fs::read_to_string(outputs.get("evaluate_queries").unwrap()).unwrap(),
            format!(
                "{0} -t block_simdbp -i {2}.block_simdbp -w {2}.wand -a wand \
                 -q {3} --terms {1}.termmap --documents {1}.docmap \
                 --stemmer porter2 -k 1000 --scorer bm25\n\
                 {0} -t block_qmx -i {2}.block_qmx -w {2}.wand -a wand \
                 -q {3} --terms {1}.termmap --documents {1}.docmap \
                 --stemmer porter2 -k 1000 --scorer bm25\n\
                 {0} -t block_simdbp -i {2}.block_simdbp -w {2}.wand -a maxscore \
                 -q {3} --terms {1}.termmap --documents {1}.docmap \
                 --stemmer porter2 -k 1000 --scorer bm25\n\
                 {0} -t block_qmx -i {2}.block_qmx -w {2}.wand -a maxscore \
                 -q {3} --terms {1}.termmap --documents {1}.docmap \
                 --stemmer porter2 -k 1000 --scorer bm25\n",
                programs.get("evaluate_queries").unwrap().display(),
                tmp.path().join("fwd").display(),
                tmp.path().join("inv").display(),
                tmp.path().join("topics.title").display(),
            )
        );
    }

    #[test]
    #[cfg_attr(target_family, unix)]
    fn test_evaluate_simple_topics() {
        let tmp = TempDir::new("build").unwrap();
        let mut mock_setup = mock_set_up(&tmp);
        mock_program(
            &tmp.path().join("bin"),
            &mut mock_setup,
            "trec_eval",
            EchoMode::Stdout,
        );
        let MockSetup {
            config,
            executor,
            programs,
            outputs,
            ..
        } = mock_setup;
        process_run(&executor, &config.run(1), &config.collection(0), true).unwrap();
        assert_eq!(
            std::fs::read_to_string(outputs.get("evaluate_queries").unwrap()).unwrap(),
            format!(
                "{0} -t block_simdbp -i {2}.block_simdbp -w {2}.wand -a wand \
                 -q {3} --terms {1}.termmap --documents {1}.docmap \
                 --stemmer porter2 -k 1000 --scorer bm25\n\
                 {0} -t block_simdbp -i {2}.block_simdbp -w {2}.wand -a maxscore \
                 -q {3} --terms {1}.termmap --documents {1}.docmap \
                 --stemmer porter2 -k 1000 --scorer bm25\n",
                programs.get("evaluate_queries").unwrap().display(),
                tmp.path().join("fwd").display(),
                tmp.path().join("inv").display(),
                tmp.path().join("topics").display(),
            )
        );
        // TODO: Revisit when #5 addressed
        // let trec_eval = programs.get("trec_eval").unwrap().to_str().unwrap();
        // let qrels = tmp
        //     .path()
        //     .join("qrels")
        //     .into_os_string()
        //     .into_string()
        //     .unwrap();
        // let run = config.run(1).output.to_str().unwrap().to_string();
        // assert_eq!(
        //     EchoOutput::from(
        //         path::PathBuf::from(format!(
        //             "{}.wand.trec_eval",
        //             config.runs[1].output.display()
        //         ))
        //         .as_path()
        //     ),
        //     EchoOutput::from(format!(
        //         "{} -q -a {} {}.wand.results",
        //         &trec_eval, &qrels, &run
        //     )),
        // );
        // assert_eq!(
        //     EchoOutput::from(
        //         path::PathBuf::from(format!(
        //             "{}.maxscore.trec_eval",
        //             config.runs[1].output.display()
        //         ))
        //         .as_path()
        //     ),
        //     EchoOutput::from(format!(
        //         "{} -q -a {} {}.maxscore.results",
        //         &trec_eval, &qrels, &run
        //     )),
        // );
    }

    #[test]
    #[cfg_attr(target_family, unix)]
    fn test_benchmark() -> Result<(), Error> {
        let tmp = TempDir::new("run").unwrap();
        let MockSetup {
            config,
            executor,
            programs,
            outputs,
            ..
        } = mock_set_up(&tmp);
        process_run(&executor, &config.run(2), &config.collection(0), true)?;
        let actual = EchoOutput::from(outputs.get("queries").unwrap().as_path());
        let expected = EchoOutput::from(format!(
            "{0} -t block_simdbp -i {2}.block_simdbp -w {2}.wand -a wand \
             -q {3} --terms {1}.termmap --stemmer porter2 -k 1000 \
             --scorer bm25\n\
             {0} -t block_simdbp -i {2}.block_simdbp -w {2}.wand -a maxscore \
             -q {3} --terms {1}.termmap --stemmer porter2 -k 1000 \
             --scorer bm25",
            programs.get("queries").unwrap().display(),
            tmp.path().join("fwd").display(),
            tmp.path().join("inv").display(),
            tmp.path().join("topics.title").display(),
        ));
        assert_eq!(actual, expected);
        Ok(())
    }
}
