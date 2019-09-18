//! All things related to experimental runs, including efficiency and precision runs.

use crate::{
    config::{Collection, Run, RunKind, Topics},
    error::Error,
    executor::Executor,
    CommandDebug,
};
use failure::ResultExt;
use itertools::iproduct;
use std::{fs, process::Command};

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

/// Runs query evaluation for on a given executor, for a given run.
///
/// Fails if the run is not of type `Evaluate`.
pub fn evaluate(
    executor: &Executor,
    run: &Run,
    collection: &Collection,
    use_scorer: bool,
) -> Result<Vec<String>, Error> {
    // TODO: loop
    let topics = &run.topics[0];
    let queries = queries_path(topics, executor)?;
    iproduct!(&run.algorithms, &run.encodings)
        .map(|(algorithm, encoding)| {
            executor.evaluate_queries(&collection, encoding, algorithm, &queries, use_scorer)
        })
        .collect()
}

/// Runs query benchmark for on a given executor, for a given run.
///
/// Fails if the run is not of type `Benchmark`.
pub fn benchmark(
    executor: &Executor,
    run: &Run,
    collection: &Collection,
    use_scorer: bool,
) -> Result<String, Error> {
    // TODO: loop
    let topics = &run.topics[0];
    let queries = queries_path(topics, executor)?;
    let results = iproduct!(&run.algorithms, &run.encodings)
        .map(|(algorithm, encoding)| {
            executor.benchmark(&collection, encoding, algorithm, &queries, use_scorer)
        })
        .collect::<Result<Vec<String>, Error>>();
    Ok(results?.iter().fold(String::new(), |mut acc, x| {
        acc.push_str(&x);
        acc
    }))
}

/// Process a run (e.g., single precision evaluation or benchmark).
pub fn process_run(
    executor: &Executor,
    run: &Run,
    collection: &Collection,
    use_scorer: bool,
) -> Result<(), Error> {
    match &run.kind {
        RunKind::Evaluate { qrels } => {
            for (output, algorithm) in evaluate(executor, run, collection, use_scorer)?
                .iter()
                .zip(&run.algorithms)
            {
                let base_path = &run.output.display();
                let results_output = format!("{}.{}.results", base_path, algorithm);
                let trec_eval_output = format!("{}.{}.trec_eval", base_path, algorithm);
                std::fs::write(&results_output, &output)?;
                let output = Command::new("trec_eval")
                    .arg("-q")
                    .arg("-a")
                    .arg(qrels.to_str().unwrap())
                    .arg(results_output)
                    .log()
                    .output()?;
                let eval_result = String::from_utf8(output.stdout)
                    .context("unable to parse result of trec_eval")?;
                fs::write(trec_eval_output, eval_result)?;
            }
            Ok(())
        }
        RunKind::Benchmark => {
            let output = benchmark(executor, run, collection, use_scorer)?;
            fs::write(&run.output, output)?;
            Ok(())
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
        evaluate(&executor, &config.run(0), &config.collection(0), true).unwrap();
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
