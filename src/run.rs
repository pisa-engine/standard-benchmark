//! All things related to experimental runs, including efficiency and precision runs.

use crate::{
    config::{format_output_path, output_path_formatter, Collection, Run, RunKind, Topics},
    error::Error,
    executor::Executor,
    Algorithm, CommandDebug, Encoding, RegressionMargin,
};
use cranky::ResultRecord;
use failure::ResultExt;
use itertools::iproduct;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::{fmt, fs, process::Command};

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

/// The result of checking against a gold standard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunStatus {
    /// Everything OK.
    Success,
    /// Regression with respect to the gold standard was detected.
    /// It holds the count of regressions for this run.
    Regression(usize),
}

/// Benchmark results as obtained from `queries` in JSON format.
#[derive(Serialize, Deserialize, Debug)]
struct BenchmarkResults {
    #[serde(rename = "type")]
    kind: Encoding,
    #[serde(rename = "query")]
    algorithm: Algorithm,
    #[serde(rename = "avg")]
    avg_time: f32,
    #[serde(rename = "q50")]
    quantile_50: f32,
    #[serde(rename = "q90")]
    quantile_90: f32,
    #[serde(rename = "q95")]
    quantile_95: f32,
}

#[derive(Serialize, Deserialize)]
struct PerformanceRegression {
    #[serde(rename = "avg")]
    avg_time: Option<(f32, f32)>,
    #[serde(rename = "q50")]
    quantile_50: Option<(f32, f32)>,
    #[serde(rename = "q90")]
    quantile_90: Option<(f32, f32)>,
    #[serde(rename = "q95")]
    quantile_95: Option<(f32, f32)>,
}

impl fmt::Display for PerformanceRegression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (prop, regression) in std::iter::once(("avg", self.avg_time))
            .chain(std::iter::once(("q50", self.quantile_50)))
            .chain(std::iter::once(("q90", self.quantile_90)))
            .chain(std::iter::once(("q95", self.quantile_95)))
        {
            if let Some((time, baseline)) = regression {
                writeln!(f, "{}: {} --> {}", prop, baseline, time)?;
            }
        }
        write!(f, "")
    }
}

impl BenchmarkResults {
    fn calc_diff(value: f32, gold: f32, margin: RegressionMargin) -> Option<(f32, f32)> {
        if value - gold * (1.0 + margin.0) > 0.0 {
            Some((value, gold))
        } else {
            None
        }
    }
    fn regression(
        &self,
        gold: &Self,
        margin: RegressionMargin,
    ) -> Result<Option<PerformanceRegression>, Error> {
        if self.kind != gold.kind {
            return Err(Error::from("Encodings do not match"));
        }
        if self.algorithm != gold.algorithm {
            return Err(Error::from("Algorithms do not match"));
        }
        let avg = Self::calc_diff(self.avg_time, gold.avg_time, margin);
        let q50 = Self::calc_diff(self.quantile_50, gold.quantile_50, margin);
        let q90 = Self::calc_diff(self.quantile_90, gold.quantile_90, margin);
        let q95 = Self::calc_diff(self.quantile_95, gold.quantile_95, margin);
        Ok(match (avg, q50, q90, q95) {
            (None, None, None, None) => None,
            (avg_time, quantile_50, quantile_90, quantile_95) => Some(PerformanceRegression {
                avg_time,
                quantile_50,
                quantile_90,
                quantile_95,
            }),
        })
    }
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
) -> Result<(), Error> {
    let scorer = if use_scorer { Some(&run.scorer) } else { None };
    let queries: Result<Vec<_>, Error> = run
        .topics
        .iter()
        .map(|t| queries_path(t, executor))
        .collect();
    match &run.kind {
        RunKind::Evaluate { qrels } => {
            for (algorithm, encoding, (tid, queries)) in
                iproduct!(&run.algorithms, &run.encodings, queries?.iter().enumerate())
            {
                let results =
                    executor.evaluate_queries(&collection, encoding, algorithm, queries, scorer)?;
                let results_path =
                    format_output_path(&run.output, algorithm, encoding, tid, "results");
                let trec_eval_path =
                    format_output_path(&run.output, algorithm, encoding, tid, "trec_eval");
                let mut results: Vec<ResultRecord> =
                    cranky::read_records(std::io::Cursor::new(results))?;
                results.sort_by(|lhs, rhs| {
                    (&lhs.run, &lhs.iter, &lhs.qid, &-lhs.score.0, &lhs.docid)
                        .partial_cmp(&(&rhs.run, &rhs.iter, &rhs.qid, &-rhs.score.0, &rhs.docid))
                        .unwrap()
                });
                let results: String = results
                    .into_iter()
                    .map(|r| r.to_string())
                    .collect::<Vec<_>>()
                    .join("\n");
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
            }
        }
        RunKind::Benchmark => {
            for (algorithm, encoding, (tid, queries)) in
                iproduct!(&run.algorithms, &run.encodings, queries?.iter().enumerate())
            {
                let results =
                    executor.benchmark(&collection, encoding, algorithm, &queries, scorer)?;
                let path = format_output_path(&run.output, algorithm, encoding, tid, "bench");
                fs::write(&path, &results)?;
            }
        }
    }
    Ok(())
}

fn load_benchmark_results(path: &Path) -> Result<BenchmarkResults, Error> {
    let results: BenchmarkResults = serde_json::from_reader(
        fs::File::open(path).with_context(|_| path.to_string_lossy().to_string())?,
    )
    .context("Unable to parse benchmark results")?;
    Ok(results)
}

fn load_eval_results(path: &Path) -> Result<String, Error> {
    Ok(fs::read_to_string(path).with_context(|_| path.to_string_lossy().to_string())?)
}

/// Compares the results of the runs with a given baseline.
pub fn compare_with_baseline(
    executor: &Executor,
    run: &Run,
    compare_with: &Path,
    margin: RegressionMargin,
) -> Result<RunStatus, Error> {
    let queries: Result<Vec<_>, Error> = run
        .topics
        .iter()
        .map(|t| queries_path(t, executor))
        .collect();
    match &run.kind {
        RunKind::Evaluate { .. } => {
            let mut regression_count = 0;
            for (algorithm, encoding, tid) in
                iproduct!(&run.algorithms, &run.encodings, 0..queries?.len())
            {
                let format_path = output_path_formatter(algorithm, encoding, tid, "trec_eval");
                let result_path = format_path(&run.output);
                let base_result_path = format_path(compare_with);
                let results = load_eval_results(&result_path)?;
                let baseline = load_eval_results(&base_result_path)?;
                if results != baseline {
                    eprintln!("Detected correctness regression!");
                    eprintln!("file: {}", result_path.display());
                    eprintln!("base: {}", base_result_path.display());
                    regression_count += 1;
                }
            }
            if regression_count > 0 {
                return Ok(RunStatus::Regression(regression_count));
            }
        }
        RunKind::Benchmark => {
            let mut regression_count = 0;
            for (algorithm, encoding, tid) in
                iproduct!(&run.algorithms, &run.encodings, 0..queries?.len())
            {
                let format_path = output_path_formatter(algorithm, encoding, tid, "bench");
                let result_path = format_path(&run.output);
                let base_result_path = format_path(compare_with);
                let results = load_benchmark_results(&result_path)?;
                let baseline = load_benchmark_results(&base_result_path)?;
                if let Some(regression) = results.regression(&baseline, margin)? {
                    eprintln!("Detected performance regression!");
                    eprintln!("file: {}", result_path.display());
                    eprintln!("base: {}", base_result_path.display());
                    eprintln!("{}", regression);
                    regression_count += 1;
                }
            }
            if regression_count > 0 {
                return Ok(RunStatus::Regression(regression_count));
            }
        }
    }
    Ok(RunStatus::Success)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{mock_program, mock_set_up, EchoMode, EchoOutput, MockSetup};
    use crate::Config;
    use crate::Error;
    use std::path;
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
                 -q {3} --terms {1}.termlex --documents {1}.doclex \
                 --stemmer porter2 -k 1000 --scorer bm25\n\
                 {0} -t block_qmx -i {2}.block_qmx -w {2}.wand -a wand \
                 -q {3} --terms {1}.termlex --documents {1}.doclex \
                 --stemmer porter2 -k 1000 --scorer bm25\n\
                 {0} -t block_simdbp -i {2}.block_simdbp -w {2}.wand -a maxscore \
                 -q {3} --terms {1}.termlex --documents {1}.doclex \
                 --stemmer porter2 -k 1000 --scorer bm25\n\
                 {0} -t block_qmx -i {2}.block_qmx -w {2}.wand -a maxscore \
                 -q {3} --terms {1}.termlex --documents {1}.doclex \
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
                 -q {3} --terms {1}.termlex --documents {1}.doclex \
                 --stemmer porter2 -k 1000 --scorer bm25\n\
                 {0} -t block_simdbp -i {2}.block_simdbp -w {2}.wand -a maxscore \
                 -q {3} --terms {1}.termlex --documents {1}.doclex \
                 --stemmer porter2 -k 1000 --scorer bm25\n",
                programs.get("evaluate_queries").unwrap().display(),
                tmp.path().join("fwd").display(),
                tmp.path().join("inv").display(),
                tmp.path().join("topics").display(),
            )
        );
        let trec_eval = programs.get("trec_eval").unwrap().to_str().unwrap();
        let qrels = tmp
            .path()
            .join("qrels")
            .into_os_string()
            .into_string()
            .unwrap();
        let run = config.run(1).output.to_str().unwrap().to_string();
        assert_eq!(
            EchoOutput::from(
                path::PathBuf::from(format!(
                    "{}.wand.block_simdbp.0.trec_eval",
                    config.run(1).output.display()
                ))
                .as_path()
            ),
            EchoOutput::from(format!(
                "{} -q -a {} {}.wand.block_simdbp.0.results",
                &trec_eval, &qrels, &run
            )),
        );
        assert_eq!(
            EchoOutput::from(
                path::PathBuf::from(format!(
                    "{}.maxscore.block_simdbp.0.trec_eval",
                    config.run(1).output.display()
                ))
                .as_path()
            ),
            EchoOutput::from(format!(
                "{} -q -a {} {}.maxscore.block_simdbp.0.results",
                &trec_eval, &qrels, &run
            )),
        );
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
             -q {3} --terms {1}.termlex --stemmer porter2 -k 1000 \
             --scorer bm25\n\
             {0} -t block_simdbp -i {2}.block_simdbp -w {2}.wand -a maxscore \
             -q {3} --terms {1}.termlex --stemmer porter2 -k 1000 \
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
