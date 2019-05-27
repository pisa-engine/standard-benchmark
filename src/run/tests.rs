extern crate tempdir;

use super::evaluate;
use crate::config::Encoding;
use crate::tests::{mock_set_up, MockSetup};
use tempdir::TempDir;

#[test]
#[cfg_attr(target_family, unix)]
fn test_collection() {
    let tmp = TempDir::new("build").unwrap();
    let MockSetup {
        config,
        executor,
        programs,
        outputs,
        term_count: _,
    } = mock_set_up(&tmp);
    evaluate(
        executor.as_ref(),
        config.runs.first().unwrap(),
        &Encoding::from("block_simdbp"),
    )
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(outputs.get("evaluate_queries").unwrap()).unwrap(),
        format!(
            "{0} -t block_simdbp -i {2}.block_simdbp -w {2}.wand -a wand \
            -q topics.title --terms {1}.termmap --documents {1}.docmap \
            --stemmer porter2",
            programs.get("evaluate_queries").unwrap().display(),
            tmp.path().join("fwd").display(),
            tmp.path().join("inv").display(),
        )
    );
}
