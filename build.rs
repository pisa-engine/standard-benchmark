use git2::{ObjectType, Repository, ResetType};
use std::{env, path::PathBuf};

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let url = "https://github.com/usnistgov/trec_eval";
    let tmp = tempdir::TempDir::new("stdbench-build").expect("Unable to create a temp dir");
    let repo_path = tmp.path().join("trec_eval");
    let repo = match Repository::clone(url, &repo_path) {
        Ok(repo) => repo,
        Err(e) => panic!("failed to clone: {}", e),
    };
    let commit = repo
        .resolve_reference_from_short_name("v8.1")
        .expect("Cound not find tag v8.1")
        .peel(ObjectType::Commit)
        .expect("v8.1 is not a commit");
    repo.reset(&commit, ResetType::Hard, None)
        .expect("Failed checking out v8.1");
    let mut cmd = cc::Build::new()
        .include(&repo_path)
        .no_default_flags(true)
        .define("VERSIONID", r#""8.1""#)
        .flag("-lm")
        .warnings(false)
        .opt_level(3)
        .get_compiler()
        .to_command();
    cmd.args(
        &[
            "trec_eval.c",
            "get_qrels.c",
            "get_top.c",
            "form_trvec.c",
            "measures.c",
            "print_meas.c",
            "trvec_teval.c",
            "buf_util.c",
            "error_msgs.c",
            "trec_eval_help.c",
        ]
        .iter()
        .map(|&f| repo_path.join(f))
        .collect::<Vec<_>>(),
    )
    .arg("-o")
    .arg(PathBuf::from(&out_dir).join("trec_eval"));
    assert!(cmd.status().unwrap().success());
    println!("cargo:libdir={}", &out_dir);
    //panic!("{}", out_dir);
}
