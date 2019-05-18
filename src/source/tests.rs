extern crate downcast_rs;
extern crate tempdir;

use super::*;
use tempdir::TempDir;
use yaml_rust::YamlLoader;

#[test]
fn test_parse_git_source() {
    assert_eq!(
        PisaSource::parse_git_source(
            &YamlLoader::load_from_str(
                r#"
                        type: git
                        branch: dev
                        url: https://github.com/pisa-engine/pisa.git
                    "#
            )
            .unwrap()[0]
        ),
        Ok(GitSource::new(
            "https://github.com/pisa-engine/pisa.git",
            "dev"
        ))
    );
    assert_eq!(
        PisaSource::parse_git_source(
            &YamlLoader::load_from_str(
                r#"
                        type: git
                        url: https://github.com/pisa-engine/pisa.git
                    "#
            )
            .unwrap()[0]
        ),
        Err("missing source.branch".into())
    );
    assert_eq!(
        PisaSource::parse_git_source(
            &YamlLoader::load_from_str(
                r#"
                        type: git
                    "#
            )
            .unwrap()[0]
        ),
        Err("missing source.url".into())
    );
}

#[test]
fn test_parse_docker_source() {
    assert_eq!(
        PisaSource::parse_docker_source(
            &YamlLoader::load_from_str(
                r#"
                        type: docker
                        tag: latest
                    "#
            )
            .unwrap()[0]
        )
        .unwrap(),
        DockerSource {
            tag: String::from("latest")
        }
    );
    assert_eq!(
        PisaSource::parse_docker_source(
            &YamlLoader::load_from_str(
                r#"
                        type: docker
                    "#
            )
            .unwrap()[0]
        ),
        Err("missing source.tag".into())
    );
}

#[test]
fn test_parse_source() {
    assert_eq!(
        PisaSource::parse(
            &YamlLoader::load_from_str(
                r#"
                        type: docker
                        tag: latest
                    "#
            )
            .unwrap()[0]
        )
        .unwrap()
        .downcast_ref::<DockerSource>(),
        Some(&DockerSource {
            tag: String::from("latest")
        })
    );
    assert_eq!(
        PisaSource::parse(
            &YamlLoader::load_from_str(
                r#"
                        type: git
                        branch: dev
                        url: https://github.com/pisa-engine/pisa.git
                    "#
            )
            .unwrap()[0]
        )
        .unwrap()
        .downcast_ref::<GitSource>(),
        Some(&GitSource {
            url: String::from("https://github.com/pisa-engine/pisa.git"),
            branch: String::from("dev")
        })
    );
    assert_eq!(
        PisaSource::parse(
            &YamlLoader::load_from_str(
                r#"
                        type: 112
                    "#
            )
            .unwrap()[0]
        )
        .err(),
        Some(Error::from("missing or corrupted source.type"))
    );
    assert_eq!(
        PisaSource::parse(
            &YamlLoader::load_from_str(
                r#"
                        type: "foo"
                    "#
            )
            .unwrap()[0]
        )
        .err(),
        Some(Error::from("unknown source type: foo"))
    );
    assert_eq!(
        PisaSource::parse(
            &YamlLoader::load_from_str(
                r#"
                        type: path
                        path: "pisa/build/bin"
                    "#
            )
            .unwrap()[0]
        )
        .unwrap()
        .downcast_ref::<CustomPathSource>(),
        Some(&CustomPathSource::from("pisa/build/bin"))
    );
    assert_eq!(
        PisaSource::parse_path_source(
            &YamlLoader::load_from_str(
                r#"
                        type: path
                    "#
            )
            .unwrap()[0]
        )
        .err(),
        Some(Error::from("missing source.path"))
    );
    assert_eq!(
        PisaSource::parse_path_source(
            &YamlLoader::load_from_str(
                r#"
                        type: path
                        path: {}
                    "#
            )
            .unwrap()[0]
        )
        .err(),
        Some(Error::from("missing source.path"))
    );
}

#[test]
fn test_custom_path_source_executor() {
    let tmp = TempDir::new("test_custom_path_source_executor").unwrap();
    let bin = tmp.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    assert_eq!(
        Config::new(tmp.path(), Box::new(CustomPathSource::from("bin")))
            .executor()
            .unwrap()
            .downcast_ref::<CustomPathExecutor>()
            .unwrap()
            .path(),
        bin.as_path()
    );
}

#[test]
fn test_custom_path_source_fail() {
    let source = CustomPathSource::from("/nonexistent-path");
    let config = Config::new("workdir", Box::new(source));
    let executor = config.executor().err();
    assert_eq!(
        executor,
        Some(Error::from(
            "Failed to construct executor: not a directory: /nonexistent-path"
        ))
    );
}
