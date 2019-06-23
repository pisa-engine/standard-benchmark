extern crate yaml_rust;

use super::*;
use yaml_rust::YamlLoader;

#[test]
fn test_string_from_yaml() {
    assert_eq!(
        String::from_yaml(&Yaml::String(String::from("value"))),
        Ok(String::from("value"))
    );
    assert_eq!(
        String::from_yaml(&Yaml::Null),
        Err(Error::from("Cannot parse as String: Null"))
    );
    assert_eq!(
        String::from_yaml_at_key(&Yaml::Null, "key"),
        Err(Error::from("No key 'key' found in: Null"))
    );

    let yaml = YamlLoader::load_from_str(
        "key1: val1
key2: val2",
    )
    .unwrap();
    assert!(String::from_yaml_at_key(&yaml[0], "key").is_err());

    let yaml = YamlLoader::load_from_str(
        "key1: val1
key:
  - a
  - b",
    )
    .unwrap();
    assert!(String::from_yaml_at_key(&yaml[0], "key").is_err());

    let yaml = YamlLoader::load_from_str("key: 0.1").unwrap();
    assert!(String::from_yaml_at_key(&yaml[0], "key").is_err());
    let yaml = YamlLoader::load_from_str("key: val").unwrap();
    assert_eq!(
        String::from_yaml_at_key(&yaml[0], "key"),
        Ok(String::from("val"))
    );
}

#[test]
fn test_int_from_yaml() {
    assert_eq!(i64::from_yaml(&Yaml::Integer(17)), Ok(17));
    assert_eq!(
        i64::from_yaml(&Yaml::String(String::from("str"))),
        Err(Error::from("Cannot parse as integer: String(\"str\")"))
    );
}
