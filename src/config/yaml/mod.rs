extern crate yaml_rust;

use crate::error::Error;
use std::{fmt::Debug, path::PathBuf};
use yaml_rust::Yaml;

/// Implementors are able to parse YAML into their own type.
pub trait FromYaml
where
    Self: Sized,
{
    /// Parse YAML to `Self`.
    fn from_yaml(yaml: &Yaml) -> Result<Self, Error>;
    /// Parse a property value at a given key.
    fn from_yaml_at_key(yaml: &Yaml, key: &str) -> Result<Self, Error> {
        match &yaml[key] {
            Yaml::BadValue => Err(Error::from(format!(
                "No key '{}' found in: {:?}",
                key, &yaml
            ))),
            y => Self::from_yaml(&y),
        }
    }
}

impl FromYaml for String {
    fn from_yaml(yaml: &Yaml) -> Result<Self, Error> {
        match yaml {
            Yaml::String(s) => Ok(s.clone()),
            y => Err(Error::from(format!("Cannot parse as String: {:?}", &y))),
        }
    }
}

impl FromYaml for PathBuf {
    fn from_yaml(yaml: &Yaml) -> Result<Self, Error> {
        Ok(Self::from(String::from_yaml(yaml)?))
    }
}
impl FromYaml for i64 {
    fn from_yaml(yaml: &Yaml) -> Result<Self, Error> {
        match yaml {
            &Yaml::Integer(n) => Ok(n),
            y => Err(Error::from(format!("Cannot parse as integer: {:?}", &y))),
        }
    }
}
impl<T> FromYaml for Vec<T>
where
    T: FromYaml,
{
    fn from_yaml(yaml: &Yaml) -> Result<Self, Error> {
        match yaml {
            Yaml::Array(arr) => {
                let mut vec: Self = vec![];
                for val in arr {
                    vec.push(val.parse()?);
                }
                Ok(vec)
            }
            y => Err(Error::from(format!("Cannot parse as vector: {:?}", &y))),
        }
    }
}

/// Extension for `Yaml` struct enabling convenient parsing functions for
/// types that implement `FromYaml`.
pub trait ParseYaml: Debug {
    /// Parse self to `V`.
    fn parse<V: FromYaml>(&self) -> Result<V, Error>;
    /// Parse value at a given key. If does not exist, return error.
    fn parse_field<V: FromYaml>(&self, key: &str) -> Result<V, Error>;
    /// Parse value at a given key. If does not exist, return `Ok(None)`.
    ///
    /// It will, however, return an error if the field exists but cannot be parsed,
    /// or if `self` is not an object with fields.
    fn parse_optional_field<V: FromYaml>(&self, key: &str) -> Result<Option<V>, Error>;
}
impl ParseYaml for Yaml {
    fn parse<V>(&self) -> Result<V, Error>
    where
        V: FromYaml,
    {
        Ok(V::from_yaml(self)?)
    }
    fn parse_field<V>(&self, key: &str) -> Result<V, Error>
    where
        V: FromYaml,
    {
        Ok(V::from_yaml_at_key(self, key)?)
    }
    fn parse_optional_field<V>(&self, key: &str) -> Result<Option<V>, Error>
    where
        V: FromYaml,
    {
        if let Yaml::BadValue = &self[key] {
            Ok(None)
        } else {
            Ok(Some(V::from_yaml_at_key(self, key)?))
        }
    }
}

#[cfg(test)]
mod tests;
