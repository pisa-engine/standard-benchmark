//! Defines the error type and its formatting and conversion.

extern crate failure;

use failure::{Context, Fail};
use std::fmt::{self, Display};

/// This error type is extensively used throughout the codebase.
/// Any external errors are converted to this one using `convert()` method
/// from [`failure`](https://docs.rs/failure/0.1.5/failure/) crate.
/// The context is a string for simplicity's sake, since the only thing we
/// care about is printing the error to the user.
#[derive(Debug)]
pub struct Error {
    inner: Context<String>,
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        format!("{}", self) == format!("{}", other)
    }
}

impl Fail for Error {
    fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.inner)?;
        if let Some(cause) = self.cause() {
            for cause in cause.iter_chain() {
                write!(f, ": {}", &cause)?;
            }
        }
        write!(f, "")
    }
}

impl From<&'static str> for Error {
    fn from(msg: &'static str) -> Self {
        Self {
            inner: Context::new(msg.to_string()),
        }
    }
}

impl From<String> for Error {
    fn from(msg: String) -> Self {
        Self {
            inner: Context::new(msg),
        }
    }
}

impl From<Context<String>> for Error {
    fn from(inner: Context<String>) -> Self {
        Self { inner }
    }
}

impl From<Context<&'static str>> for Error {
    fn from(inner: Context<&'static str>) -> Self {
        Self {
            inner: inner.map(String::from),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self {
            inner: Context::new(e.to_string()),
        }
    }
}

impl From<failure::Error> for Error {
    fn from(e: failure::Error) -> Self {
        Self {
            inner: Context::new(e.to_string()),
        }
    }
}

impl From<git2::Error> for Error {
    fn from(e: git2::Error) -> Self {
        Self {
            inner: Context::new(e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use failure::{format_err, ResultExt};

    #[test]
    fn test_print_chain() {
        let result: Result<(), Error> = Err(std::io::Error::new(std::io::ErrorKind::Other, "A"))
            .context("B")
            .context("C")
            .map_err(Error::from);
        assert_eq!(result.err().unwrap().to_string(), "C: B: A".to_string());
    }

    #[test]
    fn test_from() {
        assert_eq!(
            Error::from("error message").to_string(),
            "error message".to_string()
        );
        assert_eq!(
            Error::from("error message".to_string()).to_string(),
            "error message".to_string()
        );
        assert_eq!(
            Error::from(Context::new("error message")).to_string(),
            "error message".to_string()
        );
        assert_eq!(
            Error::from(Context::new("error message")).inner.to_string(),
            Context::new("error message").to_string()
        );
        assert_eq!(
            Error::from(Context::new("error message".to_string())).to_string(),
            "error message".to_string()
        );
        assert_eq!(
            Error::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "error message"
            ))
            .to_string(),
            "error message".to_string()
        );
        assert_eq!(
            Error::from(format_err!("error message")).to_string(),
            "error message".to_string()
        );
    }
}
