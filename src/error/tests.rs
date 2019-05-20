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
