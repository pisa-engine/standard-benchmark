extern crate tempdir;

use super::*;
use tempdir::TempDir;

#[test]
#[cfg_attr(target_family, unix)]
fn test_print() {
    assert_eq!(ExtCommand::new("ls").to_string(), "ls");
    assert_eq!(ExtCommand::new("ls").arg("-l").to_string(), "ls -l");
    assert_eq!(
        ExtCommand::new("ls").args(&["-l", "dir"]).to_string(),
        "ls -l dir"
    );
    assert_eq!(
        ExtCommand::new("ls").pipe_new("wc").to_string(),
        "ls\n    | wc"
    );
}

#[test]
fn test_simple_command() -> Result<(), Error> {
    assert_eq!(
        std::str::from_utf8(&ExtCommand::new("echo").arg("hello").output()?.stdout),
        Ok("hello\n")
    );
    let mut cmd = Command::new("echo");
    cmd.arg("hello");
    assert_eq!(
        std::str::from_utf8(&ExtCommand::from(cmd).output()?.stdout),
        Ok("hello\n")
    );
    assert!(ExtCommand::new("echo").arg("hello").mute().output().is_ok());
    assert!(ExtCommand::new("echo")
        .arg("hello")
        .mute()
        .status()?
        .success());
    Ok(())
}

#[test]
fn test_piped_commands() -> Result<(), Error> {
    assert_eq!(
        std::str::from_utf8(
            &ExtCommand::new("sh")
                .arg("-c")
                .arg("echo hello")
                .pipe_new("sh")
                .arg("-c")
                .arg("wc -l")
                .output()?
                .stdout
        ),
        Ok("1\n")
    );
    assert_eq!(
        std::str::from_utf8(
            &ExtCommand::new("echo")
                .arg("hello")
                .pipe_new("wc")
                .arg("-l")
                .pipe_new("grep")
                .arg("1")
                .output()?
                .stdout
        ),
        Ok("1\n")
    );
    assert_eq!(
        std::str::from_utf8(
            &ExtCommand::new("echo")
                .arg("hello")
                .pipe_new("wc")
                .arg("-l")
                .pipe_command(Command::new("grep"))
                .arg("1")
                .output()?
                .stdout
        ),
        Ok("1\n")
    );
    assert!(ExtCommand::new("echo")
        .arg("hello")
        .pipe_new("wc")
        .arg("-l")
        .status()?
        .success());
    assert!(ExtCommand::new("echo")
        .arg("hello")
        .pipe_new("wc")
        .arg("-l")
        .pipe_new("grep")
        .args(&["1"])
        .status()?
        .success());
    Ok(())
}

#[test]
fn test_change_dir() -> Result<(), Error> {
    let tmp = TempDir::new("command")?;
    let file = tmp.path().join("f1");
    std::fs::File::create(&file)?;
    assert_eq!(
        std::str::from_utf8(
            &ExtCommand::new("ls")
                .current_dir(&tmp.path())
                .output()?
                .stdout
        ),
        Ok("f1\n")
    );
    assert_eq!(
        std::str::from_utf8(
            &ExtCommand::new("echo")
                .pipe_new("ls")
                .current_dir(&tmp.path())
                .output()?
                .stdout
        ),
        Ok("f1\n")
    );
    assert_eq!(
        std::str::from_utf8(
            &ExtCommand::new("echo")
                .pipe_new("echo")
                .pipe_new("ls")
                .current_dir(&tmp.path())
                .output()?
                .stdout
        ),
        Ok("f1\n")
    );
    Ok(())
}
