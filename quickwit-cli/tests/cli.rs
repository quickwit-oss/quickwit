use anyhow::Result;
use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::process::Command;

/// Creates a quickwit-cli command with provided list of arguments.
fn make_command(argument: &str) -> Command {
    let mut cmd = Command::cargo_bin("quickwit-cli").unwrap();
    for arg in argument.split_whitespace() {
        cmd.arg(arg);
    }
    cmd
}

#[test]
fn test_cmd_help() -> anyhow::Result<()> {
    let mut cmd = make_command("--help");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("USAGE"));

    Ok(())
}

#[test]
fn test_cmd_create_index() -> Result<()> {
    let test_dir = create_directory();

    make_command("new")
        .assert()
        .failure()
        .stderr(predicate::str::contains("--index-uri <INDEX URI>"));

    let uri = format!("file://{}/indices/data", test_dir.path().display());
    make_command(
        format!(
            "new --index-uri {} --no-timestamp-field --doc-mapper-type wikipedia",
            uri
        )
        .as_str(),
    )
    .assert()
    .success();

    Ok(())
}

/*
    make_command(
        "new --index-uri file://quickwit-dev/evance/data --no-timestamp-field --doc-mapper-type wikipedia",
    )
    .assert()
    .success();


    let mut cmd = make_command(vec!["new"]);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("USAGE"));

*/
