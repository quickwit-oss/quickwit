use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::bail;
use path_slash::*;
use regex::Regex;

use super::GeneratedFiles;

/// Writes the input to the specified file, preceded by a header comment indicating generated code
pub fn write_file(
    input: String,
    docs: Option<&PathBuf>,
    dir: &PathBuf,
    file_name: &str,
    tracker: &mut GeneratedFiles,
) -> anyhow::Result<()> {
    let mut path = dir.clone();
    path.push(PathBuf::from_slash(file_name));

    let mut file = File::create(&path)?;
    file.write_all(
        b"/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the \"License\"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// -----------------------------------------------
// This file is generated, Please do not edit it manually.
// Run the following in the root of the repo to regenerate:
//
// cargo make generate-api
// -----------------------------------------------
",
    )?;

    if let Some(path) = docs {
        if path.exists() {
            file.write_all(b"\n")?;
            let docs_file = File::open(path)?;
            for line in std::io::BufReader::new(docs_file).lines() {
                let line = line?;

                file.write_all(b"//! ")?;
                file.write_all(line.as_bytes())?;
                file.write_all(b"\n")?;
            }
        } else {
            warn!("Missing docs file {:?}", docs)
        }
    }

    file.write_all(b"\n")?;
    file.write_all(input.as_bytes())?;
    file.write_all(b"\n")?;

    tracker.written.insert(file_name.to_owned());

    Ok(())
}

lazy_static! {
    static ref START_REGEX: Regex = Regex::new("// *GENERATED-BEGIN:([a-zA-Z0-9-_]+)").unwrap();
    static ref END_REGEX: Regex = Regex::new("// *GENERATED-END").unwrap();
}

/// Merge some generated content into an existing file. Content is fetch using the `get_content`
/// function to accommodate for various generation strategies or content sources.
///
/// Generated sections in the file are delimited by start and end markers. The start marker also
/// indicates the name of the section to be merged, allowing a file to contain several generated
/// sections:
///
/// ```
/// // This is a regular section in the file
///
/// // GENERATED-BEGIN:foo
/// // this part will be replaced by the contents of the "foo" section
/// // GENERATED-END
///
/// // Another regular section
///
/// // GENERATED-BEGIN:bar
/// // this part will be replaced by the contents of the "bar" section
/// // GENERATED-END
///
/// // End of file
/// ```
pub fn merge_file(
    mut get_content: impl FnMut(&str) -> Option<String>,
    dir: &Path,
    file_name: &str,
    tracker: &mut GeneratedFiles,
) -> anyhow::Result<()> {
    let mut path = dir.to_owned();
    path.push(PathBuf::from_slash(file_name));

    let mut in_generated_section = false;
    let mut output = String::with_capacity(1024);

    let file = File::open(&path)?;

    for (line_no, line) in BufReader::new(file).lines().enumerate() {
        let line = line?;

        if let Some(captures) = START_REGEX.captures(&line) {
            if in_generated_section {
                bail!(
                    "{}:{} - Previous generated section wasn't closed",
                    file_name,
                    line_no
                );
            }

            // Output start marker
            output.push_str(&line);
            output.push_str(
                "\n// Generated code - do not edit until the next GENERATED-END marker\n\n",
            );
            in_generated_section = true;

            // and content
            let section = captures.get(1).unwrap().as_str();

            if let Some(text) = get_content(section) {
                output.push_str(&text);
            } else {
                bail!(
                    "{}:{} - No content found to generate section '{}'",
                    file_name,
                    line_no,
                    section
                );
            }
        } else if END_REGEX.is_match(&line) {
            if !in_generated_section {
                bail!("{}:{} - Missing GENERATED-START marker", file_name, line_no);
            }

            // Output end marker
            output.push('\n');
            output.push_str(&line);
            output.push('\n');
            in_generated_section = false;
        } else if !in_generated_section {
            // Non-generated line
            output.push_str(&line);
            output.push('\n');
        }
    }

    if in_generated_section {
        bail!(
            "{} - Missing GENERATED-END marker at end of file",
            file_name
        );
    }

    std::fs::write(&path, output)?;

    tracker.merged.insert(file_name.to_owned());

    Ok(())
}

#[cfg(test)]
mod test {

    use std::fs;

    use super::super::GeneratedFiles;

    #[test]
    pub fn nominal_merge() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let dir_path = dir.path();
        let file_name = "test_merge.rs";

        let mut tracker = GeneratedFiles::default();

        let mut file_path = dir_path.to_owned();
        file_path.push(file_name);

        fs::write(
            &file_path,
            r#"
// Start of file

// GENERATED-BEGIN:foo - we can add a comment here
// this part will be replaced by the contents of the "foo" section
// GENERATED-END

// Another regular section

// GENERATED-BEGIN:bar
// this part will be replaced by the contents of the "bar" section
// GENERATED-END

// End of file
"#,
        )?;

        super::merge_file(
            |section| Some(format!("Contents of section {}\n", section)),
            dir_path,
            file_name,
            &mut tracker,
        )?;

        let expected = r#"
// Start of file

// GENERATED-BEGIN:foo - we can add a comment here
// Generated code - do not edit until the next GENERATED-END marker

Contents of section foo

// GENERATED-END

// Another regular section

// GENERATED-BEGIN:bar
// Generated code - do not edit until the next GENERATED-END marker

Contents of section bar

// GENERATED-END

// End of file
"#;

        let generated = fs::read_to_string(&file_path)?;
        assert_eq!(expected, generated);
        Ok(())
    }

    #[test]
    fn unbalanced_sections() -> anyhow::Result<()> {
        merge_should_fail(
            r#"
// GENERATED-BEGIN:foo
"#,
        )?;

        merge_should_fail(
            r#"
// GENERATED-END
"#,
        )?;

        merge_should_fail(
            r#"
// GENERATED-BEGIN:foo
// GENERATED-BEGIN:bar
"#,
        )?;

        merge_should_fail(
            r#"
// GENERATED-BEGIN:foo
// GENERATED-END
// GENERATED-END
"#,
        )?;

        Ok(())
    }

    fn merge_should_fail(input: &str) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let dir_path = dir.path();
        let file_name = "test_merge.rs";

        let mut file_path = dir_path.to_owned();
        file_path.push(file_name);

        fs::write(&file_path, input)?;

        let mut tracker = GeneratedFiles::default();

        let r = super::merge_file(
            |section| Some(format!("Contents of section {}\n", section)),
            dir_path,
            file_name,
            &mut tracker,
        );

        assert!(r.is_err());

        Ok(())
    }
}
