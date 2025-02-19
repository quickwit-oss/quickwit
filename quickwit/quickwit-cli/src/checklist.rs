// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Display;

use colored::{Color, Colorize};
use itertools::Itertools;
use thiserror::Error;

/// Quickwit main colors slightly adapted to be readable on a terminal.
pub const BLUE_COLOR: Color = Color::TrueColor {
    r: 22,
    g: 74,
    b: 209,
};

pub const GREEN_COLOR: Color = Color::Green;
pub const WHITE_COLOR: Color = Color::TrueColor {
    r: 255,
    g: 255,
    b: 255,
};
pub const RED_COLOR: Color = Color::TrueColor {
    r: 230,
    g: 0,
    b: 34,
};

pub fn print_checklist(check_list_results: &[(&str, anyhow::Result<()>)]) {
    eprintln!(
        "\n{}\n{}",
        "---------------------------------------------------".color(GREEN_COLOR),
        " Connectivity checklist "
            .color(WHITE_COLOR)
            .on_color(GREEN_COLOR)
    );
    let mut errors = Vec::new();
    for (check_item_name, check_item_result) in check_list_results {
        let outcome_symbol = if check_item_result.is_ok() {
            "✔".color(GREEN_COLOR) // '✓'
        } else {
            "✖".color(RED_COLOR) //𐄂
        };
        eprintln!(" {outcome_symbol} {check_item_name}");
        if let Err(check_item_err) = check_item_result {
            errors.push((check_item_name, check_item_err));
        }
    }
    if errors.is_empty() {
        println!();
        return;
    }
    eprintln!(
        "{}\n{}",
        "---------------------------------------------------".color(RED_COLOR),
        " Error Details ".color(WHITE_COLOR).on_color(RED_COLOR)
    );
    for (check_item_name, check_item_err) in errors {
        eprintln!(
            "\n{}\n{:?}",
            format!(" ✖ {check_item_name}").color(RED_COLOR),
            check_item_err
        );
    }
    eprintln!("\n\n");
}

/// Run a checklist and print out its successes and failures on stdout.
///
/// If an error is encountered, the process will exit with exit code 1.
pub fn run_checklist(checks: Vec<(&str, anyhow::Result<()>)>) -> Result<(), ChecklistError> {
    print_checklist(&checks);
    if !checks
        .iter()
        .all(|(_, check_items_res)| check_items_res.is_ok())
    {
        return Err(ChecklistError::from_results(checks));
    }

    Ok(())
}

#[derive(Error, Debug)]
pub struct ChecklistError {
    pub errors: Vec<(String, anyhow::Result<()>)>,
}

impl ChecklistError {
    pub fn from_results(results: Vec<(&str, anyhow::Result<()>)>) -> Self {
        let errors = results
            .into_iter()
            .filter(|(_, check_res)| check_res.is_err())
            .map(|(check_elem, check_res)| (check_elem.to_string(), check_res))
            .collect();
        ChecklistError { errors }
    }
}

impl Display for ChecklistError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let err_string = self
            .errors
            .iter()
            .map(|(check_item, check_item_err)| {
                format!(
                    "\n{}: {}",
                    check_item,
                    check_item_err
                        .as_ref()
                        .expect_err("ChecklistError can't contain success results")
                )
            })
            .join("");
        write!(f, "{err_string}")
    }
}
