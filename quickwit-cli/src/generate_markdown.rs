// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use clap::Command;
use quickwit_cli::cli::build_cli;
use toml::Value;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let version_text = format!(
        "{} (commit-hash: {})",
        env!("CARGO_PKG_VERSION"),
        env!("GIT_COMMIT_HASH")
    );

    let app = build_cli()
        .version(version_text.as_str())
        .disable_help_subcommand(true);

    generate_markdown_from_clap(&app);

    Ok(())
}

fn markdown_for_subcommand(
    subcommand: &Command,
    command_group: Vec<String>,
    doc_extensions: &toml::Value,
) {
    let subcommand_name = subcommand.get_name();

    let command_name = format!("{} {}", command_group.join(" "), subcommand_name);
    println!("### {}\n", command_name);

    let subcommand_ext: Option<&Value> = {
        let mut val_opt: Option<&Value> = doc_extensions.get(command_group[0].to_owned());
        for command in command_group
            .iter()
            .skip(1)
            .chain(&[subcommand_name.to_owned()])
        {
            if let Some(val) = val_opt {
                val_opt = val.get(command);
            }
        }
        val_opt
    };
    let long_about_opt: Option<&str> =
        subcommand_ext.and_then(|el| el.get("long_about").and_then(|el| el.as_str()));

    let note: Option<&str> =
        subcommand_ext.and_then(|el| el.get("note").and_then(|el| el.as_str()));

    let examples_opt: Option<&Vec<Value>> =
        subcommand_ext.and_then(|el| el.get("examples").and_then(|el| el.as_array()));

    if let Some(about) = long_about_opt {
        if !about.trim().is_empty() {
            println!("{}  ", about);
        }
    } else if let Some(about) = subcommand.get_about() {
        if !about.trim().is_empty() {
            println!("{}  ", about);
        }
    }

    if let Some(note) = note {
        println!(":::note");
        println!("{}", note);
        println!(":::");
    }

    println!(
        "`quickwit {} {} [args]`",
        command_group.join(" "),
        subcommand.get_name()
    );
    for alias in subcommand.get_all_aliases() {
        println!("`quickwit {} {} [args]`", command_group.join(" "), alias);
    }

    let arguments = subcommand
        .get_arguments()
        .filter(|arg| !(arg.get_id() == "help" || arg.get_id() == "version"))
        .collect::<Vec<_>>();
    if !arguments.is_empty() {
        println!("\n*Synopsis*\n");

        println!("```bash");
        println!("quickwit {}", command_name);
        for arg in &arguments {
            let is_required = arg.is_required_set();
            let is_bool = !arg.is_takes_value_set();

            let mut commando = format!("--{}", arg.get_id());
            if !is_bool {
                commando = format!("{} <{}>", commando, arg.get_id());
            }
            if !is_required {
                commando = format!("[{}]", commando);
            }
            println!("    {}", commando);
        }
        println!("```");

        println!("\n*Options*\n");
        for arg in arguments {
            let default = if let Some(val) = arg.get_default_values().get(0) {
                format!(" (default: {})", val.to_str().unwrap())
            } else {
                "".to_string()
            };
            println!(
                "`--{}` {}{}<br>",
                arg.get_id(),
                arg.get_help().unwrap_or_default(),
                default
            );
        }
    }

    if let Some(examples) = examples_opt {
        println!("\n*Examples*\n");
        for example in examples {
            println!("*{}*", example.get("name").unwrap().as_str().unwrap());
            println!(
                "```bash\n{}\n```\n",
                example.get("command").unwrap().as_str().unwrap()
            );
        }
    }
}

fn generate_markdown_from_clap(command: &Command) {
    let ext_toml = include_str!("cli_doc_ext.toml");
    let doc_extensions: Value = ext_toml.parse::<Value>().unwrap();

    let commands = command.get_subcommands();
    for command in commands {
        let command_name = command.get_name(); // index, split, source, service
        println!("## {}", command_name);
        if let Some(about) = command.get_about() {
            if !about.trim().is_empty() {
                println!("{}\n", about);
            }
        }

        for subcommand in command.get_subcommands().filter(|subcommand| {
            subcommand.get_name() != "demux"
                && subcommand.get_name() != "merge"
                && subcommand.get_name() != "extract"
                && !(subcommand.get_name() == "describe" && command_name == "split")
        }) {
            let commands = vec![command.get_name().to_string()];
            markdown_for_subcommand(subcommand, commands, &doc_extensions);

            for subsubcommand in subcommand.get_subcommands() {
                let commands = vec![
                    command.get_name().to_string(),
                    subcommand.get_name().to_string(),
                ];
                markdown_for_subcommand(subsubcommand, commands, &doc_extensions);
            }
        }
    }
    std::process::exit(0);
}
