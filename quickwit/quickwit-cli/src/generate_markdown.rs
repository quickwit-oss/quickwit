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

use clap::Command;
use quickwit_cli::cli::build_cli;
use quickwit_serve::BuildInfo;
use toml::Value;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let version_text = BuildInfo::get_version_text();
    let app = build_cli()
        .version(version_text)
        .disable_help_subcommand(true);

    generate_markdown_from_clap(&app);
    Ok(())
}

fn markdown_for_command(command: &Command, doc_extensions: &toml::Value) {
    let command_name = command.get_name();
    let command_ext: Option<&Value> = doc_extensions.get(command_name.to_string());
    markdown_for_command_helper(command, command_ext, command_name.to_string(), Vec::new());
}

fn markdown_for_subcommand(
    subcommand: &Command,
    command_group: Vec<String>,
    doc_extensions: &toml::Value,
    level: usize,
) {
    let subcommand_name = subcommand.get_name();

    let command_name = format!("{} {}", command_group.join(" "), subcommand_name);
    let header_level = "#".repeat(level);
    println!("{header_level} {command_name}\n");

    let subcommand_ext: Option<&Value> = {
        let mut val_opt: Option<&Value> = doc_extensions.get(command_group[0].to_string());
        for command in command_group
            .iter()
            .skip(1)
            .chain(&[subcommand_name.to_string()])
        {
            if let Some(val) = val_opt {
                val_opt = val.get(command);
            }
        }
        val_opt
    };
    markdown_for_command_helper(subcommand, subcommand_ext, command_name, command_group);
}

fn markdown_for_command_helper(
    subcommand: &Command,
    subcommand_ext: Option<&Value>,
    command_name: String,
    command_group: Vec<String>,
) {
    let long_about_opt: Option<&str> =
        subcommand_ext.and_then(|el| el.get("long_about").and_then(|el| el.as_str()));

    let note: Option<&str> =
        subcommand_ext.and_then(|el| el.get("note").and_then(|el| el.as_str()));

    let examples_opt: Option<&Vec<Value>> =
        subcommand_ext.and_then(|el| el.get("examples").and_then(|el| el.as_array()));

    if let Some(about) = long_about_opt {
        if !about.trim().is_empty() {
            println!("{about}  ");
        }
    } else if let Some(about) = subcommand.get_about()
        && !about.to_string().trim().is_empty()
    {
        println!("{about}  ");
    }

    if let Some(note) = note {
        println!(":::note");
        println!("{note}");
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
        println!("quickwit {command_name}");
        for arg in &arguments {
            let is_required = arg.is_required_set();
            let is_bool = !arg.get_action().takes_values();

            let mut commando = format!("--{}", arg.get_id());
            if !is_bool {
                commando = format!("{} <{}>", commando, arg.get_id());
            }
            if !is_required {
                commando = format!("[{commando}]");
            }
            println!("    {commando}");
        }
        println!("```");
        println!("\n*Options*\n");

        // Check if any options have defaults to know if the "Default" column is needed
        let has_defaults = arguments
            .iter()
            .any(|arg| !arg.get_default_values().is_empty());

        if has_defaults {
            println!("| Option | Description | Default |");
            println!("|-----------------|-------------|--------:|");
            for arg in arguments {
                let default = if let Some(val) = arg.get_default_values().first() {
                    format!("`{}`", val.to_str().unwrap())
                } else {
                    "".to_string()
                };
                println!(
                    "| `--{}` | {} | {} |",
                    arg.get_id(),
                    arg.get_help().unwrap_or_default(),
                    default
                );
            }
        } else {
            println!("| Option | Description |");
            println!("|-----------------|-------------|");
            for arg in arguments {
                println!(
                    "| `--{}` | {} |",
                    arg.get_id(),
                    arg.get_help().unwrap_or_default()
                );
            }
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
        let command_name = command.get_name(); // index, split, source
        println!("## {command_name}");
        if let Some(about) = command.get_long_about().or_else(|| command.get_about())
            && !about.to_string().trim().is_empty()
        {
            println!("{about}\n");
        }

        if command.get_subcommands().count() == 0 {
            markdown_for_command(command, &doc_extensions);
            continue;
        }

        let excluded_doc_commands = ["merge", "local-search"];
        for subcommand in command
            .get_subcommands()
            .filter(|subcommand| !excluded_doc_commands.contains(&subcommand.get_name()))
        {
            let commands = vec![command.get_name().to_string()];
            markdown_for_subcommand(subcommand, commands, &doc_extensions, 3);

            for subsubcommand in subcommand.get_subcommands() {
                let commands = vec![
                    command.get_name().to_string(),
                    subcommand.get_name().to_string(),
                ];
                markdown_for_subcommand(subsubcommand, commands, &doc_extensions, 4);
            }
        }
    }
    std::process::exit(0);
}
