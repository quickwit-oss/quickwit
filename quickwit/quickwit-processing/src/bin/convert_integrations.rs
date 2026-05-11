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

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::str::FromStr;

use quickwit_processing::PipelineStepConfig;
use serde::{Deserialize, Serialize};
use vrl::datadog_search_syntax::{BooleanType, QueryNode};

#[derive(Debug, Serialize, Deserialize)]
struct Integration {
    id: String,
    pipeline: PipelineStepConfig,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Store the config per source
    let mut pipeline_per_source: BTreeMap<String, PipelineStepConfig> = Default::default();
    let mut files = fs::read_dir("integrations")
        .expect("Failed to read integrations directory. Run the command in the root of PomChi")
        .map(|entry| {
            entry
                .expect("Failed to read entry in integrations directory")
                .path()
        })
        .collect::<Vec<_>>();
    files.sort();
    // Count unsupported processors per type
    let mut unsupported_processor_count: HashMap<String, usize> = HashMap::new();
    for path in files {
        if !path.is_dir() {
            let integration_name = path.file_name().unwrap().to_str().unwrap();
            if integration_name.ends_with("_tests.yaml") {
                continue; // Skip test files
            }
            if integration_name.ends_with(".yaml") {
                // Deserialize the YAML file to PipelineConfig
                let content = fs::read_to_string(&path)?;
                let integration: Integration = serde_yaml::from_str(&content)
                    .map_err(|e| format!("Failed to parse YAML file {path:?}: {e}"))?;
                match integration.pipeline {
                    // All pipelines have simple filters on source
                    PipelineStepConfig::NestedPipeline {
                        ref common,
                        ref processors,
                        ..
                    } => {
                        let source_values = extract_source_values(&common.filter.query);
                        for source in source_values {
                            assert!(!pipeline_per_source.contains_key(&source));
                            pipeline_per_source.insert(source, integration.pipeline.clone());
                        }
                        let unsupported_processors: Vec<_> =
                            processors.iter().filter(|p| !p.is_supported()).collect();
                        for processor in &unsupported_processors {
                            let count = unsupported_processor_count
                                .entry(processor.name())
                                .or_insert(0);
                            *count += 1;
                        }
                        if !unsupported_processors.is_empty() {
                            let unsupported_names: Vec<_> =
                                unsupported_processors.iter().map(|p| p.name()).collect();
                            println!(
                                "{integration_name}: ⚠️ Unsupported processors: \
                                 {unsupported_names:?}",
                            );
                        } else {
                            println!("{integration_name}: ✅ All processors are supported");
                        }
                    }
                    _ => {
                        panic!(
                            "Unexpected pipeline step type in integration: {integration_name:?}",
                        );
                    }
                }
            } else {
                println!("Skipping non-YAML file: {path:?}");
            }
        } else {
            println!("Skipping directory entry: {path:?}");
        }
    }

    for (processor, count) in &unsupported_processor_count {
        if *count > 0 {
            println!("⚠️ Unsupported processor '{processor}' found {count} times",);
        }
    }

    // Write the map to a file
    let output_path = "integrations_map.json";
    let json_content = serde_json::to_string_pretty(&pipeline_per_source)
        .map_err(|e| format!("Failed to serialize map to JSON: {e}"))?;
    fs::write(output_path, json_content)?;

    Ok(())
}

fn extract_source_values(query: &str) -> Vec<String> {
    let query_node =
        QueryNode::from_str(query).unwrap_or_else(|_| panic!("Failed to parse query: {query}"));

    let mut source_values = Vec::new();
    match query_node {
        QueryNode::MatchAllDocs => todo!(),
        QueryNode::MatchNoDocs => todo!(),
        QueryNode::AttributeTerm { attr, value } => {
            assert_eq!(
                attr, "source",
                "Only 'source' attribute is supported in this context"
            );
            source_values.push(value);
        }
        QueryNode::Boolean { oper, nodes } => {
            assert!(
                oper == BooleanType::Or,
                "Only 'or' operator is supported in this context"
            );
            for node in nodes {
                if let QueryNode::AttributeTerm { attr, value } = node {
                    assert_eq!(
                        attr, "source",
                        "Only 'source' attribute is supported in this context"
                    );
                    source_values.push(value);
                } else {
                    panic!("Unsupported node type in boolean operation: {node:?}");
                }
            }
        }
        _ => {
            panic!("Unsupported query node type: {query_node:?}");
        }
    }

    source_values
}
