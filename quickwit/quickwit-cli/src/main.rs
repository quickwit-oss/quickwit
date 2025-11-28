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

#![recursion_limit = "256"]

use std::collections::BTreeMap;

use anyhow::Context;
use colored::Colorize;
use opentelemetry::global;
use quickwit_cli::checklist::RED_COLOR;
use quickwit_cli::cli::{CliCommand, build_cli};
#[cfg(feature = "jemalloc")]
use quickwit_cli::jemalloc::start_jemalloc_metrics_loop;
use quickwit_cli::logger::setup_logging_and_tracing;
use quickwit_cli::{busy_detector, install_default_crypto_ring_provider};
use quickwit_common::runtimes::scrape_tokio_runtime_metrics;
use quickwit_serve::BuildInfo;
use tracing::error;

/// The main tokio runtime takes num_cores / 3 threads by default, and can be overridden by the
/// QW_RUNTIME_NUM_THREADS environment variable.
fn get_main_runtime_num_threads() -> usize {
    let default_num_runtime_threads: usize = quickwit_common::num_cpus().div_ceil(3);
    quickwit_common::get_from_env(
        "QW_TOKIO_RUNTIME_NUM_THREADS",
        default_num_runtime_threads,
        false,
    )
}

fn main() -> anyhow::Result<()> {
    let main_runtime_num_threads: usize = get_main_runtime_num_threads();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_thread_unpark(busy_detector::thread_unpark)
        .on_thread_park(busy_detector::thread_park)
        .thread_name("main_runtime_thread")
        .worker_threads(main_runtime_num_threads)
        .build()
        .context("failed to start main Tokio runtime")?;

    scrape_tokio_runtime_metrics(rt.handle(), "main");

    rt.block_on(main_impl())
}

fn register_build_info_metric() {
    use itertools::Itertools;
    let build_info = BuildInfo::get();
    let mut build_kvs = BTreeMap::default();
    build_kvs.insert("build_date", build_info.build_date.to_string());
    build_kvs.insert("commit_hash", build_info.commit_short_hash.to_string());
    build_kvs.insert("version", build_info.version.to_string());
    if !build_info.commit_tags.is_empty() {
        let tags_str = build_info.commit_tags.iter().join(",");
        build_kvs.insert("commit_tags", tags_str);
    }
    build_kvs.insert("target", build_info.build_target.to_string());
    quickwit_common::metrics::register_info("build_info", "Quickwit's build info", build_kvs);
}

async fn main_impl() -> anyhow::Result<()> {
    #[cfg(feature = "openssl-support")]
    unsafe {
        openssl_probe::init_openssl_env_vars()
    };
    register_build_info_metric();

    let about_text = about_text();
    let version_text = BuildInfo::get_version_text();

    let app = build_cli().about(about_text).version(version_text);
    let matches = app.get_matches();
    let ansi_colors = !matches.get_flag("no-color");

    let command = match CliCommand::parse_cli_args(matches) {
        Ok(command) => command,
        Err(error) => {
            eprintln!("failed to parse command line arguments: {error:?}");
            std::process::exit(1);
        }
    };

    install_default_crypto_ring_provider();

    #[cfg(feature = "jemalloc")]
    start_jemalloc_metrics_loop();

    let build_info = BuildInfo::get();
    let env_filter_reload_fn =
        setup_logging_and_tracing(command.default_log_level(), ansi_colors, build_info)?;

    let return_code: i32 = if let Err(command_error) = command.execute(env_filter_reload_fn).await {
        error!(error=%command_error, "command failed");
        eprintln!(
            "{} command failed: {:?}\n",
            "✘".color(RED_COLOR),
            command_error
        );
        1
    } else {
        0
    };

    global::shutdown_tracer_provider();
    std::process::exit(return_code)
}

/// Return the about text with telemetry info.
fn about_text() -> String {
    let mut about_text = String::from(
        "Sub-second search & analytics engine on cloud storage.\n  Find more information at https://quickwit.io/docs\n\n",
    );
    if !quickwit_telemetry::is_telemetry_disabled() {
        about_text += "Telemetry: enabled";
    }
    about_text
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::time::Duration;

    use bytesize::ByteSize;
    use quickwit_cli::ClientArgs;
    use quickwit_cli::cli::{CliCommand, build_cli};
    use quickwit_cli::index::{
        ClearIndexArgs, CreateIndexArgs, DeleteIndexArgs, DescribeIndexArgs, IndexCliCommand,
        IngestDocsArgs, SearchIndexArgs,
    };
    use quickwit_cli::split::{DescribeSplitArgs, SplitCliCommand};
    use quickwit_cli::tool::{
        ExtractSplitArgs, GarbageCollectIndexArgs, LocalIngestDocsArgs, LocalSearchArgs, MergeArgs,
        ToolCliCommand,
    };
    use quickwit_common::uri::Uri;
    use quickwit_config::SourceInputFormat;
    use quickwit_rest_client::models::Timeout;
    use quickwit_rest_client::rest_client::CommitType;
    use reqwest::Url;

    #[test]
    fn test_parse_clear_args() {
        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from(["index", "clear", "--index", "wikipedia"])
            .unwrap();
        let command = CliCommand::parse_cli_args(matches).unwrap();
        let expected_cmd = CliCommand::Index(IndexCliCommand::Clear(ClearIndexArgs {
            client_args: ClientArgs::default(),
            index_id: "wikipedia".to_string(),
            assume_yes: false,
        }));
        assert_eq!(command, expected_cmd);

        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from(["index", "clear", "--index", "wikipedia", "--yes"])
            .unwrap();
        let command = CliCommand::parse_cli_args(matches).unwrap();
        let expected_cmd = CliCommand::Index(IndexCliCommand::Clear(ClearIndexArgs {
            client_args: ClientArgs::default(),
            index_id: "wikipedia".to_string(),
            assume_yes: true,
        }));
        assert_eq!(command, expected_cmd);
    }

    #[test]
    fn test_parse_create_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let _ = app
            .try_get_matches_from(["new", "--index-uri", "file:///indexes/wikipedia"])
            .unwrap_err();

        let app = build_cli().no_binary_name(true);
        let matches =
            app.try_get_matches_from(["index", "create", "--index-config", "index-conf.yaml"])?;
        let command = CliCommand::parse_cli_args(matches)?;
        let expected_index_config_uri = Uri::from_str(&format!(
            "file://{}/index-conf.yaml",
            std::env::current_dir().unwrap().display()
        ))
        .unwrap();
        let expected_cmd = CliCommand::Index(IndexCliCommand::Create(CreateIndexArgs {
            client_args: ClientArgs::default(),
            index_config_uri: expected_index_config_uri.clone(),
            overwrite: false,
            assume_yes: false,
        }));
        assert_eq!(command, expected_cmd);

        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "index",
            "create",
            "--index-config",
            "index-conf.yaml",
            "--overwrite",
        ])?;
        let command = CliCommand::parse_cli_args(matches)?;
        let expected_cmd = CliCommand::Index(IndexCliCommand::Create(CreateIndexArgs {
            client_args: ClientArgs::default(),
            index_config_uri: expected_index_config_uri,
            overwrite: true,
            assume_yes: false,
        }));
        assert_eq!(command, expected_cmd);

        Ok(())
    }

    #[test]
    fn test_parse_ingest_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "index",
            "ingest",
            "--index",
            "wikipedia",
            "--endpoint",
            "http://127.0.0.1:8000",
            "--retries",
            "2",
        ])?;
        let command = CliCommand::parse_cli_args(matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Ingest(
                IngestDocsArgs {
                    client_args,
                    index_id,
                    input_path_opt: None,
                    batch_size_limit_opt: None,
                    commit_type: CommitType::Auto,
                    detailed_response: false,
                })) if &index_id == "wikipedia"
                && client_args.timeout.is_none()
                && client_args.connect_timeout.is_none()
                && client_args.commit_timeout.is_none()
                && client_args.cluster_endpoint == Url::from_str("http://127.0.0.1:8000").unwrap()
                && client_args.num_retries == 2
        ));

        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "index",
            "ingest",
            "--index",
            "wikipedia",
            "--detailed-response",
            "--batch-size-limit",
            "8MB",
            "--force",
        ])?;
        let command = CliCommand::parse_cli_args(matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Ingest(
                IngestDocsArgs {
                    client_args,
                    index_id,
                    input_path_opt: None,
                    batch_size_limit_opt: Some(batch_size_limit),
                    commit_type: CommitType::Force,
                    detailed_response: true,
                })) if &index_id == "wikipedia"
                        && client_args.cluster_endpoint == Url::from_str("http://127.0.0.1:7280").unwrap()
                        && client_args.timeout.is_none()
                        && client_args.connect_timeout.is_none()
                        && client_args.commit_timeout.is_none()
                        && client_args.num_retries == 0
                        && batch_size_limit == ByteSize::mb(8)
        ));

        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "index",
            "ingest",
            "--index",
            "wikipedia",
            "--batch-size-limit",
            "4KB",
            "--wait",
        ])?;
        let command = CliCommand::parse_cli_args(matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Ingest(
                IngestDocsArgs {
                    client_args,
                    index_id,
                    input_path_opt: None,
                    batch_size_limit_opt: Some(batch_size_limit),
                    commit_type: CommitType::WaitFor,
                    detailed_response: false,
                })) if &index_id == "wikipedia"
                    && client_args.cluster_endpoint == Url::from_str("http://127.0.0.1:7280").unwrap()
                    && client_args.timeout.is_none()
                    && client_args.connect_timeout.is_none()
                    && client_args.commit_timeout.is_none()
                    && client_args.num_retries == 0
                    && batch_size_limit == ByteSize::kb(4)
        ));

        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "index",
            "ingest",
            "--index",
            "wikipedia",
            "--timeout",
            "10s",
            "--connect-timeout",
            "2s",
        ])?;
        let command = CliCommand::parse_cli_args(matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Ingest(
                IngestDocsArgs {
                    client_args,
                    index_id,
                    input_path_opt: None,
                    batch_size_limit_opt: None,
                    commit_type: CommitType::Auto,
                    detailed_response: false,
                })) if &index_id == "wikipedia"
                        && client_args.cluster_endpoint == Url::from_str("http://127.0.0.1:7280").unwrap()
                        && client_args.timeout == Some(Timeout::from_secs(10))
                        && client_args.connect_timeout == Some(Timeout::from_secs(2))
                        && client_args.commit_timeout.is_none()
                        && client_args.num_retries == 0
        ));

        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "index",
            "ingest",
            "--index",
            "wikipedia",
            "--timeout",
            "none",
            "--wait",
            "--connect-timeout",
            "15s",
            "--commit-timeout",
            "4h",
        ])?;
        let command = CliCommand::parse_cli_args(matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Ingest(
                IngestDocsArgs {
                    client_args,
                    index_id,
                    input_path_opt: None,
                    batch_size_limit_opt: None,
                    commit_type: CommitType::WaitFor,
                    detailed_response: false,
                })) if &index_id == "wikipedia"
                        && client_args.cluster_endpoint == Url::from_str("http://127.0.0.1:7280").unwrap()
                        && client_args.timeout == Some(Timeout::none())
                        && client_args.connect_timeout == Some(Timeout::from_secs(15))
                        && client_args.commit_timeout == Some(Timeout::from_hours(4))
        ));

        let app = build_cli().no_binary_name(true);
        assert_eq!(
            app.try_get_matches_from([
                "index",
                "ingest",
                "--index",
                "wikipedia",
                "--wait",
                "--force",
            ])
            .unwrap_err()
            .kind(),
            clap::error::ErrorKind::ArgumentConflict
        );
        Ok(())
    }

    #[test]
    fn test_parse_local_ingest_args() {
        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from([
                "tool",
                "local-ingest",
                "--index",
                "wikipedia",
                "--config",
                "/config.yaml",
                "--overwrite",
                "--keep-cache",
                "--input-format",
                "plain",
                "--transform-script",
                ".message = downcase(string!(.message))",
            ])
            .unwrap();
        let command = CliCommand::parse_cli_args(matches).unwrap();
        assert!(matches!(
            command,
            CliCommand::Tool(ToolCliCommand::LocalIngest(
                LocalIngestDocsArgs {
                    config_uri,
                    index_id,
                    input_path_opt: None,
                    input_format,
                    overwrite,
                    vrl_script: Some(vrl_script),
                    clear_cache,
                })) if &index_id == "wikipedia"
                       && config_uri == Uri::from_str("file:///config.yaml").unwrap()
                       && vrl_script == ".message = downcase(string!(.message))"
                       && overwrite
                       && !clear_cache
                       && input_format == SourceInputFormat::PlainText,
        ));
    }

    #[test]
    fn test_parse_search_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "index",
            "search",
            "--index",
            "wikipedia",
            "--query",
            "Barack Obama",
        ])?;
        let command = CliCommand::parse_cli_args(matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Search(SearchIndexArgs {
                index_id,
                query,
                max_hits: 20,
                start_offset: 0,
                search_fields: None,
                snippet_fields: None,
                start_timestamp: None,
                end_timestamp: None,
                aggregation: None,
                ..
            })) if &index_id == "wikipedia" && &query == "Barack Obama"
        ));

        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "index",
            "search",
            "--index",
            "wikipedia",
            "--query",
            "Barack Obama",
            "--max-hits",
            "50",
            "--start-offset",
            "100",
            "--start-timestamp",
            "0",
            "--end-timestamp",
            "1",
            "--search-fields",
            "title",
            "url",
            "--snippet-fields",
            "body",
        ])?;
        let command = CliCommand::parse_cli_args(matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Search(SearchIndexArgs {
                client_args: _,
                index_id,
                query,
                aggregation: None,
                max_hits: 50,
                start_offset: 100,
                search_fields: Some(search_field_names),
                snippet_fields: Some(snippet_field_names),
                start_timestamp: Some(0),
                end_timestamp: Some(1),
                sort_by_score: false,
            })) if &index_id == "wikipedia"
                  && query == "Barack Obama"
                  && search_field_names == vec!["title".to_string(), "url".to_string()]
                  && snippet_field_names == vec!["body".to_string()]
        ));
        Ok(())
    }

    #[test]
    fn test_parse_local_search_args() {
        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from([
                "tool",
                "local-search",
                "--index",
                "wikipedia",
                "--query",
                "Barack Obama",
            ])
            .unwrap();
        let command = CliCommand::parse_cli_args(matches).unwrap();
        assert!(matches!(
            command,
            CliCommand::Tool(ToolCliCommand::LocalSearch(LocalSearchArgs {
                index_id,
                query,
                max_hits: 20,
                start_offset: 0,
                search_fields: None,
                snippet_fields: None,
                start_timestamp: None,
                end_timestamp: None,
                aggregation: None,
                ..
            })) if &index_id == "wikipedia" && &query == "Barack Obama"
        ));

        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from([
                "tool",
                "local-search",
                "--index",
                "wikipedia",
                "--query",
                "Barack Obama",
                "--max-hits",
                "50",
                "--start-offset",
                "100",
                "--start-timestamp",
                "0",
                "--end-timestamp",
                "1",
                "--search-fields",
                "title",
                "url",
                "--snippet-fields",
                "body",
                "--sort-by-field=-score",
            ])
            .unwrap();
        let command = CliCommand::parse_cli_args(matches).unwrap();
        assert!(matches!(
            command,
            CliCommand::Tool(ToolCliCommand::LocalSearch(LocalSearchArgs {
                config_uri: _,
                index_id,
                query,
                aggregation: None,
                max_hits: 50,
                start_offset: 100,
                search_fields: Some(search_field_names),
                snippet_fields: Some(snippet_field_names),
                start_timestamp: Some(0),
                end_timestamp: Some(1),
                sort_by_field: Some(sort_by_field),
            })) if &index_id == "wikipedia"
                  && query == "Barack Obama"
                  && search_field_names == vec!["title".to_string(), "url".to_string()]
                  && snippet_field_names == vec!["body".to_string()]
                  && sort_by_field == "-score"
        ));
    }

    #[test]
    fn test_parse_delete_args() {
        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from(["index", "delete", "--index", "wikipedia"])
            .unwrap();
        let command = CliCommand::parse_cli_args(matches).unwrap();
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Delete(DeleteIndexArgs {
                index_id,
                dry_run: false,
                ..
            })) if &index_id == "wikipedia"
        ));

        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from(["index", "delete", "--index", "wikipedia", "--dry-run"])
            .unwrap();
        let command = CliCommand::parse_cli_args(matches).unwrap();
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Delete(DeleteIndexArgs {
                index_id,
                dry_run: true,
                ..
            })) if &index_id == "wikipedia"
        ));
    }

    #[test]
    fn test_parse_describe_index_args() {
        let app = build_cli().no_binary_name(true);
        let matches = app
            .try_get_matches_from(["index", "describe", "--index", "wikipedia"])
            .unwrap();
        let command = CliCommand::parse_cli_args(matches).unwrap();
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Describe(DescribeIndexArgs {
                index_id,
                ..
            })) if &index_id == "wikipedia"
        ));
    }

    #[test]
    fn test_parse_split_describe_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "split",
            "describe",
            "--index",
            "wikipedia",
            "--split",
            "ABC",
        ])?;
        let command = CliCommand::parse_cli_args(matches)?;
        assert!(matches!(
            command,
            CliCommand::Split(SplitCliCommand::Describe(DescribeSplitArgs {
                index_id,
                split_id,
                verbose: false,
                ..
            })) if &index_id == "wikipedia" && &split_id == "ABC"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_split_extract_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "tool",
            "extract-split",
            "--index",
            "wikipedia",
            "--split",
            "ABC",
            "--target-dir",
            "datadir",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(matches)?;
        assert!(matches!(
            command,
            CliCommand::Tool(ToolCliCommand::ExtractSplit(ExtractSplitArgs {
                index_id,
                split_id,
                target_dir,
                ..
            })) if &index_id == "wikipedia" && &split_id == "ABC" && target_dir == *"datadir"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_garbage_collect_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "tool",
            "gc",
            "--index",
            "wikipedia",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(matches)?;
        assert!(matches!(
            command,
            CliCommand::Tool(ToolCliCommand::GarbageCollect(GarbageCollectIndexArgs {
                index_id,
                grace_period,
                dry_run: false,
                ..
            })) if &index_id == "wikipedia" && grace_period == Duration::from_secs(60 * 60)
        ));

        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "tool",
            "gc",
            "--index",
            "wikipedia",
            "--grace-period",
            "5m",
            "--config",
            "/config.yaml",
            "--dry-run",
        ])?;
        let command = CliCommand::parse_cli_args(matches)?;
        let expected_config_uri = Uri::from_str("file:///config.yaml").unwrap();
        assert!(matches!(
            command,
            CliCommand::Tool(ToolCliCommand::GarbageCollect(GarbageCollectIndexArgs {
                index_id,
                grace_period,
                config_uri,
                dry_run: true,
            })) if &index_id == "wikipedia" && grace_period == Duration::from_secs(5 * 60) && config_uri == expected_config_uri
        ));
        Ok(())
    }

    #[test]
    fn test_parse_merge_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from([
            "tool",
            "merge",
            "--index",
            "wikipedia",
            "--source",
            "ingest-source",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(matches)?;
        assert!(matches!(
            command,
            CliCommand::Tool(ToolCliCommand::Merge(MergeArgs {
                index_id,
                source_id,
                ..
            })) if &index_id == "wikipedia" && source_id == "ingest-source"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_no_color() {
        // SAFETY: this test may not be entirely sound if not run with nextest or --test-threads=1
        // as this is only a test, and it would be extremly inconvenient to run it in a different
        // way, we are keeping it that way

        let previous_no_color_res = std::env::var("NO_COLOR");
        {
            unsafe { std::env::set_var("NO_COLOR", "whatever_interpreted_as_true") };
            let app = build_cli().no_binary_name(true);
            let matches = app.try_get_matches_from(["run"]).unwrap();
            let no_color = matches.get_flag("no-color");
            assert!(no_color);
        }
        {
            // empty string is false.
            unsafe { std::env::set_var("NO_COLOR", "") };
            let app = build_cli().no_binary_name(true);
            let matches = app.try_get_matches_from(["run"]).unwrap();
            let no_color = matches.get_flag("no-color");
            assert!(!no_color);
        }
        {
            // empty string is false.
            let app = build_cli().no_binary_name(true);
            let matches = app.try_get_matches_from(["run", "--no-color"]).unwrap();
            let no_color = matches.get_flag("no-color");
            assert!(no_color);
        }
        if let Ok(previous_no_color) = previous_no_color_res {
            unsafe { std::env::set_var("NO_COLOR", previous_no_color) };
        }
    }
}
