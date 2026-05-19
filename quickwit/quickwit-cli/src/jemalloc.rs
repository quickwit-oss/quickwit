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

use std::time::Duration;

use quickwit_common::metrics::{
    MEMORY_ACTIVE_BYTES, MEMORY_ALLOCATED_BYTES, MEMORY_RESIDENT_BYTES,
};
use quickwit_common::rate_limited_warn;
use tikv_jemallocator::Jemalloc;
use tracing::error;

#[cfg(feature = "jemalloc-profiled")]
#[global_allocator]
pub static GLOBAL: quickwit_common::jemalloc_profiled::JemallocProfiled =
    quickwit_common::jemalloc_profiled::JemallocProfiled(Jemalloc);

#[cfg(not(feature = "jemalloc-profiled"))]
#[global_allocator]
pub static GLOBAL: Jemalloc = Jemalloc;

const JEMALLOC_METRICS_POLLING_INTERVAL: Duration = Duration::from_secs(5);

pub async fn jemalloc_metrics_loop() -> tikv_jemalloc_ctl::Result<()> {
    // Obtain a MIB for the `epoch`, `stats.active`, `stats.allocated`, and `stats.resident` keys:
    let epoch_mib = tikv_jemalloc_ctl::epoch::mib()?;
    let active_mib = tikv_jemalloc_ctl::stats::active::mib()?;
    let allocated_mib = tikv_jemalloc_ctl::stats::allocated::mib()?;
    let resident_mib = tikv_jemalloc_ctl::stats::resident::mib()?;

    let mut poll_interval = tokio::time::interval(JEMALLOC_METRICS_POLLING_INTERVAL);
    poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        poll_interval.tick().await;

        if let Err(error) = epoch_mib.advance() {
            rate_limited_warn!(limit_per_min = 1, %error, "failed to advance jemalloc epoch");
            continue;
        }
        match active_mib.read() {
            Ok(active) => MEMORY_ACTIVE_BYTES.set(active as f64),
            Err(error) => {
                rate_limited_warn!(limit_per_min = 1, %error, "failed to read jemalloc stats.active");
            }
        }
        match allocated_mib.read() {
            Ok(allocated) => MEMORY_ALLOCATED_BYTES.set(allocated as f64),
            Err(error) => {
                rate_limited_warn!(limit_per_min = 1, %error, "failed to read jemalloc stats.allocated");
            }
        }
        match resident_mib.read() {
            Ok(resident) => MEMORY_RESIDENT_BYTES.set(resident as f64),
            Err(error) => {
                rate_limited_warn!(limit_per_min = 1, %error, "failed to read jemalloc stats.resident");
            }
        }
    }
}

pub fn start_jemalloc_metrics_loop() {
    tokio::task::spawn(async {
        if let Err(error) = jemalloc_metrics_loop().await {
            error!(%error, "failed to collect metrics from jemalloc");
        }
    });
}

#[cfg(feature = "jemalloc-profiled")]
pub use profiled_tracing::tracing_registry;

#[cfg(feature = "jemalloc-profiled")]
mod profiled_tracing {
    use std::fmt;

    use anyhow::Context;
    use quickwit_common::jemalloc_profiled::JEMALLOC_PROFILER_TARGET;
    use quickwit_telemetry_exporters::EnvFilterReloadFn;
    use time::format_description::BorrowedFormatItem;
    use tracing::{Event, Level, Metadata, Subscriber};
    use tracing_subscriber::filter::filter_fn;
    use tracing_subscriber::fmt::format::{DefaultFields, Writer};
    use tracing_subscriber::fmt::time::{FormatTime, UtcTime};
    use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, FormattedFields};
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::registry::LookupSpan;
    use tracing_subscriber::{EnvFilter, Layer};

    fn time_formatter() -> UtcTime<Vec<BorrowedFormatItem<'static>>> {
        let time_format = time::format_description::parse(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z",
        )
        .expect("time format description should be valid");
        UtcTime::new(time_format)
    }

    /// An event formatter specific to the memory profiler output.
    ///
    /// Also displays a backtrace after the spans and fields of the tracing
    /// event (into separate lines).
    struct ProfilingFormat {
        time_formatter: UtcTime<Vec<BorrowedFormatItem<'static>>>,
    }

    impl Default for ProfilingFormat {
        fn default() -> Self {
            Self {
                time_formatter: time_formatter(),
            }
        }
    }

    impl<S, N> FormatEvent<S, N> for ProfilingFormat
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
        N: for<'a> FormatFields<'a> + 'static,
    {
        fn format_event(
            &self,
            ctx: &FmtContext<'_, S, N>,
            mut writer: Writer<'_>,
            event: &Event<'_>,
        ) -> fmt::Result {
            self.time_formatter.format_time(&mut writer)?;
            write!(writer, " {JEMALLOC_PROFILER_TARGET} ")?;
            if let Some(scope) = ctx.event_scope() {
                let mut seen = false;

                for span in scope.from_root() {
                    write!(writer, "{}", span.metadata().name())?;
                    seen = true;

                    let ext = span.extensions();
                    if let Some(fields) = &ext.get::<FormattedFields<N>>()
                        && !fields.is_empty()
                    {
                        write!(writer, "{{{fields}}}:")?;
                    }
                }

                if seen {
                    writer.write_char(' ')?;
                }
            };

            ctx.format_fields(writer.by_ref(), event)?;
            writeln!(writer)?;

            // Print a backtrace to help identify the callsite.
            backtrace::trace(|frame| {
                backtrace::resolve_frame(frame, |symbol| {
                    if let Some(symbol_name) = symbol.name() {
                        let _ = writeln!(writer, "{symbol_name}");
                    } else {
                        let _ = writeln!(writer, "symb failed");
                    }
                });
                true
            });
            Ok(())
        }
    }

    fn profiler_tracing_filter(metadata: &Metadata) -> bool {
        metadata.is_span() || (metadata.is_event() && metadata.target() == JEMALLOC_PROFILER_TARGET)
    }

    fn startup_env_filter(level: Level) -> anyhow::Result<EnvFilter> {
        let env_filter = std::env::var("RUST_LOG")
            .map(|_| EnvFilter::from_default_env())
            .or_else(|_| EnvFilter::try_new(format!("quickwit={level},tantivy=WARN")))
            .context("failed to set up tracing env filter")?;
        Ok(env_filter)
    }

    /// Configures the regular logging layer and a specific layer that gathers
    /// extra debug information for the jemalloc profiler.
    ///
    /// The jemalloc profiler formatter disables the env filter reloading
    /// because the [tracing_subscriber::reload::Layer] seems to overwrite the
    /// filter configured by [profiler_tracing_filter()] even though it is
    /// applied to a separate layer.
    pub fn tracing_registry(
        level: Level,
        ansi_colors: bool,
    ) -> anyhow::Result<(
        impl Subscriber + for<'span> LookupSpan<'span> + Send + Sync + 'static,
        EnvFilterReloadFn,
    )> {
        let registry = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .event_format(ProfilingFormat::default())
                .fmt_fields(DefaultFields::new())
                .with_ansi(ansi_colors)
                .with_filter(filter_fn(profiler_tracing_filter)),
        );
        let registry = registry.with(
            quickwit_telemetry_exporters::logging_layer(ansi_colors)
                .with_filter(startup_env_filter(level)?),
        );
        Ok((
            registry,
            quickwit_telemetry_exporters::do_nothing_env_filter_reload_fn(),
        ))
    }
}
