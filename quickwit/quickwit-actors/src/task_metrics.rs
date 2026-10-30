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

//! Individual wake-to-poll samples in
//! `quickwit_actor_duration_seconds{actor,operation="wake_to_poll"}`. Each actor task owns its
//! monitor; all instances of an actor name share the histogram. No task, worker, pipeline, or
//! actor-instance labels are added.
//!
//! TaskMonitor measures from the first wake until the next poll, including wakeups inside handler
//! awaits and framework yields. A wake during an ongoing poll includes that poll's remaining
//! execution, so this is not pure runtime-queue or off-CPU time. Initial spawn-to-first-poll delay,
//! time pending before a wakeup, and separately spawned tasks are excluded.
//!
//! Samples are recorded after each poll returns, while the actor is still running. A poll that
//! panics does not produce a sample. Correlate with the runtime's worker queue depth and busy
//! ratio. For example, the p99 wake-to-poll duration in seconds across Indexer instances is:
//! ```text
//! histogram_quantile(0.99, sum by (le) (
//!   rate(quickwit_actor_duration_seconds_bucket{actor="Indexer",operation="wake_to_poll"}[5m])
//! ))
//! ```

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::time::Duration;

use quickwit_metrics::Histogram;
use tokio_metrics::TaskMonitor;

/// Record each wake-to-next-poll interval of one task in the supplied histogram.
/// Callers control opt-in and supply a histogram labeled with the stable actor name.
pub(crate) async fn measure_wake_to_poll<F: Future>(task: F, histogram: Histogram) -> F::Output {
    // This monitor must instrument exactly one task. TaskMonitor adds at most one scheduling
    // interval per poll, so the cumulative-duration delta is one sample, not an average.
    let monitor = TaskMonitor::new();
    let mut task = pin!(monitor.instrument(task));
    let mut previous_count = 0;
    let mut previous_duration = Duration::ZERO;
    poll_fn(|context| {
        let result = task.as_mut().poll(context);
        let metrics = monitor.cumulative();
        if metrics.total_scheduled_count != previous_count {
            assert_eq!(
                metrics.total_scheduled_count,
                previous_count + 1,
                "one task poll can record at most one wake-to-poll interval"
            );
            let duration = metrics.total_scheduled_duration - previous_duration;
            histogram.observe(duration.as_secs_f64());
            previous_count = metrics.total_scheduled_count;
            previous_duration = metrics.total_scheduled_duration;
        }
        result
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::time::Instant;

    use async_trait::async_trait;
    use futures::task::noop_waker;
    use quickwit_metrics::__metrics as metrics;
    use tokio::sync::{mpsc, oneshot};

    use super::*;
    use crate::duration_metrics::{ActorOperation, duration_histogram};
    use crate::{Actor, ActorContext, ActorExitStatus, Universe};

    #[derive(Default)]
    struct Samples(Mutex<Vec<f64>>);

    impl metrics::HistogramFn for Samples {
        fn record(&self, value: f64) {
            self.0.lock().unwrap().push(value);
        }
    }

    #[derive(Default)]
    struct TestRecorder {
        samples: Arc<Samples>,
    }

    impl TestRecorder {
        fn histogram(&self, actor_name: &str) -> Histogram {
            metrics::with_local_recorder(self, || {
                duration_histogram(actor_name, ActorOperation::WakeToPoll)
            })
        }

        fn samples(&self) -> Vec<f64> {
            self.samples.0.lock().unwrap().clone()
        }
    }

    impl metrics::Recorder for TestRecorder {
        fn describe_counter(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_gauge(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_histogram(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }

        fn register_counter(
            &self,
            _: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            metrics::Counter::noop()
        }

        fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
            metrics::Gauge::noop()
        }

        fn register_histogram(
            &self,
            key: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            if key
                .labels()
                .any(|label| label.key() == "operation" && label.value() == "wake_to_poll")
            {
                metrics::Histogram::from_arc(self.samples.clone())
            } else {
                metrics::Histogram::noop()
            }
        }
    }

    #[test]
    fn test_individual_samples_and_polls_without_wakes() {
        let recorder = TestRecorder::default();
        let histogram = recorder.histogram("IndividualSamplesTestActor");
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let future = async move {
            receiver.recv().await.unwrap();
            receiver.recv().await.unwrap();
            Err::<(), _>("completed error")
        };
        let mut task = pin!(measure_wake_to_poll(future, histogram));
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(task.as_mut().poll(&mut context).is_pending());
        assert!(recorder.samples().is_empty());

        let started_at = Instant::now();
        sender.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        assert!(task.as_mut().poll(&mut context).is_pending());
        assert_eq!(recorder.samples().len(), 1);
        // A spurious poll without another wake must not repeat the previous sample.
        assert!(task.as_mut().poll(&mut context).is_pending());
        assert_eq!(recorder.samples().len(), 1);

        sender.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(
            task.as_mut().poll(&mut context),
            Poll::Ready(Err("completed error"))
        );
        let samples = recorder.samples();
        assert_eq!(samples.len(), 2);
        assert!(samples.iter().all(|&sample| sample >= 0.002));
        // Each interval contributes only its own duration, not the cumulative total.
        assert!(samples.iter().sum::<f64>() <= started_at.elapsed().as_secs_f64());
    }

    #[test]
    fn test_first_poll_and_cancellation_do_not_record() {
        let recorder = TestRecorder::default();
        let histogram = recorder.histogram("CancelledTaskTestActor");
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        let mut ready_task = pin!(measure_wake_to_poll(
            std::future::ready(42),
            histogram.clone()
        ));
        assert_eq!(ready_task.as_mut().poll(&mut context), Poll::Ready(42));
        assert!(recorder.samples().is_empty());

        let (sender, receiver) = oneshot::channel();
        let mut task = Box::pin(measure_wake_to_poll(receiver, histogram));
        assert!(task.as_mut().poll(&mut context).is_pending());
        sender.send(()).unwrap();
        drop(task);
        assert!(recorder.samples().is_empty());
    }

    #[test]
    fn test_tasks_have_separate_monitors_and_share_histogram() {
        let recorder = TestRecorder::default();
        let first_histogram = recorder.histogram("SharedHistogramTestActor");
        let second_histogram = recorder.histogram("SharedHistogramTestActor");
        assert_eq!(first_histogram, second_histogram);
        assert_eq!(
            first_histogram.key().name(),
            "quickwit_actor_duration_seconds"
        );
        let labels: Vec<_> = first_histogram.key().labels().collect();
        assert_eq!(labels.len(), 2);
        assert!(
            labels
                .iter()
                .any(|label| label.key() == "actor" && label.value() == "SharedHistogramTestActor")
        );
        assert!(
            labels
                .iter()
                .any(|label| label.key() == "operation" && label.value() == "wake_to_poll")
        );

        let (first_sender, first_receiver) = oneshot::channel();
        let (second_sender, second_receiver) = oneshot::channel();
        let mut first_task = pin!(measure_wake_to_poll(first_receiver, first_histogram));
        let mut second_task = pin!(measure_wake_to_poll(second_receiver, second_histogram));
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(first_task.as_mut().poll(&mut context).is_pending());
        assert!(second_task.as_mut().poll(&mut context).is_pending());
        first_sender.send(()).unwrap();
        assert!(first_task.as_mut().poll(&mut context).is_ready());
        assert_eq!(recorder.samples().len(), 1);
        second_sender.send(()).unwrap();
        assert!(second_task.as_mut().poll(&mut context).is_ready());
        assert_eq!(recorder.samples().len(), 2);
    }

    #[test]
    fn test_wake_during_poll_includes_remaining_poll_time() {
        let recorder = TestRecorder::default();
        let histogram = recorder.histogram("WakeDuringPollTestActor");
        let remaining_poll_time = Duration::from_millis(2);
        let mut first_poll = true;
        let future = poll_fn(|context| {
            if !first_poll {
                return Poll::Ready(());
            }
            first_poll = false;
            context.waker().wake_by_ref();
            std::thread::sleep(remaining_poll_time);
            Poll::Pending
        });
        let mut task = pin!(measure_wake_to_poll(future, histogram));
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(task.as_mut().poll(&mut context).is_pending());
        assert!(task.as_mut().poll(&mut context).is_ready());
        let samples = recorder.samples();
        assert_eq!(samples.len(), 1);
        assert!(samples[0] >= remaining_poll_time.as_secs_f64());
    }

    struct TestActor<const RECORD: bool>;

    #[async_trait]
    impl<const RECORD: bool> Actor for TestActor<RECORD> {
        type ObservableState = ();
        const RECORD_DURATION_METRICS: bool = RECORD;

        fn name(&self) -> String {
            if RECORD {
                "MonitoredTaskTestActor".to_string()
            } else {
                "UnmonitoredTaskTestActor".to_string()
            }
        }

        fn observable_state(&self) {}

        async fn initialize(&mut self, _ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
            tokio::task::yield_now().await;
            Err(ActorExitStatus::Success)
        }
    }

    #[tokio::test]
    async fn test_actor_spawn_monitoring_opt_in() {
        // No process-wide Quickwit runtime initialization or background collector is needed.
        let recorder = TestRecorder::default();
        let universe = Universe::new();
        let (_, monitored) = metrics::with_local_recorder(&recorder, || {
            universe.spawn_builder().spawn(TestActor::<true>)
        });
        assert!(monitored.join().await.0.is_success());
        assert_eq!(recorder.samples().len(), 1);
        let (_, unmonitored) = metrics::with_local_recorder(&recorder, || {
            universe.spawn_builder().spawn(TestActor::<false>)
        });
        assert!(unmonitored.join().await.0.is_success());
        assert_eq!(recorder.samples().len(), 1);
    }
}
