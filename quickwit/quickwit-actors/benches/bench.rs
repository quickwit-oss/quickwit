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

use async_trait::async_trait;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Universe};

#[derive(Default)]
struct DoNothingActor<const YIELD_AFTER_EACH_MESSAGE: bool>(u64);

#[async_trait]
impl<const YIELD_AFTER_EACH_MESSAGE: bool> Actor for DoNothingActor<YIELD_AFTER_EACH_MESSAGE> {
    type ObservableState = u64;

    fn observable_state(&self) -> u64 {
        self.0
    }

    fn yield_after_each_message(&self) -> bool {
        YIELD_AFTER_EACH_MESSAGE
    }
}

#[derive(Debug)]
struct AddMessage(u64);

#[async_trait]
impl<const YIELD_AFTER_EACH_MESSAGE: bool> Handler<AddMessage>
    for DoNothingActor<YIELD_AFTER_EACH_MESSAGE>
{
    type Reply = ();

    async fn handle(
        &mut self,
        msg: AddMessage,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.0 += msg.0;
        Ok(())
    }
}

async fn actor_bench_code<const YIELD_AFTER_EACH_MESSAGE: bool>(num_messages: usize) {
    let universe = Universe::default();
    let actor: DoNothingActor<YIELD_AFTER_EACH_MESSAGE> = DoNothingActor::default();
    let (mailbox, handle) = universe.spawn_builder().spawn(actor);
    for _ in 0..num_messages {
        mailbox.send_message(AddMessage(1)).await.unwrap();
    }
    drop(mailbox);
    let (_, total) = handle.join().await;
    assert_eq!(total, num_messages as u64);
}

async fn flume_bench_code(num_messages: usize) {
    let (tx, rx) = flume::unbounded::<AddMessage>();
    for _ in 0..num_messages {
        tx.send_async(AddMessage(1)).await.unwrap();
    }
    let join = tokio::task::spawn(async move {
        let mut sum = 0;
        while rx.recv_async().await.is_ok() {
            sum += 1;
        }
        sum
    });
    drop(tx);
    let total = join.await.unwrap();
    assert_eq!(total, num_messages as u64);
}

async fn chan_with_priority_bench_code(num_messages: usize) {
    let (tx, rx) =
        quickwit_actors::channel_with_priority::channel(quickwit_actors::QueueCapacity::Unbounded);
    for _ in 0..num_messages {
        tx.send_low_priority(AddMessage(1)).await.unwrap();
    }
    let join = tokio::task::spawn(async move {
        let mut sum = 0;
        while rx.recv().await.is_ok() {
            sum += 1;
        }
        sum
    });
    drop(tx);
    let total = join.await.unwrap();
    assert_eq!(total, num_messages as u64);
}

fn message_throughput(c: &mut Criterion) {
    let num_messages = [10_000]; // [1, 1_000, 10_000]
    for num_messages in num_messages {
        c.bench_with_input(
            BenchmarkId::new(
                "unlimited_capacity_actors_yield_after_each_message",
                num_messages,
            ),
            &num_messages,
            |b, &num_messages| {
                // Insert a call to `to_async` to convert the bencher to async mode.
                // The timing loops are the same as with the normal bencher.
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(runtime)
                    .iter(|| actor_bench_code::<true>(num_messages));
            },
        );
        c.bench_with_input(
            BenchmarkId::new(
                "unlimited_capacity_actors_no_yield_after_each_message",
                num_messages,
            ),
            &num_messages,
            |b, &num_messages| {
                // Insert a call to `to_async` to convert the bencher to async mode.
                // The timing loops are the same as with the normal bencher.
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(runtime)
                    .iter(|| actor_bench_code::<false>(num_messages));
            },
        );
        c.bench_with_input(
            BenchmarkId::new("unlimited_capacity_flume", num_messages),
            &num_messages,
            |b, &num_messages| {
                // Insert a call to `to_async` to convert the bencher to async mode.
                // The timing loops are the same as with the normal bencher.
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(runtime).iter(|| flume_bench_code(num_messages));
            },
        );
        c.bench_with_input(
            BenchmarkId::new("unlimited_capacity_chan_with_priority", num_messages),
            &num_messages,
            |b, &num_messages| {
                // Insert a call to `to_async` to convert the bencher to async mode.
                // The timing loops are the same as with the normal bencher.
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(runtime)
                    .iter(|| chan_with_priority_bench_code(num_messages));
            },
        );
    }
}

criterion_group!(benches, message_throughput);
criterion_main!(benches);
