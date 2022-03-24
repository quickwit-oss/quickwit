# Quickwit actors

Yet another actor crate for rust.
This crate exists specifically to answer quickwit needs.
The API may change in the future.

## Objective

- Producing easy-to-reason with code: Quickwit's indexing pipeline is complex as it is.
- Easy to test actors.
- Control over the runtime.

## Non-objective

- High number of message throughput. Most of message exchanged in quickwit
are "large". For instance, it can hold a temp directory with gigabytes worth of data.
The actor dealing with the highest number of messages are the indexer and sources.
One message then typically holds a batch of records.

# Features

- Actor message box
- The framework is meant to run asynchronous actors by default, but it can also run actors that are blocking for long amount of time. The message handler methods are technically asynchronous in both case, but the `Actor::runner` method makes it possible to run an actor with blocking code on a dedicated thread.
- A scheduler actor that makes it possible to mock simulate time.

# Example

```rust
use std::time::Duration;
use async_trait::async_trait;
use quickwit_actors::{Handler, Actor, Universe, ActorContext, ActorExitStatus, Mailbox};

#[derive(Default)]
struct PingReceiver;

impl Actor for PingReceiver {
    type ObservableState = ();
    fn observable_state(&self) -> Self::ObservableState {}
}

#[async_trait]
impl Handler<Ping> for PingReceiver {
    type Reply = String;
    async fn handle(
        &mut self,
        _msg: Ping,
        _ctx: &ActorContext<Self>,
    ) -> Result<String, ActorExitStatus> {
        Ok("Pong".to_string())
    }
}

struct PingSender {
    peer: Mailbox<PingReceiver>,
}

#[derive(Debug)]
struct Loop;

#[derive(Debug)]
struct Ping;

#[async_trait]
impl Actor for PingSender {
    type ObservableState = ();
    fn observable_state(&self) -> Self::ObservableState {}

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(),ActorExitStatus> {
        ctx.send_self_message(Loop).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<Loop> for PingSender {
    type Reply = ();

    async fn handle(
        &mut self,
        _: Loop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let reply_msg = ctx.ask(&self.peer, Ping).await.unwrap();
        println!("{reply_msg}");
        ctx.schedule_self_msg(Duration::from_secs(1), Loop).await;
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let universe = Universe::new();

    let (recv_mailbox, _) =
        universe.spawn_actor(PingReceiver::default()).spawn();

    let ping_sender = PingSender { peer: recv_mailbox };
    let (_, ping_sender_handler) = universe.spawn_actor(ping_sender).spawn();

    ping_sender_handler.join().await;
}
```
