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

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, Universe};
use rand::prelude::IteratorRandom;

struct PingReceiver {
    name: &'static str,
    num_ping_received: usize,
}

impl PingReceiver {
    pub fn with_name(name: &'static str) -> Self {
        PingReceiver {
            name,
            num_ping_received: 0,
        }
    }
}

impl Actor for PingReceiver {
    type ObservableState = usize;

    fn observable_state(&self) -> Self::ObservableState {
        self.num_ping_received
    }
}

#[async_trait]
impl Handler<Ping> for PingReceiver {
    type Reply = String;
    async fn handle(
        &mut self,
        _msg: Ping,
        _ctx: &ActorContext<Self>,
    ) -> Result<String, ActorExitStatus> {
        self.num_ping_received += 1;
        Ok(format!(
            "Actor `{}` received {} pings",
            self.name, self.num_ping_received
        ))
    }
}

// ------------------

#[derive(Default)]
struct PingSender {
    peers: Vec<Mailbox<PingReceiver>>,
    num_ping_emitted: usize,
}

#[derive(Debug)]
struct Loop;

#[derive(Debug)]
struct Ping;

#[derive(Debug)]
pub struct AddPeer(Mailbox<PingReceiver>);

#[async_trait]
impl Actor for PingSender {
    type ObservableState = ();
    fn observable_state(&self) -> Self::ObservableState {}

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        ctx.send_self_message(Loop).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<Loop> for PingSender {
    type Reply = ();

    async fn handle(&mut self, _: Loop, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        let random_peer_id_opt = (0..self.peers.len()).choose(&mut rand::thread_rng());
        if let Some(random_peer_id) = random_peer_id_opt {
            match ctx.ask(&self.peers[random_peer_id], Ping).await {
                Ok(reply_msg) => {
                    println!("{reply_msg}");
                }
                Err(_send_error) => {
                    self.peers.swap_remove(random_peer_id);
                }
            }
        }
        self.num_ping_emitted += 1;
        if self.num_ping_emitted == 10 {
            return Err(ActorExitStatus::Success);
        }
        ctx.schedule_self_msg(Duration::from_secs(1), Loop);
        Ok(())
    }
}

#[async_trait]
impl Handler<AddPeer> for PingSender {
    type Reply = ();

    async fn handle(
        &mut self,
        add_peer_msg: AddPeer,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let AddPeer(peer_mailbox) = add_peer_msg;
        self.peers.push(peer_mailbox);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let universe = Universe::new();

    let (roger_mailbox, _) = universe
        .spawn_builder()
        .spawn(PingReceiver::with_name("Roger"));

    let (myriam_mailbox, _) = universe
        .spawn_builder()
        .spawn(PingReceiver::with_name("Myriam"));

    let (ping_sender_mailbox, ping_sender_handler) =
        universe.spawn_builder().spawn(PingSender::default());

    ping_sender_mailbox
        .send_message(AddPeer(roger_mailbox))
        .await
        .unwrap();
    ping_sender_mailbox
        .send_message(AddPeer(myriam_mailbox))
        .await
        .unwrap();

    ping_sender_handler.join().await;
}
