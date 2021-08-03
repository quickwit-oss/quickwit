// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use quickwit_actors::Actor;
use quickwit_actors::ActorContext;
use quickwit_actors::AsyncActor;
use async_trait::async_trait;
use crate::models::PackagedSplit;
use crate::models::UploadedSplit;

pub struct Publisher;

impl Actor for Publisher {
    type Message = UploadedSplit;
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {
        ()
    }
}

#[async_trait]
impl AsyncActor for Publisher {
    async fn process_message(
        &mut self,
        message: UploadedSplit,
        context: ActorContext<'_, Self::Message>,
    ) -> Result<(), quickwit_actors::MessageProcessError> {
        println!("publishing");
        Ok(())
    }
}
