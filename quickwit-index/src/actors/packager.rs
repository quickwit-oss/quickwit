use quickwit_actors::Actor;
use quickwit_actors::Mailbox;
use quickwit_actors::SyncActor;

use crate::models::IndexedSplit;
use crate::models::PackagedSplit;

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

pub struct Packager {
    sink: Mailbox<PackagedSplit>,
}

impl Actor for Packager {
    type Message = IndexedSplit;

    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {
        ()
    }
}

impl SyncActor for Packager {
    fn process_message(
        &mut self,
        split: IndexedSplit,
        context: quickwit_actors::ActorContext<'_, Self::Message>,
    ) -> Result<(), quickwit_actors::MessageProcessError> {
        todo!()
    }
}
