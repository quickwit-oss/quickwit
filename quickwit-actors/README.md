<!--
 Quickwit
  Copyright (C) 2021 Quickwit Inc.

  Quickwit is offered under the AGPL v3.0 and as commercial software.
  For commercial licensing, contact us at hello@quickwit.io.

  AGPL:
  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Affero General Public License as
  published by the Free Software Foundation, either version 3 of the
  License, or (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Affero General Public License for more details.

  You should have received a copy of the GNU Affero General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.
-->

This crate is yet another actor framework.
This one was developped by Quickwit.

# Objective of this quickwit-actors

This crate was created to better organize quickwit's indexing.
Indexing involves different tasks:
- sourcing
- indexing
- packaging
- uploading
- publishing

All of these task have a very different CPU/IO profile,
that range from CPU heavy blocking to code that can only be
asynchronous on the tokio multithreaded runtime (thanks rusoto!).

We also have the following extra requirement:
- We need to be resilient to fault.
- We also need to be testable.
- We want the code to be easy to reason with.
- Finally we need to have good observability.

# Why one of the 10000 other existing framework?

As of today, (2021) 3 framework are standing out.
- actix
- riker
- bastion

Actix does a lot of what we need. In particular, it really
addresses the problem of letting the async world discuss with the sync world.
It has the reputation of being a bit cryptic, but recently it has been largely
improved.
The deal breaker here, is that it relies on Tokio's current thread runtime,
which is incompatible with the use of Rusoto.

Riker has a nice documentation and a nice website. As of today it does nothing
to work with async actors.

Bastion... I don't understand bastion.

There is a 4th one that is well aligned with our need. meio.
Unfortunately, meio does not have any documentation nor examples.



