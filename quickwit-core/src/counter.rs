/*
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
*/

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

/// Represent an atomic counter we can use to collect metrics.
/// The underlying atomic type uses [`Ordering::Relaxed`] ordering
#[derive(Debug, Default)]
pub struct AtomicCounter(AtomicUsize);

impl AtomicCounter {
    /// Increment the underlying value.
    pub fn inc(&self) -> usize {
        self.add(1)
    }

    /// Add amount to the underlying value
    pub fn add(&self, amount: usize) -> usize {
        self.0.fetch_add(amount, Ordering::Relaxed)
    }

    /// Get the underlying value
    pub fn get(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }

    /// Reset the underlying value to zero.
    pub fn reset(&self) -> usize {
        self.0.swap(0, Ordering::Relaxed)
    }
}
