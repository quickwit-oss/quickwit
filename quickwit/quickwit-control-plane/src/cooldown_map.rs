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

use std::fmt::Debug;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use lru::LruCache;

/// A map that keeps track of a cooldown deadline for each of its keys.
///
/// Internally it uses an [`LruCache`] to prune the oldest entries when the
/// capacity is reached. If the capacity is reached but the oldest entry is not
/// outdated, the capacity is extended (2x).
pub struct CooldownMap<K>(LruCache<K, Instant>);

#[derive(Debug, PartialEq)]
pub enum CooldownStatus {
    Ready,
    InCooldown,
}

impl<K: Hash + Eq> CooldownMap<K> {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self(LruCache::new(capacity))
    }

    /// Updates the deadline for the given key if it isn't currently in cooldown.
    ///
    /// The status returned is the one before the update (after an update, the
    /// status is always `InCooldown`).
    pub fn update(&mut self, key: K, cooldown_interval: Duration) -> CooldownStatus {
        let deadline_opt = self.0.get_mut(&key);
        let now = Instant::now();
        if let Some(deadline) = deadline_opt {
            if *deadline > now {
                CooldownStatus::InCooldown
            } else {
                *deadline = now + cooldown_interval;
                CooldownStatus::Ready
            }
        } else {
            let capacity: usize = self.0.cap().into();
            if self.0.len() == capacity
                && let Some((_, deadline)) = self.0.peek_lru()
                && *deadline > now
            {
                // the oldest entry is not outdated, grow the LRU
                self.0.resize(NonZeroUsize::new(capacity * 2).unwrap());
            }
            self.0.push(key, now + cooldown_interval);
            CooldownStatus::Ready
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cooldown_map_resize() {
        let mut cooldown_map = CooldownMap::new(NonZeroUsize::new(2).unwrap());
        let cooldown_interval = Duration::from_secs(1);
        assert_eq!(
            cooldown_map.update("test_key1", cooldown_interval),
            CooldownStatus::Ready
        );
        assert_eq!(
            cooldown_map.update("test_key1", cooldown_interval),
            CooldownStatus::InCooldown
        );
        assert_eq!(
            cooldown_map.update("test_key2", cooldown_interval),
            CooldownStatus::Ready
        );
        assert_eq!(
            cooldown_map.update("test_key2", cooldown_interval),
            CooldownStatus::InCooldown
        );
        // Hitting the capacity, the map should grow transparently
        assert_eq!(
            cooldown_map.update("test_key3", cooldown_interval),
            CooldownStatus::Ready
        );
        assert_eq!(
            cooldown_map.update("test_key1", cooldown_interval),
            CooldownStatus::InCooldown
        );
        assert_eq!(
            cooldown_map.update("test_key2", cooldown_interval),
            CooldownStatus::InCooldown
        );
        assert_eq!(cooldown_map.0.cap(), NonZeroUsize::new(4).unwrap());
    }

    #[test]
    fn test_cooldown_map_expired() {
        let mut cooldown_map = CooldownMap::new(NonZeroUsize::new(2).unwrap());
        let cooldown_interval_short = Duration::from_millis(100);
        let cooldown_interval_long = Duration::from_secs(5);

        assert_eq!(
            cooldown_map.update("test_key_short", cooldown_interval_short),
            CooldownStatus::Ready
        );
        assert_eq!(
            cooldown_map.update("test_key_long", cooldown_interval_long),
            CooldownStatus::Ready
        );

        std::thread::sleep(cooldown_interval_short.mul_f32(1.1));
        assert_eq!(
            cooldown_map.update("test_key_short", cooldown_interval_short),
            CooldownStatus::Ready
        );
        assert_eq!(
            cooldown_map.update("test_key_long", cooldown_interval_long),
            CooldownStatus::InCooldown
        );
    }

    #[test]
    fn test_cooldown_map_eviction() {
        let mut cooldown_map = CooldownMap::new(NonZeroUsize::new(2).unwrap());
        let cooldown_interval_short = Duration::from_millis(100);
        let cooldown_interval_long = Duration::from_secs(5);

        assert_eq!(
            cooldown_map.update("test_key_short", cooldown_interval_short),
            CooldownStatus::Ready
        );
        assert_eq!(
            cooldown_map.update("test_key_long_1", cooldown_interval_long),
            CooldownStatus::Ready
        );

        // after the cooldown period `test_key_short` should be evicted when adding a new key
        std::thread::sleep(cooldown_interval_short.mul_f32(1.1));
        assert_eq!(cooldown_map.0.len(), 2);
        assert_eq!(
            cooldown_map.update("test_key_long_2", cooldown_interval_long),
            CooldownStatus::Ready
        );
        assert_eq!(cooldown_map.0.len(), 2);
    }
}
