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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use tracing::debug;

#[derive(Clone, Default)]
pub struct KillSwitch {
    inner: Arc<Inner>,
}

struct Inner {
    alive: AtomicBool,
    children: Mutex<Vec<Weak<Inner>>>,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            alive: AtomicBool::new(true),
            children: Mutex::default(),
        }
    }
}

fn garbage_collect(children: &mut Vec<Weak<Inner>>) {
    let mut i = 0;
    while i < children.len() {
        if Weak::strong_count(&children[i]) == 0 {
            children.swap_remove(i);
        } else {
            i += 1;
        }
    }
}

impl KillSwitch {
    pub fn is_alive(&self) -> bool {
        self.inner.alive.load(Ordering::Relaxed)
    }

    pub fn is_dead(&self) -> bool {
        !self.is_alive()
    }

    pub fn kill(&self) {
        self.inner.kill();
    }

    // Creates a child killswitch.
    //
    // If the parent kill switch is dead to begin with, the child will be dead too.
    pub fn child(&self) -> KillSwitch {
        let mut lock = self.inner.children.lock().unwrap();
        let child_inner = Inner {
            alive: AtomicBool::new(self.is_alive()),
            ..Default::default()
        };
        garbage_collect(&mut lock);
        let child_inner_arc = Arc::new(child_inner);
        lock.push(Arc::downgrade(&child_inner_arc));
        KillSwitch {
            inner: child_inner_arc,
        }
    }
}

impl Inner {
    pub fn kill(&self) {
        debug!("kill-switch-activated");
        self.alive.store(false, Ordering::Relaxed);
        let mut lock = self.children.lock().unwrap();
        for weak in lock.drain(..) {
            if let Some(inner) = weak.upgrade() {
                inner.kill();
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::KillSwitch;

    #[test]
    fn test_kill_switch() {
        let kill_switch = KillSwitch::default();
        assert!(kill_switch.is_alive());
        assert!(!kill_switch.is_dead());
        kill_switch.kill();
        assert!(!kill_switch.is_alive());
        assert!(kill_switch.is_dead());
        kill_switch.kill();
        assert!(!kill_switch.is_alive());
        assert!(kill_switch.is_dead());
    }

    #[test]
    fn test_kill_switch_child() {
        let kill_switch = KillSwitch::default();
        let child_kill_switch = kill_switch.child();
        let child_kill_switch2 = kill_switch.child();
        assert!(child_kill_switch.is_alive());
        assert!(child_kill_switch2.is_alive());
        kill_switch.kill();
        assert!(child_kill_switch.is_dead());
        assert!(child_kill_switch2.is_dead());
    }

    #[test]
    fn test_kill_switch_grandchildren() {
        let kill_switch = KillSwitch::default();
        let child_kill_switch = kill_switch.child();
        let grandchild_kill_switch = child_kill_switch.child();
        assert!(kill_switch.is_alive());
        assert!(child_kill_switch.is_alive());
        assert!(grandchild_kill_switch.is_alive());
        kill_switch.kill();
        assert!(kill_switch.is_dead());
        assert!(child_kill_switch.is_dead());
        assert!(grandchild_kill_switch.is_dead());
    }

    #[test]
    fn test_kill_switch_to_quoque_me_fili() {
        let kill_switch = KillSwitch::default();
        let child_kill_switch = kill_switch.child();
        assert!(kill_switch.is_alive());
        assert!(child_kill_switch.is_alive());
        child_kill_switch.kill();
        assert!(kill_switch.is_alive());
        assert!(child_kill_switch.is_dead());
    }
}
