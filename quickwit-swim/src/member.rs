use std::cmp::Ordering;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use serde::*;

#[derive(Serialize, Deserialize, Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Copy)]
pub enum ArtilleryMemberState {
    /// Looks alive as in the original paper
    #[serde(rename = "a")]
    Alive,
    /// Suspect from the given node
    #[serde(rename = "s")]
    Suspect,
    /// AKA `Confirm` in the original paper
    #[serde(rename = "d")]
    Down,
    /// Left the cluster
    #[serde(rename = "l")]
    Left,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ArtilleryMember {
    #[serde(rename = "h")]
    node_id: String,
    #[serde(rename = "r")]
    remote_host: Option<SocketAddr>,
    #[serde(rename = "i")]
    incarnation_number: u64,
    #[serde(rename = "m")]
    member_state: ArtilleryMemberState,
    #[serde(rename = "t", skip, default = "Instant::now")]
    last_state_change: Instant,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct ArtilleryStateChange {
    member: ArtilleryMember,
}

impl ArtilleryMember {
    pub fn new(
        host_id: String,
        remote_host: SocketAddr,
        incarnation_number: u64,
        known_state: ArtilleryMemberState,
    ) -> Self {
        ArtilleryMember {
            node_id: host_id,
            remote_host: Some(remote_host),
            incarnation_number,
            member_state: known_state,
            last_state_change: Instant::now(),
        }
    }

    pub fn current(node_id: String) -> Self {
        ArtilleryMember {
            node_id,
            remote_host: None,
            incarnation_number: 0,
            member_state: ArtilleryMemberState::Alive,
            last_state_change: Instant::now(),
        }
    }

    pub fn node_id(&self) -> String {
        self.node_id.clone()
    }

    pub fn remote_host(&self) -> Option<SocketAddr> {
        self.remote_host
    }

    pub fn is_remote(&self) -> bool {
        self.remote_host.is_some()
    }

    pub fn is_current(&self) -> bool {
        self.remote_host.is_none()
    }

    pub fn state_change_older_than(&self, duration: Duration) -> bool {
        self.last_state_change + duration < Instant::now()
    }

    pub fn state(&self) -> ArtilleryMemberState {
        self.member_state
    }

    pub fn set_state(&mut self, state: ArtilleryMemberState) {
        if self.member_state != state {
            self.member_state = state;
            self.last_state_change = Instant::now();
        }
    }

    pub fn member_by_changing_host(&self, remote_host: SocketAddr) -> ArtilleryMember {
        ArtilleryMember {
            remote_host: Some(remote_host),
            ..self.clone()
        }
    }

    pub fn reincarnate(&mut self) {
        self.incarnation_number += 1
    }
}

impl ArtilleryStateChange {
    pub fn new(member: ArtilleryMember) -> ArtilleryStateChange {
        ArtilleryStateChange { member }
    }

    pub fn member(&self) -> &ArtilleryMember {
        &self.member
    }

    pub fn update(&mut self, member: ArtilleryMember) {
        self.member = member
    }
}

impl PartialOrd for ArtilleryMember {
    fn partial_cmp(&self, rhs: &ArtilleryMember) -> Option<Ordering> {
        let t1 = (
            self.node_id.as_bytes(),
            format!("{:?}", self.remote_host),
            self.incarnation_number,
            self.member_state,
        );

        let t2 = (
            rhs.node_id.as_bytes(),
            format!("{:?}", rhs.remote_host),
            rhs.incarnation_number,
            rhs.member_state,
        );

        t1.partial_cmp(&t2)
    }
}

impl Ord for ArtilleryMember {
    fn cmp(&self, rhs: &ArtilleryMember) -> Ordering {
        self.partial_cmp(rhs).unwrap()
    }
}

impl Debug for ArtilleryMember {
    fn fmt(&self, fmt: &mut Formatter) -> fmt::Result {
        fmt.debug_struct("ArtilleryMember")
            .field("incarnation_number", &self.incarnation_number)
            .field("host", &self.node_id)
            .field("state", &self.member_state)
            .field(
                "drift_time_ms",
                &self.last_state_change.elapsed().as_millis(),
            )
            .field(
                "remote_host",
                &self
                    .remote_host
                    .map_or(String::from("(current)"), |r| format!("{}", r))
                    .as_str(),
            )
            .finish()
    }
}

pub fn most_uptodate_member_data<'a>(
    lhs: &'a ArtilleryMember,
    rhs: &'a ArtilleryMember,
) -> &'a ArtilleryMember {
    // Don't apply clippy here.
    // It's important bit otherwise we won't understand.
    #![allow(clippy::match_same_arms)]

    let lhs_overrides = match (
        lhs.member_state,
        lhs.incarnation_number,
        rhs.member_state,
        rhs.incarnation_number,
    ) {
        (ArtilleryMemberState::Alive, i, ArtilleryMemberState::Suspect, j) => i > j,
        (ArtilleryMemberState::Alive, i, ArtilleryMemberState::Alive, j) => i > j,
        (ArtilleryMemberState::Suspect, i, ArtilleryMemberState::Suspect, j) => i > j,
        (ArtilleryMemberState::Suspect, i, ArtilleryMemberState::Alive, j) => i >= j,
        (ArtilleryMemberState::Down, _, ArtilleryMemberState::Alive, _) => true,
        (ArtilleryMemberState::Down, _, ArtilleryMemberState::Suspect, _) => true,
        (ArtilleryMemberState::Left, _, _, _) => true,
        _ => false,
    };

    if lhs_overrides {
        lhs
    } else {
        rhs
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;
    use std::time::{Duration, Instant};

    use uuid;

    use super::{ArtilleryMember, ArtilleryMemberState};

    #[test]
    fn test_member_encode_decode() {
        let member = ArtilleryMember {
            node_id: uuid::Uuid::new_v4().to_string(),
            remote_host: Some(FromStr::from_str("127.0.0.1:1337").unwrap()),
            incarnation_number: 123,
            member_state: ArtilleryMemberState::Alive,
            last_state_change: Instant::now() - Duration::from_secs(3600),
        };
        let encoded = bincode::serialize(&member).unwrap();
        let decoded: ArtilleryMember = bincode::deserialize(&encoded).unwrap();
        let encoded_again = bincode::serialize(&decoded).unwrap();
        assert_eq!(encoded, encoded_again);
    }
}
