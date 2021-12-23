use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::Duration;

use rand::prelude::SliceRandom;

use crate::member::{self, ArtilleryMember, ArtilleryMemberState, ArtilleryStateChange};

pub struct ArtilleryMemberList {
    members: Vec<ArtilleryMember>,
    periodic_index: usize,
}

impl ArtilleryMemberList {
    pub fn new(current: ArtilleryMember) -> Self {
        ArtilleryMemberList {
            members: vec![current],
            periodic_index: 0,
        }
    }

    pub fn available_nodes(&self) -> Vec<ArtilleryMember> {
        self.members
            .iter()
            .filter(|m| m.state() != ArtilleryMemberState::Left)
            .cloned()
            .collect()
    }

    pub fn to_map(&self) -> HashMap<String, ArtilleryMember> {
        self.members
            .iter()
            .map(|m| (m.node_id(), (*m).clone()))
            .collect()
    }

    fn mut_myself(&mut self) -> &mut ArtilleryMember {
        for member in &mut self.members {
            if member.is_current() {
                return member;
            }
        }

        panic!("Could not find this instance as registered member");
    }

    pub fn reincarnate_self(&mut self) -> ArtilleryMember {
        let myself = self.mut_myself();
        myself.reincarnate();

        myself.clone()
    }

    pub fn leave(&mut self) -> ArtilleryMember {
        let myself = self.mut_myself();
        myself.set_state(ArtilleryMemberState::Left);
        myself.reincarnate();

        myself.clone()
    }

    pub fn next_random_member(&mut self) -> Option<ArtilleryMember> {
        if self.periodic_index == 0 {
            let mut rng = rand::thread_rng();
            self.members.shuffle(&mut rng);
        }

        let other_members: Vec<_> = self.members.iter().filter(|&m| m.is_remote()).collect();

        if other_members.is_empty() {
            None
        } else {
            self.periodic_index = (self.periodic_index + 1) % other_members.len();
            Some(other_members[self.periodic_index].clone())
        }
    }

    pub fn time_out_nodes(
        &mut self,
        expired_hosts: &HashSet<SocketAddr>,
    ) -> (Vec<ArtilleryMember>, Vec<ArtilleryMember>) {
        let mut suspect_members = Vec::new();
        let mut down_members = Vec::new();

        for member in &mut self.members {
            if let Some(remote_host) = member.remote_host() {
                if !expired_hosts.contains(&remote_host) {
                    continue;
                }

                match member.state() {
                    ArtilleryMemberState::Alive => {
                        member.set_state(ArtilleryMemberState::Suspect);
                        suspect_members.push(member.clone());
                    }
                    // TODO: Config suspect timeout
                    ArtilleryMemberState::Suspect
                        if member.state_change_older_than(Duration::from_secs(3)) =>
                    {
                        member.set_state(ArtilleryMemberState::Down);
                        down_members.push(member.clone());
                    }
                    ArtilleryMemberState::Suspect
                    | ArtilleryMemberState::Down
                    | ArtilleryMemberState::Left => {}
                }
            }
        }

        (suspect_members, down_members)
    }

    pub fn mark_node_alive(&mut self, src_addr: &SocketAddr) -> Option<ArtilleryMember> {
        for member in &mut self.members {
            if member.remote_host() == Some(*src_addr)
                && member.state() != ArtilleryMemberState::Alive
            {
                member.set_state(ArtilleryMemberState::Alive);

                return Some(member.clone());
            }
        }

        None
    }

    pub fn apply_state_changes(
        &mut self,
        state_changes: Vec<ArtilleryStateChange>,
        from: &SocketAddr,
    ) -> (Vec<ArtilleryMember>, Vec<ArtilleryMember>) {
        let mut current_members = self.to_map();

        let mut changed_nodes = Vec::new();
        let mut new_nodes = Vec::new();

        let my_host_key = self.mut_myself().node_id();

        for state_change in state_changes {
            let new_member_data = state_change.member();
            let old_member_data = current_members.entry(new_member_data.node_id());

            if new_member_data.node_id() == my_host_key {
                if new_member_data.state() != ArtilleryMemberState::Alive {
                    let myself = self.reincarnate_self();
                    changed_nodes.push(myself.clone());
                }
            } else {
                match old_member_data {
                    Entry::Occupied(mut entry) => {
                        let new_member =
                            member::most_uptodate_member_data(new_member_data, entry.get()).clone();
                        let new_host = new_member
                            .remote_host()
                            .or_else(|| entry.get().remote_host())
                            .unwrap();
                        let new_member = new_member.member_by_changing_host(new_host);

                        if new_member.state() != entry.get().state() {
                            entry.insert(new_member.clone());
                            changed_nodes.push(new_member);
                        }
                    }
                    Entry::Vacant(entry) => {
                        let new_host = new_member_data.remote_host().unwrap_or(*from);
                        let new_member = new_member_data.member_by_changing_host(new_host);

                        entry.insert(new_member.clone());
                        new_nodes.push(new_member);
                    }
                };
            }
        }

        self.members = current_members.values().cloned().collect();

        (new_nodes, changed_nodes)
    }

    /// Random ping enqueuing
    pub fn hosts_for_indirect_ping(
        &self,
        host_count: usize,
        target: &SocketAddr,
    ) -> Vec<SocketAddr> {
        let mut possible_members: Vec<_> = self
            .members
            .iter()
            .filter_map(|m| {
                if m.state() == ArtilleryMemberState::Alive
                    && m.is_remote()
                    && m.remote_host() != Some(*target)
                {
                    m.remote_host()
                } else {
                    None
                }
            })
            .collect();

        let mut rng = rand::thread_rng();
        possible_members.shuffle(&mut rng);
        possible_members.iter().take(host_count).cloned().collect()
    }

    pub fn has_member(&self, remote_host: &SocketAddr) -> bool {
        self.members
            .iter()
            .any(|m| m.remote_host() == Some(*remote_host))
    }

    pub fn add_member(&mut self, member: ArtilleryMember) {
        self.members.push(member)
    }

    /// `get_member` will return artillery member if the given uuid is matches with any of the
    /// member in the cluster.
    pub fn get_member(&self, id: &str) -> Option<ArtilleryMember> {
        let member: Vec<_> = self
            .members
            .iter()
            .filter(|&m| m.node_id() == *id)
            .collect();

        if member.is_empty() {
            return None;
        }
        Some(member[0].clone())
    }
}
