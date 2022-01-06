use std::collections::HashSet;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::time::Duration;

use rand::prelude::SliceRandom;

use crate::member::{self, ArtilleryMember, ArtilleryMemberState, ArtilleryStateChange};

pub struct ArtilleryMemberList {
    members: Vec<ArtilleryMember>,
    periodic_index: usize,
}

impl Debug for ArtilleryMemberList {
    fn fmt(&self, fmt: &mut Formatter) -> fmt::Result {
        fmt.debug_struct("ArtilleryEpidemic")
            .field("members", &self.members)
            .finish()
    }
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

    pub fn current_node_id(&self) -> String {
        for member in self.members.iter() {
            if member.is_current() {
                return member.node_id();
            }
        }
        panic!("Could not find current node as registered member");
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

    // make sure node id is set correctly for the host (in case it changes)
    pub fn ensure_node_id(&mut self, src_addr: SocketAddr, node_id: &str) {
        for member in &mut self.members {
            if member.remote_host() == Some(src_addr) {
                member.node_id = node_id.to_string();
            }
        }
    }
    pub fn mark_node_alive(
        &mut self,
        src_addr: &SocketAddr,
        node_id: String,
    ) -> Option<ArtilleryMember> {
        self.ensure_node_id(*src_addr, &node_id);
        // remove self duplicates
        let my_node_id = self.current_node_id();
        self.members
            .retain(|member| !(member.node_id() == my_node_id && member.remote_host().is_some()));
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

    fn find_member<'a>(
        members: &'a mut [ArtilleryMember],
        node_id: &str,
        addr: SocketAddr,
    ) -> Option<&'a mut ArtilleryMember> {
        let pos_matching_node_id: Option<usize> = members
            .iter()
            .position(|member| member.node_id() == node_id);
        let pos_matching_address: Option<usize> = members
            .iter()
            .position(|member| member.remote_host() == Some(addr));
        let chosen_position: usize = pos_matching_node_id.or(pos_matching_address)?;
        Some(&mut members[chosen_position])
    }

    pub fn apply_state_changes(
        &mut self,
        state_changes: Vec<ArtilleryStateChange>,
        from: &SocketAddr,
    ) -> (Vec<ArtilleryMember>, Vec<ArtilleryMember>) {
        let mut current_members = self.members.clone();

        let mut changed_nodes = Vec::new();
        let mut new_nodes = Vec::new();

        let my_node_id = self.current_node_id();

        for state_change in state_changes {
            let member_change = state_change.member();

            if member_change.node_id() == my_node_id {
                if member_change.state() != ArtilleryMemberState::Alive {
                    let myself = self.reincarnate_self();
                    changed_nodes.push(myself.clone());
                }
            } else if let Some(existing_member) = Self::find_member(
                &mut current_members,
                &member_change.node_id(),
                member_change.remote_host().unwrap_or(*from),
            ) {
                let update_member =
                    member::most_uptodate_member_data(member_change, existing_member).clone();
                let new_host = update_member
                    .remote_host()
                    .or_else(|| existing_member.remote_host())
                    .unwrap();
                let update_member = update_member.member_by_changing_host(new_host);

                if update_member.state() != existing_member.state() {
                    existing_member.node_id = member_change.node_id();
                    existing_member.set_state(update_member.state());
                    existing_member.incarnation_number = update_member.incarnation_number;
                    if let Some(host) = update_member.remote_host() {
                        existing_member.set_remote_host(host);
                    }
                    changed_nodes.push(update_member);
                }
            } else if member_change.state() != ArtilleryMemberState::Down
                && member_change.state() != ArtilleryMemberState::Left
            {
                let new_host = member_change.remote_host().unwrap_or(*from);
                let new_member = member_change.member_by_changing_host(new_host);

                current_members.push(new_member.clone());
                new_nodes.push(new_member);
            }
        }

        self.members = current_members
            .into_iter()
            .filter(|member| {
                member.state() != ArtilleryMemberState::Down
                    && member.state() != ArtilleryMemberState::Left
            })
            .collect::<Vec<_>>();

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

    pub fn remove_member(&mut self, id: &str) {
        self.members.retain(|member| member.node_id() != id)
    }

    /// `get_member` will return artillery member if the given uuid is matched with any of the
    /// member in the cluster.
    pub fn get_member(&self, id: &str) -> Option<ArtilleryMember> {
        self.members.iter().find(|&m| m.node_id() == *id).cloned()
    }
}
