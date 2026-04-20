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

//! Resolves the service/env/version tags for a connection and applies the
//! NSX direction-fixup heuristics before the parsers run.
//!
//! This is a Rust port of dd-source's
//! `domains/quickhouse/apps/byoc-usm-stats/internal/resolver/` package.
//!
//! The agent-payload tag buffer is version-prefixed (see
//! `github.com/DataDog/agent-payload/v5/process/tags.go` and `tags_v2.go`):
//!
//! * V1 buffer: `[version=1][...group1...][...group2...]` where each group is `[u16 numTags
//!   LE][(u16 tagLen LE, tag bytes)...]`.
//! * V2/V3 buffer: preamble `[version][u32 footer_pos LE]`, followed by a blob of `(u16 tagLen LE,
//!   tag bytes)` entries, and a footer per encoded group of `[u16 numTags][(u32 tag_position)...]`.
//!
//! Connections carry an integer offset into the appropriate buffer
//! (`encoded_tags` for container/host tags, `encoded_connections_tags`
//! for per-connection tags).

use std::collections::HashSet;

use crate::protos::process::{
    CollectorConnections, Connection, ConnectionDirection, ConnectionType,
};

/// Walks an encoded-tag buffer at `tag_index` and yields each tag slice
/// until the caller returns `false` or the group ends. Handles both V1 and
/// V2/V3 buffer formats. Tag bytes are borrowed from `buffer`.
pub(super) fn iterate_tags<'a, F>(buffer: &'a [u8], tag_index: i32, mut cb: F)
where F: FnMut(&'a [u8]) -> bool {
    if buffer.is_empty() || tag_index < 0 {
        return;
    }
    match buffer[0] {
        1 => iterate_v1(buffer, tag_index as usize, &mut cb),
        2 | 3 => iterate_v2(buffer, tag_index as usize, &mut cb),
        _ => {}
    }
}

fn iterate_v1<'a, F>(buffer: &'a [u8], tag_index: usize, cb: &mut F)
where F: FnMut(&'a [u8]) -> bool {
    if tag_index >= buffer.len() {
        return;
    }
    let group = &buffer[tag_index..];
    if group.len() < 2 {
        return;
    }
    let num_tags = u16::from_le_bytes([group[0], group[1]]) as usize;
    let mut cursor = 2;
    for _ in 0..num_tags {
        if cursor + 2 > group.len() {
            return;
        }
        let tag_len = u16::from_le_bytes([group[cursor], group[cursor + 1]]) as usize;
        cursor += 2;
        if cursor + tag_len > group.len() {
            return;
        }
        let tag = &group[cursor..cursor + tag_len];
        if !cb(tag) {
            return;
        }
        cursor += tag_len;
    }
}

fn iterate_v2<'a, F>(buffer: &'a [u8], tag_index: usize, cb: &mut F)
where F: FnMut(&'a [u8]) -> bool {
    if buffer.len() < 5 {
        return;
    }
    let footer_position = u32::from_le_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]) as usize;
    let footer_start = footer_position.saturating_add(tag_index);
    if footer_start >= buffer.len() {
        return;
    }
    let footer = &buffer[footer_start..];
    if footer.len() < 2 {
        return;
    }
    let num_tags = u16::from_le_bytes([footer[0], footer[1]]) as usize;
    let mut cursor = 2;
    for _ in 0..num_tags {
        if cursor + 4 > footer.len() {
            return;
        }
        let tag_position = u32::from_le_bytes([
            footer[cursor],
            footer[cursor + 1],
            footer[cursor + 2],
            footer[cursor + 3],
        ]) as usize;
        cursor += 4;
        if tag_position + 2 > buffer.len() {
            continue;
        }
        let tag_len = u16::from_le_bytes([buffer[tag_position], buffer[tag_position + 1]]) as usize;
        let start = tag_position + 2;
        let end = start + tag_len;
        if end > buffer.len() {
            continue;
        }
        if !cb(&buffer[start..end]) {
            return;
        }
    }
}

/// Looks up the first tag matching `key:` and returns the value portion.
pub(super) fn find_tag(buffer: &[u8], tag_index: i32, key: &str) -> Option<String> {
    let mut found: Option<String> = None;
    let prefix_len = key.len() + 1; // "<key>:"
    iterate_tags(buffer, tag_index, |tag| {
        if tag.len() <= prefix_len {
            return true;
        }
        if &tag[..key.len()] != key.as_bytes() || tag[key.len()] != b':' {
            return true;
        }
        if let Ok(value) = std::str::from_utf8(&tag[prefix_len..]) {
            found = Some(value.to_string());
            return false;
        }
        true
    });
    found
}

/// Resolves the service name for a connection using the Go sidecar's
/// precedence chain: process tags → container tags → host tags →
/// container-id fallback → hostname fallback.
pub(super) fn resolve_service(cc: &CollectorConnections, conn: &Connection) -> String {
    // Priority 1: process-level connection tags (Go: `if c.TagsIdx > 0`).
    if conn.tags_idx > 0
        && let Some(v) = find_tag(&cc.encoded_connections_tags, conn.tags_idx, "service")
    {
        return v;
    }
    // Priority 2: container tags (Go: `if c.LocalContainerTagsIndex >= 0`).
    if conn.local_container_tags_index >= 0
        && let Some(v) = find_tag(&cc.encoded_tags, conn.local_container_tags_index, "service")
    {
        return v;
    }
    // Priority 3: host tags (Go: `if cc.HostTagsIndex > 0`).
    if cc.host_tags_index > 0
        && let Some(v) = find_tag(&cc.encoded_tags, cc.host_tags_index, "service")
    {
        return v;
    }
    // Fallback: container ID (from laddr, else container_for_pid) → hostname.
    let container_id = conn
        .laddr
        .as_ref()
        .map(|a| a.container_id.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from)
        .or_else(|| cc.container_for_pid.get(&conn.pid).cloned());
    if let Some(cid) = container_id
        && !cid.is_empty()
    {
        let take = cid.len().min(12);
        return format!("container:{}", &cid[..take]);
    }
    cc.host_name.clone()
}

/// Resolves the env tag for a connection with the same precedence chain
/// as `resolve_service` but no fallback (returns `None` if absent).
pub(super) fn resolve_env(cc: &CollectorConnections, conn: &Connection) -> Option<String> {
    if conn.tags_idx > 0
        && let Some(v) = find_tag(&cc.encoded_connections_tags, conn.tags_idx, "env")
    {
        return Some(v);
    }
    if conn.local_container_tags_index >= 0
        && let Some(v) = find_tag(&cc.encoded_tags, conn.local_container_tags_index, "env")
    {
        return Some(v);
    }
    if cc.host_tags_index > 0
        && let Some(v) = find_tag(&cc.encoded_tags, cc.host_tags_index, "env")
    {
        return Some(v);
    }
    None
}

/// Applies NSX's two direction-fixup heuristics in place.
pub(super) fn fixup_directions(cc: &mut CollectorConnections) {
    let mut listening: HashSet<(i32, i32, i32, u32)> = HashSet::new();
    for conn in &cc.connections {
        if conn.direction != ConnectionDirection::Incoming as i32 {
            continue;
        }
        if conn.r#type != ConnectionType::Tcp as i32 {
            continue;
        }
        let Some(laddr) = conn.laddr.as_ref() else {
            continue;
        };
        listening.insert((conn.r#type, laddr.port, conn.pid, conn.net_ns));
    }

    for conn in &mut cc.connections {
        if conn.direction == ConnectionDirection::Outgoing as i32
            && conn.r#type == ConnectionType::Tcp as i32
            && let Some(laddr) = conn.laddr.as_ref()
        {
            let key = (conn.r#type, laddr.port, conn.pid, conn.net_ns);
            if listening.contains(&key) {
                conn.direction = ConnectionDirection::Incoming as i32;
            }
        }

        let has_dns = !conn.dns_stats_by_domain.is_empty()
            || !conn.dns_stats_by_domain_by_query_type.is_empty();
        if has_dns
            && conn.r#type == ConnectionType::Udp as i32
            && let (Some(laddr), Some(raddr)) = (conn.laddr.as_ref(), conn.raddr.as_ref())
            && laddr.port >= 32768
            && raddr.port < 1024
        {
            conn.direction = ConnectionDirection::Outgoing as i32;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protos::process::Addr;

    /// Builds a V1 buffer containing a single tag group at offset 1 (the
    /// byte after the version marker).
    fn v1_buffer(tags: &[&str]) -> Vec<u8> {
        let mut buf = vec![1_u8];
        buf.extend_from_slice(&(tags.len() as u16).to_le_bytes());
        for t in tags {
            buf.extend_from_slice(&(t.len() as u16).to_le_bytes());
            buf.extend_from_slice(t.as_bytes());
        }
        buf
    }

    /// Builds a V1 buffer containing two groups. Returns (buffer, group2_offset).
    fn v1_two_groups(a: &[&str], b: &[&str]) -> (Vec<u8>, i32) {
        let mut buf = vec![1_u8];
        let push_group = |buf: &mut Vec<u8>, tags: &[&str]| {
            buf.extend_from_slice(&(tags.len() as u16).to_le_bytes());
            for t in tags {
                buf.extend_from_slice(&(t.len() as u16).to_le_bytes());
                buf.extend_from_slice(t.as_bytes());
            }
        };
        push_group(&mut buf, a);
        let offset = buf.len() as i32;
        push_group(&mut buf, b);
        (buf, offset)
    }

    #[test]
    fn find_tag_v1_single_group() {
        let buf = v1_buffer(&["service:foo", "env:prod", "version:1.2.3"]);
        assert_eq!(find_tag(&buf, 1, "service"), Some("foo".to_string()));
        assert_eq!(find_tag(&buf, 1, "env"), Some("prod".to_string()));
        assert_eq!(find_tag(&buf, 1, "missing"), None);
    }

    #[test]
    fn find_tag_v1_respects_group_offsets() {
        let (buf, offset_b) = v1_two_groups(&["service:first"], &["service:second"]);
        assert_eq!(find_tag(&buf, 1, "service"), Some("first".to_string()));
        assert_eq!(
            find_tag(&buf, offset_b, "service"),
            Some("second".to_string())
        );
    }

    #[test]
    fn find_tag_negative_offset_returns_none() {
        let buf = v1_buffer(&["service:foo"]);
        assert_eq!(find_tag(&buf, -1, "service"), None);
    }

    #[test]
    fn find_tag_empty_buffer_returns_none() {
        assert_eq!(find_tag(&[], 1, "service"), None);
    }

    #[test]
    fn find_tag_truncated_buffer_returns_none() {
        let mut buf = v1_buffer(&["service:foo"]);
        buf.truncate(3); // drops most of the payload
        assert_eq!(find_tag(&buf, 1, "service"), None);
    }

    #[test]
    fn resolve_service_prefers_process_tags() {
        let cc = CollectorConnections {
            encoded_connections_tags: v1_buffer(&["service:proc"]),
            encoded_tags: v1_buffer(&["service:container"]),
            ..Default::default()
        };
        let conn = Connection {
            tags_idx: 1,
            local_container_tags_index: 1,
            ..Default::default()
        };
        assert_eq!(resolve_service(&cc, &conn), "proc");
    }

    #[test]
    fn resolve_service_falls_back_to_container_tags() {
        let cc = CollectorConnections {
            encoded_tags: v1_buffer(&["service:container"]),
            ..Default::default()
        };
        let conn = Connection {
            tags_idx: 0, // zero idx is ignored (Go: `> 0`)
            local_container_tags_index: 1,
            ..Default::default()
        };
        assert_eq!(resolve_service(&cc, &conn), "container");
    }

    #[test]
    fn resolve_service_host_tags_fallback() {
        let cc = CollectorConnections {
            encoded_tags: v1_buffer(&["service:hosted"]),
            host_tags_index: 1,
            ..Default::default()
        };
        let conn = Connection {
            tags_idx: -1,
            local_container_tags_index: -1,
            ..Default::default()
        };
        assert_eq!(resolve_service(&cc, &conn), "hosted");
    }

    #[test]
    fn resolve_service_container_id_fallback_from_laddr() {
        let cc = CollectorConnections {
            host_name: "host".into(),
            ..Default::default()
        };
        let conn = Connection {
            tags_idx: -1,
            local_container_tags_index: -1,
            laddr: Some(Addr {
                container_id: "abcdef1234567890".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(resolve_service(&cc, &conn), "container:abcdef123456");
    }

    #[test]
    fn resolve_service_container_id_fallback_from_pid_map() {
        let mut cc = CollectorConnections {
            host_name: "host".into(),
            ..Default::default()
        };
        cc.container_for_pid.insert(42, "deadbeefcafe".into());
        let conn = Connection {
            tags_idx: -1,
            local_container_tags_index: -1,
            pid: 42,
            laddr: Some(Addr::default()),
            ..Default::default()
        };
        assert_eq!(resolve_service(&cc, &conn), "container:deadbeefcafe");
    }

    #[test]
    fn resolve_service_hostname_fallback() {
        let cc = CollectorConnections {
            host_name: "my-host".into(),
            ..Default::default()
        };
        let conn = Connection {
            tags_idx: -1,
            local_container_tags_index: -1,
            ..Default::default()
        };
        assert_eq!(resolve_service(&cc, &conn), "my-host");
    }

    #[test]
    fn resolve_env_returns_none_without_fallback() {
        let cc = CollectorConnections::default();
        let conn = Connection {
            tags_idx: -1,
            local_container_tags_index: -1,
            ..Default::default()
        };
        assert_eq!(resolve_env(&cc, &conn), None);
    }

    #[test]
    fn fixup_flips_outgoing_tcp_matching_listening_port() {
        let mut cc = CollectorConnections::default();
        cc.connections.push(Connection {
            r#type: ConnectionType::Tcp as i32,
            direction: ConnectionDirection::Incoming as i32,
            pid: 1,
            net_ns: 10,
            laddr: Some(Addr {
                port: 8080,
                ..Default::default()
            }),
            ..Default::default()
        });
        cc.connections.push(Connection {
            r#type: ConnectionType::Tcp as i32,
            direction: ConnectionDirection::Outgoing as i32,
            pid: 1,
            net_ns: 10,
            laddr: Some(Addr {
                port: 8080,
                ..Default::default()
            }),
            ..Default::default()
        });
        fixup_directions(&mut cc);
        assert_eq!(
            cc.connections[1].direction,
            ConnectionDirection::Incoming as i32
        );
    }

    #[test]
    fn fixup_does_not_flip_mismatched_pid() {
        let mut cc = CollectorConnections::default();
        cc.connections.push(Connection {
            r#type: ConnectionType::Tcp as i32,
            direction: ConnectionDirection::Incoming as i32,
            pid: 1,
            laddr: Some(Addr {
                port: 8080,
                ..Default::default()
            }),
            ..Default::default()
        });
        cc.connections.push(Connection {
            r#type: ConnectionType::Tcp as i32,
            direction: ConnectionDirection::Outgoing as i32,
            pid: 2,
            laddr: Some(Addr {
                port: 8080,
                ..Default::default()
            }),
            ..Default::default()
        });
        fixup_directions(&mut cc);
        assert_eq!(
            cc.connections[1].direction,
            ConnectionDirection::Outgoing as i32
        );
    }

    #[test]
    fn fixup_flips_dns_udp_to_outgoing() {
        let mut cc = CollectorConnections::default();
        let mut conn = Connection {
            r#type: ConnectionType::Udp as i32,
            direction: ConnectionDirection::Incoming as i32,
            laddr: Some(Addr {
                port: 50000,
                ..Default::default()
            }),
            raddr: Some(Addr {
                port: 53,
                ..Default::default()
            }),
            ..Default::default()
        };
        conn.dns_stats_by_domain.insert(1, Default::default());
        cc.connections.push(conn);
        fixup_directions(&mut cc);
        assert_eq!(
            cc.connections[0].direction,
            ConnectionDirection::Outgoing as i32
        );
    }
}
