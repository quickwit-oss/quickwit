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
    CollectorConnections, Connection, ConnectionDirection, ConnectionType, EphemeralPortState,
};

/// Well-known DNS port. Traffic on this remote port is assumed to be DNS
/// regardless of UDP/TCP or the agent's direction heuristic.
const DNS_PORT: i32 = 53;
/// Upper bound (exclusive) of the "well-known" port range. DNS-fixup uses
/// this to distinguish server-side ports from ephemeral client ports.
const WELL_KNOWN_PORT_MAX: i32 = 1024;
/// Lower bound (inclusive) of the Linux ephemeral-port range on most
/// distros. Used as a fallback heuristic when the agent did not set
/// `is_local_port_ephemeral` explicitly.
const EPHEMERAL_PORT_MIN: i32 = 32768;
/// Length of the Docker "short" container-ID prefix we fall back to when no
/// service-name tag is resolvable. Matches what SaaS displays.
const CONTAINER_ID_PREFIX_LEN: usize = 12;

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

/// Tag source, mirrors NSX's `usm.TagSource`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TagSource {
    Process,
    Container,
    Host,
}

/// `(source, tag_name)` candidate in the service-naming priority list.
/// Order in [`SERVICE_CANDIDATES`] is significant — lowest index wins.
struct PrioritizedTag {
    source: TagSource,
    tag_name: &'static str,
}

/// Default service-naming priority list mirroring the Consul-configured
/// default in SaaS (`dd.usm.service_tags.tags_prioritization`). Ported from
/// the internal USM service-naming doc and `dd-go trace/usm`.
///
/// First non-empty match wins; lower index = higher priority.
///
/// Since BYOC has no Consul, this is the single default. If per-org
/// overrides ever become a requirement, this can grow into a configurable
/// list loaded from the pipeline config.
const SERVICE_CANDIDATES: &[PrioritizedTag] = &[
    PrioritizedTag {
        source: TagSource::Process,
        tag_name: "service",
    }, // DD_SERVICE env var
    PrioritizedTag {
        source: TagSource::Container,
        tag_name: "service",
    }, // container `service:` label
    PrioritizedTag {
        source: TagSource::Process,
        tag_name: "http.iis.subsite",
    }, // IIS (Windows)
    PrioritizedTag {
        source: TagSource::Process,
        tag_name: "http.iis.app_pool",
    }, // IIS (Windows)
    PrioritizedTag {
        source: TagSource::Container,
        tag_name: "app",
    },
    PrioritizedTag {
        source: TagSource::Container,
        tag_name: "short_image",
    },
    PrioritizedTag {
        source: TagSource::Container,
        tag_name: "kube_container_name",
    },
    PrioritizedTag {
        source: TagSource::Container,
        tag_name: "container_name",
    },
    PrioritizedTag {
        source: TagSource::Container,
        tag_name: "kube_deployment",
    },
    PrioritizedTag {
        source: TagSource::Container,
        tag_name: "kube_service",
    },
    PrioritizedTag {
        source: TagSource::Host,
        tag_name: "service",
    },
    PrioritizedTag {
        source: TagSource::Host,
        tag_name: "app",
    },
    PrioritizedTag {
        source: TagSource::Process,
        tag_name: "process_context",
    },
];

/// Resolves the service name for a connection using NSX's priority-list
/// algorithm: walk every tag in each source, check against
/// [`SERVICE_CANDIDATES`], keep the lowest-priority-index match. Falls back
/// to `container:<id prefix>` then to `cc.host_name` if no candidate matches
/// — matches the shape the agent-side sidecar produces in the empty case.
pub(super) fn resolve_service(cc: &CollectorConnections, conn: &Connection) -> String {
    let mut best: Option<(usize, String)> = None;

    let mut consider = |source: TagSource, buffer: &[u8], tag_index: i32| {
        iterate_tags(buffer, tag_index, |tag| {
            let Some((name, value)) = split_tag(tag) else {
                return true;
            };
            if value.is_empty() {
                return true;
            }
            for (idx, candidate) in SERVICE_CANDIDATES.iter().enumerate() {
                if candidate.source != source || candidate.tag_name != name {
                    continue;
                }
                match best {
                    Some((best_idx, _)) if best_idx <= idx => {}
                    _ => best = Some((idx, value.to_string())),
                }
                break;
            }
            // Top priority (idx == 0) wins immediately; short-circuit.
            !matches!(best, Some((0, _)))
        });
    };

    // Process tags live in `encoded_connections_tags` at `conn.tags_idx`;
    // container + host tags live in `encoded_tags` at their respective
    // indices. (Cf. agent-payload `GetConnectionsTags` vs `GetTags`.)
    if conn.tags_idx > 0 {
        consider(
            TagSource::Process,
            &cc.encoded_connections_tags,
            conn.tags_idx,
        );
    }
    if conn.local_container_tags_index >= 0 {
        consider(
            TagSource::Container,
            &cc.encoded_tags,
            conn.local_container_tags_index,
        );
    }
    if cc.host_tags_index > 0 {
        consider(TagSource::Host, &cc.encoded_tags, cc.host_tags_index);
    }

    if let Some((_, value)) = best {
        return value;
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
        return format!(
            "container:{}",
            truncate_at_char_boundary(&cid, CONTAINER_ID_PREFIX_LEN)
        );
    }
    cc.host_name.clone()
}

/// Truncates `s` to at most `max_bytes` bytes without splitting a UTF-8
/// character. Container IDs are hex in practice, but `Connection.laddr.container_id`
/// is an arbitrary proto3 `string` (valid UTF-8), so a multi-byte char at the
/// boundary would panic a raw `&s[..max_bytes]`.
fn truncate_at_char_boundary(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Splits `key:value` on the first colon. Returns `None` if no colon.
fn split_tag(tag: &[u8]) -> Option<(&str, &str)> {
    let sep = tag.iter().position(|&b| b == b':')?;
    let name = std::str::from_utf8(&tag[..sep]).ok()?;
    let value = std::str::from_utf8(&tag[sep + 1..]).ok()?;
    Some((name, value))
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
                // Don't also evaluate the DNS heuristic on this connection —
                // the listening-match flip already decided it.
                continue;
            }
        }

        if is_probably_dns(conn) {
            conn.direction = ConnectionDirection::Outgoing as i32;
        }
    }
}

/// Matches the Go sidecar's DNS heuristic: flip a connection to "outgoing"
/// if ANY of the following holds.
///
/// 1. `raddr.port == 53` (protocol-agnostic).
/// 2. The connection carries DNS stats in any of its three DNS maps (`dns_stats_by_domain`,
///    `dns_stats_by_domain_by_query_type`, or `dns_count_by_rcode`).
/// 3. UDP + local port marked ephemeral by the agent + `raddr.port < 1024`.
///
/// Reference: `byoc-usm-stats/internal/resolver/direction.go::fixupDirection`.
fn is_probably_dns(conn: &Connection) -> bool {
    let Some(raddr) = conn.raddr.as_ref() else {
        return false;
    };
    if raddr.port == DNS_PORT {
        return true;
    }
    if has_dns_stats(conn) {
        return true;
    }
    if conn.r#type == ConnectionType::Udp as i32
        && conn.is_local_port_ephemeral == EphemeralPortState::EphemeralTrue as i32
        && raddr.port < WELL_KNOWN_PORT_MAX
    {
        return true;
    }
    // Fallback ephemeral heuristic for agents that don't set
    // `is_local_port_ephemeral`: UDP + laddr in the Linux ephemeral range +
    // raddr in the well-known range. Keeps parity with older agent payloads
    // that pre-date the explicit ephemeral-port flag.
    if conn.r#type == ConnectionType::Udp as i32
        && let Some(laddr) = conn.laddr.as_ref()
        && laddr.port >= EPHEMERAL_PORT_MIN
        && raddr.port < WELL_KNOWN_PORT_MAX
    {
        return true;
    }
    false
}

fn has_dns_stats(conn: &Connection) -> bool {
    !conn.dns_stats_by_domain.is_empty()
        || !conn.dns_stats_by_domain_by_query_type.is_empty()
        || !conn.dns_count_by_rcode.is_empty()
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
    fn resolve_service_picks_kube_deployment_over_nothing_else() {
        // Only orchestrator tags present — no `service:` anywhere.
        let cc = CollectorConnections {
            encoded_tags: v1_buffer(&["kube_deployment:my-app", "pod_name:my-app-abc"]),
            ..Default::default()
        };
        let conn = Connection {
            local_container_tags_index: 1,
            ..Default::default()
        };
        assert_eq!(resolve_service(&cc, &conn), "my-app");
    }

    #[test]
    fn resolve_service_prefers_service_over_kube_deployment() {
        let cc = CollectorConnections {
            encoded_tags: v1_buffer(&["service:real-svc", "kube_deployment:my-deploy"]),
            ..Default::default()
        };
        let conn = Connection {
            local_container_tags_index: 1,
            ..Default::default()
        };
        assert_eq!(resolve_service(&cc, &conn), "real-svc");
    }

    #[test]
    fn resolve_service_prefers_process_service_over_container_kube_deployment() {
        // process:service (priority 0) beats container:kube_deployment (priority 8).
        let cc = CollectorConnections {
            encoded_connections_tags: v1_buffer(&["service:from-env"]),
            encoded_tags: v1_buffer(&["kube_deployment:from-k8s"]),
            ..Default::default()
        };
        let conn = Connection {
            tags_idx: 1,
            local_container_tags_index: 1,
            ..Default::default()
        };
        assert_eq!(resolve_service(&cc, &conn), "from-env");
    }

    #[test]
    fn resolve_service_container_app_beats_kube_deployment() {
        // Priority order: container:app (5) < container:kube_deployment (8).
        let cc = CollectorConnections {
            encoded_tags: v1_buffer(&["app:from-app", "kube_deployment:from-deploy"]),
            ..Default::default()
        };
        let conn = Connection {
            local_container_tags_index: 1,
            ..Default::default()
        };
        assert_eq!(resolve_service(&cc, &conn), "from-app");
    }

    #[test]
    fn resolve_service_container_short_image_used_when_nothing_better() {
        let cc = CollectorConnections {
            encoded_tags: v1_buffer(&["short_image:nginx", "image_name:docker.io/nginx"]),
            ..Default::default()
        };
        let conn = Connection {
            local_container_tags_index: 1,
            ..Default::default()
        };
        // image_name is not in the priority list; short_image is.
        assert_eq!(resolve_service(&cc, &conn), "nginx");
    }

    #[test]
    fn resolve_service_kube_service_lowest_container_priority() {
        // Only kube_service — last of the container entries in the list.
        let cc = CollectorConnections {
            encoded_tags: v1_buffer(&["kube_service:backend-svc"]),
            ..Default::default()
        };
        let conn = Connection {
            local_container_tags_index: 1,
            ..Default::default()
        };
        assert_eq!(resolve_service(&cc, &conn), "backend-svc");
    }

    #[test]
    fn resolve_service_host_tags_only_used_when_no_process_or_container_match() {
        let cc = CollectorConnections {
            encoded_tags: v1_buffer(&["service:from-host"]),
            host_tags_index: 1,
            ..Default::default()
        };
        let conn = Connection {
            tags_idx: -1,
            local_container_tags_index: -1,
            ..Default::default()
        };
        assert_eq!(resolve_service(&cc, &conn), "from-host");
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

    #[test]
    fn fixup_flips_tcp_to_dns_port() {
        // Go's heuristic flips DNS regardless of protocol: raddr.port == 53
        // alone is enough. TCP DNS (AXFR, big responses) must flip too.
        let mut cc = CollectorConnections::default();
        cc.connections.push(Connection {
            r#type: ConnectionType::Tcp as i32,
            direction: ConnectionDirection::Incoming as i32,
            laddr: Some(Addr {
                port: 12345,
                ..Default::default()
            }),
            raddr: Some(Addr {
                port: 53,
                ..Default::default()
            }),
            ..Default::default()
        });
        fixup_directions(&mut cc);
        assert_eq!(
            cc.connections[0].direction,
            ConnectionDirection::Outgoing as i32
        );
    }

    #[test]
    fn fixup_flips_on_dns_count_by_rcode_alone() {
        // Go also considers `dns_count_by_rcode` when testing for DNS.
        // A connection that carries that map but none of the other two
        // must still flip.
        let mut cc = CollectorConnections::default();
        let mut conn = Connection {
            r#type: ConnectionType::Udp as i32,
            direction: ConnectionDirection::Incoming as i32,
            laddr: Some(Addr {
                port: 12345,
                ..Default::default()
            }),
            raddr: Some(Addr {
                port: 12345,
                ..Default::default()
            }),
            ..Default::default()
        };
        conn.dns_count_by_rcode.insert(0, 1);
        cc.connections.push(conn);
        fixup_directions(&mut cc);
        assert_eq!(
            cc.connections[0].direction,
            ConnectionDirection::Outgoing as i32
        );
    }

    #[test]
    fn fixup_flips_udp_with_explicit_ephemeral_flag() {
        // Connection has laddr.port below the 32768 fallback threshold, but
        // the agent marked it ephemeral explicitly. Go uses the flag; we
        // match.
        let mut cc = CollectorConnections::default();
        cc.connections.push(Connection {
            r#type: ConnectionType::Udp as i32,
            direction: ConnectionDirection::Incoming as i32,
            is_local_port_ephemeral: EphemeralPortState::EphemeralTrue as i32,
            laddr: Some(Addr {
                port: 20000,
                ..Default::default()
            }),
            raddr: Some(Addr {
                port: 500,
                ..Default::default()
            }),
            ..Default::default()
        });
        fixup_directions(&mut cc);
        assert_eq!(
            cc.connections[0].direction,
            ConnectionDirection::Outgoing as i32
        );
    }

    #[test]
    fn truncate_at_char_boundary_handles_multibyte() {
        // Pure-ASCII container id: normal truncation.
        assert_eq!(
            truncate_at_char_boundary("abcdef1234567890", 12),
            "abcdef123456"
        );
        // Shorter than limit: returned as-is.
        assert_eq!(truncate_at_char_boundary("abc", 12), "abc");
        // Multi-byte boundary: '€' is 3 bytes. Naive `&s[..7]` would panic;
        // this helper should back up to the previous char boundary.
        let s = "abcd€xyz";
        let out = truncate_at_char_boundary(s, 7);
        assert!(s.starts_with(out), "{out:?} must be a prefix of {s:?}");
        assert!(!out.is_empty());
    }

    #[test]
    fn fixup_does_not_flip_plain_udp() {
        // UDP with no DNS markers and non-DNS ports — no fixup.
        let mut cc = CollectorConnections::default();
        cc.connections.push(Connection {
            r#type: ConnectionType::Udp as i32,
            direction: ConnectionDirection::Incoming as i32,
            laddr: Some(Addr {
                port: 1234,
                ..Default::default()
            }),
            raddr: Some(Addr {
                port: 5678,
                ..Default::default()
            }),
            ..Default::default()
        });
        fixup_directions(&mut cc);
        assert_eq!(
            cc.connections[0].direction,
            ConnectionDirection::Incoming as i32
        );
    }
}
