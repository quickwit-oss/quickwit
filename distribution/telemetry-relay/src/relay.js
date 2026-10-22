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

const MAX_BODY_NUM_BYTES = 16 * 1024;
// Quickwit queues at most `MAX_NUM_EVENTS_IN_QUEUE` (10) events per request.
const MAX_NUM_EVENTS = 10;
const MAX_NUM_NAMES = 32;
const EVENT_TYPES = new Set([
  "run_command",
  "end_command",
  "running",
  "ui_index_page_load",
  "install",
]);
const ACTIVITY_EVENT_TYPES = new Set(["end_command", "running"]);
const API_KINDS = new Set([
  "elasticsearch_search",
  "elasticsearch_ingest",
  "elasticsearch_other",
  "native_search",
  "native_ingest",
  "otlp",
  "jaeger",
]);
const MAX_SCHEMA_VERSION = 2;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const MD5_HEX_RE = /^[0-9a-f]{32}$/;
const NAME_RE = /^[a-z0-9_-]{1,64}$/;
const VERSION_RE = /^[0-9A-Za-z.+-]{1,64}$/;
const BUCKET_RE = /^[0-9A-Za-z<+-]{1,16}$/;
const COMMIT_RE = /^([0-9a-f]{7,40}|unknown)$/;
const TARGET_RE = /^[a-z0-9_.-]{1,64}$/;
const COUNTRY_HEADER = "cloudfront-viewer-country";
const CITY_HEADER = "cloudfront-viewer-city";
const COUNTRY_RE = /^[A-Z]{2}$/;
const CITY_RE = /^[^\p{Cc}]{1,100}$/u;
const REQUIRED_SETTINGS = ["DD_API_KEY", "DD_LOGS_URL", "INSTALL_ID_SECRET"];

class InvalidPayloadError extends Error {}

class BodyTooLargeError extends Error {}

function check(condition, message) {
  if (!condition) {
    throw new InvalidPayloadError(message);
  }
}

function isObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNonNegativeInteger(value) {
  return Number.isSafeInteger(value) && value >= 0;
}

function isMatch(value, regex) {
  return typeof value === "string" && regex.test(value);
}

function parseNames(value, field) {
  check(Array.isArray(value), `${field} must be a list`);
  check(value.length <= MAX_NUM_NAMES, `${field} has more than ${MAX_NUM_NAMES} items`);
  for (const name of value) {
    check(isMatch(name, NAME_RE), `${field} contains an invalid name`);
  }
  return [...new Set(value)].sort();
}

function parseBucket(value, field) {
  check(isMatch(value, BUCKET_RE), `invalid ${field}`);
  return value;
}

function parseApiUsage(apiUsage) {
  check(isObject(apiUsage), "api_usage must be an object");
  const parsedApiUsage = {};
  for (const [apiKind, bucket] of Object.entries(apiUsage)) {
    check(API_KINDS.has(apiKind), "unknown api_usage key");
    parsedApiUsage[apiKind] = parseBucket(bucket, "api_usage bucket");
  }
  return parsedApiUsage;
}

function parseClusterSummary(clusterSummary) {
  check(isObject(clusterSummary), "cluster_summary must be an object");
  return {
    num_indexes: parseBucket(clusterSummary.num_indexes, "cluster_summary.num_indexes"),
    source_types: parseNames(clusterSummary.source_types, "cluster_summary.source_types"),
  };
}

function parseEvent(event) {
  check(isObject(event), "events must be objects");
  check(EVENT_TYPES.has(event.type), "unknown event type");
  check(isNonNegativeInteger(event.unixtime), "invalid event unixtime");
  // The client clock may be off, so the log timestamp is the intake time.
  const parsedEvent = { type: event.type, client_unixtime: event.unixtime };
  if (event.type === "end_command") {
    check(Number.isSafeInteger(event.return_code), "invalid end_command return_code");
    parsedEvent.return_code = event.return_code;
  }
  if (ACTIVITY_EVENT_TYPES.has(event.type)) {
    if (event.api_usage !== undefined) {
      parsedEvent.api_usage = parseApiUsage(event.api_usage);
    }
    if (event.cluster_summary !== undefined) {
      parsedEvent.cluster_summary = parseClusterSummary(event.cluster_summary);
    }
  }
  return parsedEvent;
}

function parseBuild(build) {
  check(isObject(build), "quickwit_info.build must be an object");
  check(isMatch(build.commit, COMMIT_RE), "invalid quickwit_info.build.commit");
  check(
    build.target === "unknown" || isMatch(build.target, TARGET_RE),
    "invalid quickwit_info.build.target",
  );
  check(typeof build.release === "boolean", "invalid quickwit_info.build.release");
  return { commit: build.commit, target: build.target, release: build.release };
}

function parsePayload(payload) {
  check(isObject(payload), "payload must be an object");
  const schemaVersion = payload.schema_version ?? 1;
  const isKnownSchemaVersion =
    Number.isSafeInteger(schemaVersion) &&
    schemaVersion >= 1 &&
    schemaVersion <= MAX_SCHEMA_VERSION;
  check(isKnownSchemaVersion, "unsupported schema_version");
  const clientInfo = payload.client_info;
  check(isObject(clientInfo), "missing client_info");
  check(isMatch(clientInfo.session_uuid, UUID_RE), "invalid client_info.session_uuid");
  check(isMatch(clientInfo.os, NAME_RE), "invalid client_info.os");
  check(isMatch(clientInfo.arch, NAME_RE), "invalid client_info.arch");
  check(
    isMatch(clientInfo.hashed_host_username, MD5_HEX_RE),
    "invalid client_info.hashed_host_username",
  );
  check(typeof clientInfo.kubernetes === "boolean", "invalid client_info.kubernetes");
  const client = {
    session_uuid: clientInfo.session_uuid,
    os: clientInfo.os,
    arch: clientInfo.arch,
    hashed_host_username: clientInfo.hashed_host_username,
    kubernetes: clientInfo.kubernetes,
  };
  if (clientInfo.cpus !== undefined) {
    client.cpus = parseBucket(clientInfo.cpus, "client_info.cpus");
  }
  if (clientInfo.memory !== undefined) {
    client.memory = parseBucket(clientInfo.memory, "client_info.memory");
  }

  const quickwitInfo = payload.quickwit_info;
  check(isObject(quickwitInfo), "missing quickwit_info");
  check(isMatch(quickwitInfo.version, VERSION_RE), "invalid quickwit_info.version");
  const quickwit = {
    version: quickwitInfo.version,
    services: parseNames(quickwitInfo.services, "quickwit_info.services"),
    features: parseNames(quickwitInfo.features, "quickwit_info.features"),
  };
  if (quickwitInfo.build !== undefined) {
    quickwit.build = parseBuild(quickwitInfo.build);
  }
  if (quickwitInfo.install_uuid !== undefined) {
    check(isMatch(quickwitInfo.install_uuid, UUID_RE), "invalid quickwit_info.install_uuid");
    client.install_uuid = quickwitInfo.install_uuid;
  }
  if (quickwitInfo.hashed_cluster_id !== undefined) {
    check(
      isMatch(quickwitInfo.hashed_cluster_id, MD5_HEX_RE),
      "invalid quickwit_info.hashed_cluster_id",
    );
    quickwit.hashed_cluster_id = quickwitInfo.hashed_cluster_id;
  }

  check(Array.isArray(payload.events), "events must be a list");
  check(payload.events.length <= MAX_NUM_EVENTS, `more than ${MAX_NUM_EVENTS} events`);
  check(isNonNegativeInteger(payload.num_dropped_events), "invalid num_dropped_events");

  return {
    schema_version: schemaVersion,
    client,
    quickwit,
    events: payload.events.map(parseEvent),
    num_dropped_events: payload.num_dropped_events,
  };
}

async function computeKeyedHash(secret, value) {
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign("HMAC", key, encoder.encode(value));
  return Array.from(new Uint8Array(signature), (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );
}

function parseLocation(headers) {
  const location = {};
  const country = headers.get(COUNTRY_HEADER);
  if (country !== null && COUNTRY_RE.test(country)) {
    location.country = country;
  }
  const city = headers.get(CITY_HEADER);
  if (city !== null && CITY_RE.test(city)) {
    location.city = city;
  }
  return location;
}

// The prefix keeps a cluster ID from ever equaling an install ID.
async function replaceHashes(parsedPayload, secret) {
  const { hashed_host_username: hashedHostUsername, ...client } = parsedPayload.client;
  client.install_id = await computeKeyedHash(secret, hashedHostUsername);
  const { hashed_cluster_id: hashedClusterId, ...quickwit } = parsedPayload.quickwit;
  if (hashedClusterId !== undefined) {
    quickwit.cluster_id = await computeKeyedHash(secret, `cluster\n${hashedClusterId}`);
  }
  return { ...parsedPayload, client, quickwit };
}

function toDatadogLogs(keyedPayload, location) {
  const hasLocation = Object.keys(location).length > 0;
  return keyedPayload.events.map((event) => ({
    ddsource: "quickwit-telemetry",
    service: "quickwit-telemetry",
    message: event.type,
    schema_version: keyedPayload.schema_version,
    event,
    client: keyedPayload.client,
    quickwit: keyedPayload.quickwit,
    ...(hasLocation ? { location } : {}),
    num_dropped_events: keyedPayload.num_dropped_events,
  }));
}

async function readBody(request) {
  const contentLength = Number(request.headers.get("content-length"));
  if (contentLength > MAX_BODY_NUM_BYTES) {
    throw new BodyTooLargeError();
  }
  if (request.body === null) {
    return "";
  }
  const reader = request.body.getReader();
  const chunks = [];
  let numBytes = 0;
  while (true) {
    const { done, value: chunk } = await reader.read();
    if (done) {
      break;
    }
    numBytes += chunk.byteLength;
    if (numBytes > MAX_BODY_NUM_BYTES) {
      await reader.cancel();
      throw new BodyTooLargeError();
    }
    chunks.push(chunk);
  }
  const body = new Uint8Array(numBytes);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(body);
}

function textResponse(status, text, headers = {}) {
  return new Response(`${text}\n`, { status, headers });
}

export default {
  async fetch(request, env) {
    const missingSettings = REQUIRED_SETTINGS.filter((name) => !env[name]);
    if (missingSettings.length > 0) {
      console.error(`telemetry relay is missing settings: ${missingSettings.join(", ")}`);
      return textResponse(500, "relay is misconfigured");
    }
    if (new URL(request.url).pathname !== "/") {
      return textResponse(404, "not found");
    }
    if (request.method !== "POST") {
      return textResponse(405, "method not allowed", { Allow: "POST" });
    }
    let parsedPayload;
    try {
      parsedPayload = parsePayload(JSON.parse(await readBody(request)));
    } catch (error) {
      if (error instanceof BodyTooLargeError) {
        return textResponse(413, "payload too large");
      }
      if (error instanceof SyntaxError || error instanceof InvalidPayloadError) {
        return textResponse(400, `invalid payload: ${error.message}`);
      }
      throw error;
    }
    if (parsedPayload.events.length === 0) {
      return new Response(null, { status: 202 });
    }
    const keyedPayload = await replaceHashes(parsedPayload, env.INSTALL_ID_SECRET);
    const logs = toDatadogLogs(keyedPayload, parseLocation(request.headers));
    const intakeResponse = await fetch(env.DD_LOGS_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json", "DD-API-KEY": env.DD_API_KEY },
      body: JSON.stringify(logs),
    });
    if (!intakeResponse.ok) {
      console.error(`Datadog logs intake returned HTTP ${intakeResponse.status}`);
      return textResponse(502, "failed to forward telemetry");
    }
    return new Response(null, { status: 202 });
  },
};
