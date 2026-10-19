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

// Quickwit telemetry relay.
//
// Quickwit nodes POST their usage payload (`quickwit/quickwit-telemetry/src/payload.rs`, documented
// in `docs/telemetry.md`) to `telemetry.quickwit.io`. The relay:
// - rejects anything that does not match that format, and keeps only the documented fields;
// - never reads or forwards the client's IP address or request headers;
// - replaces `hashed_host_username`, an unsalted MD5 of `hostname:username` that can be reversed
//   by guessing, with an HMAC-SHA256 keyed with a secret only the relay knows;
// - forwards one log per event to the Datadog Logs intake API.

const MAX_BODY_NUM_BYTES = 16 * 1024;
// Quickwit queues at most `MAX_NUM_EVENTS_IN_QUEUE` (10) events per request.
const MAX_NUM_EVENTS = 10;
const MAX_NUM_NAMES = 32;
const EVENT_TYPES = new Set(["run_command", "end_command", "running", "ui_index_page_load"]);
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const MD5_HEX_RE = /^[0-9a-f]{32}$/;
// OS, CPU architecture, service and feature names.
const NAME_RE = /^[a-z0-9_-]{1,64}$/;
const VERSION_RE = /^[0-9A-Za-z.+-]{1,64}$/;
const REQUIRED_SETTINGS = ["DD_API_KEY", "DD_LOGS_URL", "INSTALL_ID_SECRET"];

export class InvalidPayloadError extends Error {}

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

function parseEvent(event) {
  check(isObject(event), "events must be objects");
  check(EVENT_TYPES.has(event.type), "unknown event type");
  check(isNonNegativeInteger(event.unixtime), "invalid event unixtime");
  // The client clock may be off, so the client time is kept as an attribute and the log
  // timestamp is the intake time.
  const parsedEvent = { type: event.type, client_unixtime: event.unixtime };
  if (event.type === "end_command") {
    check(Number.isSafeInteger(event.return_code), "invalid end_command return_code");
    parsedEvent.return_code = event.return_code;
  }
  return parsedEvent;
}

// Validates a telemetry payload and returns a copy that holds only the documented fields.
export function parsePayload(payload) {
  check(isObject(payload), "payload must be an object");
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

  const quickwitInfo = payload.quickwit_info;
  check(isObject(quickwitInfo), "missing quickwit_info");
  check(isMatch(quickwitInfo.version, VERSION_RE), "invalid quickwit_info.version");

  check(Array.isArray(payload.events), "events must be a list");
  check(payload.events.length <= MAX_NUM_EVENTS, `more than ${MAX_NUM_EVENTS} events`);
  check(isNonNegativeInteger(payload.num_dropped_events), "invalid num_dropped_events");

  return {
    client: {
      session_uuid: clientInfo.session_uuid,
      os: clientInfo.os,
      arch: clientInfo.arch,
      hashed_host_username: clientInfo.hashed_host_username,
      kubernetes: clientInfo.kubernetes,
    },
    quickwit: {
      version: quickwitInfo.version,
      services: parseNames(quickwitInfo.services, "quickwit_info.services"),
      features: parseNames(quickwitInfo.features, "quickwit_info.features"),
    },
    events: payload.events.map(parseEvent),
    num_dropped_events: payload.num_dropped_events,
  };
}

// Returns the hex HMAC-SHA256 of `hashedHostUsername` keyed with `secret`.
export async function computeInstallId(secret, hashedHostUsername) {
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign("HMAC", key, encoder.encode(hashedHostUsername));
  return Array.from(new Uint8Array(signature), (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );
}

function toDatadogLogs(parsedPayload, installId) {
  const { hashed_host_username: _hashedHostUsername, ...client } = parsedPayload.client;
  return parsedPayload.events.map((event) => ({
    ddsource: "quickwit-telemetry",
    service: "quickwit-telemetry",
    message: event.type,
    event,
    client: { ...client, install_id: installId },
    quickwit: parsedPayload.quickwit,
    num_dropped_events: parsedPayload.num_dropped_events,
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
    const installId = await computeInstallId(
      env.INSTALL_ID_SECRET,
      parsedPayload.client.hashed_host_username,
    );
    const intakeResponse = await fetch(env.DD_LOGS_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json", "DD-API-KEY": env.DD_API_KEY },
      body: JSON.stringify(toDatadogLogs(parsedPayload, installId)),
    });
    if (!intakeResponse.ok) {
      console.error(`Datadog logs intake returned HTTP ${intakeResponse.status}`);
      return textResponse(502, "failed to forward telemetry");
    }
    return new Response(null, { status: 202 });
  },
};
