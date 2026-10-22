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

import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import { afterEach, beforeEach, describe, it } from "node:test";

import relay from "../src/relay.js";

const ENV = {
  DD_API_KEY: "test-api-key",
  DD_LOGS_URL: "https://http-intake.logs.example.com/api/v2/logs",
  INSTALL_ID_SECRET: "test-secret",
};
const HASHED_HOST_USERNAME = "5d41402abc4b2a76b9719d911017c592";
const HASHED_CLUSTER_ID = "9e107d9d372bb6826bd81d3542a419d6";
const INSTALL_UUID = "8c4f2a5e-3b1d-4e7f-9a6c-2d5e8f1b3c7a";
const CLIENT_IP = "203.0.113.7";

function keyedHash(value) {
  return createHmac("sha256", ENV.INSTALL_ID_SECRET).update(value).digest("hex");
}

function makePayload() {
  return {
    client_info: {
      session_uuid: "0d6e4079-e2c8-4bd5-a1a1-a6bce5ad1c29",
      os: "linux",
      arch: "x86_64",
      hashed_host_username: HASHED_HOST_USERNAME,
      kubernetes: true,
    },
    quickwit_info: {
      version: "0.9.1",
      services: ["searcher", "control-plane"],
      features: ["s3_storage", "otlp"],
    },
    events: [
      { unixtime: 1760000000, type: "run_command" },
      { unixtime: 1760000060, type: "end_command", return_code: 1 },
    ],
    num_dropped_events: 0,
  };
}

function makeV2Payload() {
  const payload = makePayload();
  payload.schema_version = 2;
  payload.client_info.cpus = "5-8";
  payload.client_info.memory = "8-16GiB";
  payload.quickwit_info.build = {
    commit: "4f20e4f",
    target: "aarch64-unknown-linux-gnu",
    release: false,
  };
  payload.quickwit_info.install_uuid = INSTALL_UUID;
  payload.quickwit_info.hashed_cluster_id = HASHED_CLUSTER_ID;
  payload.events = [
    {
      unixtime: 1760043200,
      type: "running",
      api_usage: { native_search: "1-9", elasticsearch_ingest: "100-999" },
      cluster_summary: { num_indexes: "2-10", source_types: ["vec", "kafka"] },
    },
  ];
  return payload;
}

function makeRequest(body, headers = {}) {
  return new Request("https://telemetry.quickwit.io/", {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Forwarded-For": CLIENT_IP, ...headers },
    body: typeof body === "string" ? body : JSON.stringify(body),
  });
}

describe("telemetry relay", () => {
  let intakeCalls;
  const originalFetch = globalThis.fetch;

  beforeEach(() => {
    intakeCalls = [];
    globalThis.fetch = async (url, init) => {
      intakeCalls.push({ url, init });
      return new Response(null, { status: 202 });
    };
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  function forwardedLogs() {
    return JSON.parse(intakeCalls[0].init.body);
  }

  it("forwards one log per event", async () => {
    const response = await relay.fetch(makeRequest(makePayload()), ENV);

    assert.equal(response.status, 202);
    assert.equal(intakeCalls[0].url, ENV.DD_LOGS_URL);
    assert.equal(intakeCalls[0].init.headers["DD-API-KEY"], ENV.DD_API_KEY);
    const commonAttributes = {
      ddsource: "quickwit-telemetry",
      service: "quickwit-telemetry",
      schema_version: 1,
      client: {
        session_uuid: "0d6e4079-e2c8-4bd5-a1a1-a6bce5ad1c29",
        os: "linux",
        arch: "x86_64",
        kubernetes: true,
        install_id: keyedHash(HASHED_HOST_USERNAME),
      },
      quickwit: {
        version: "0.9.1",
        services: ["control-plane", "searcher"],
        features: ["otlp", "s3_storage"],
      },
      num_dropped_events: 0,
    };
    assert.deepEqual(forwardedLogs(), [
      {
        ...commonAttributes,
        message: "run_command",
        event: { type: "run_command", client_unixtime: 1760000000 },
      },
      {
        ...commonAttributes,
        message: "end_command",
        event: { type: "end_command", client_unixtime: 1760000060, return_code: 1 },
      },
    ]);
  });

  it("forwards the schema version 2 fields", async () => {
    await relay.fetch(makeRequest(makeV2Payload()), ENV);

    const [log] = forwardedLogs();
    assert.equal(log.schema_version, 2);
    assert.equal(log.client.cpus, "5-8");
    assert.equal(log.client.memory, "8-16GiB");
    assert.equal(log.client.install_uuid, INSTALL_UUID);
    assert.deepEqual(log.quickwit.build, {
      commit: "4f20e4f",
      target: "aarch64-unknown-linux-gnu",
      release: false,
    });
    assert.equal(log.quickwit.cluster_id, keyedHash(`cluster\n${HASHED_CLUSTER_ID}`));
    assert.deepEqual(log.event, {
      type: "running",
      client_unixtime: 1760043200,
      api_usage: { elasticsearch_ingest: "100-999", native_search: "1-9" },
      cluster_summary: { num_indexes: "2-10", source_types: ["kafka", "vec"] },
    });
  });

  it("never forwards the client IP, unkeyed hashes or undocumented fields", async () => {
    const payload = makeV2Payload();
    payload.client_info.email = "someone@example.com";
    payload.events[0].query = "secret query";

    await relay.fetch(makeRequest(payload), ENV);

    const forwarded = JSON.stringify(intakeCalls[0]);
    for (const value of [
      CLIENT_IP,
      HASHED_HOST_USERNAME,
      HASHED_CLUSTER_ID,
      "someone@example.com",
      "secret query",
    ]) {
      assert.ok(!forwarded.includes(value), `${value} was forwarded`);
    }
  });

  it("adds the country and city set by CloudFront", async () => {
    const headers = { "CloudFront-Viewer-Country": "FR", "CloudFront-Viewer-City": "Lyon" };

    await relay.fetch(makeRequest(makePayload(), headers), ENV);

    for (const log of forwardedLogs()) {
      assert.deepEqual(log.location, { country: "FR", city: "Lyon" });
    }
  });

  const invalidPayloads = {
    "malformed JSON": "{",
    "an unknown event type": { ...makePayload(), events: [{ unixtime: 1, type: "search" }] },
    "a free-form feature name": (() => {
      const payload = makePayload();
      payload.quickwit_info.features = ["Index my-logs on s3://bucket"];
      return payload;
    })(),
    "an unsupported schema version": { ...makeV2Payload(), schema_version: 3 },
  };

  for (const [description, payload] of Object.entries(invalidPayloads)) {
    it(`rejects ${description}`, async () => {
      const response = await relay.fetch(makeRequest(payload), ENV);

      assert.equal(response.status, 400);
      assert.equal(intakeCalls.length, 0);
    });
  }

  it("rejects payloads larger than 16 KiB", async () => {
    const payload = { ...makePayload(), padding: "x".repeat(17 * 1024) };

    const response = await relay.fetch(makeRequest(payload), ENV);

    assert.equal(response.status, 413);
    assert.equal(intakeCalls.length, 0);
  });
});
