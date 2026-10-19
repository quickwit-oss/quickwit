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

import relay, { computeInstallId } from "../src/relay.js";

const ENV = {
  DD_API_KEY: "test-api-key",
  DD_LOGS_URL: "https://http-intake.logs.example.com/api/v2/logs",
  INSTALL_ID_SECRET: "test-secret",
};
const HASHED_HOST_USERNAME = "5d41402abc4b2a76b9719d911017c592";
const CLIENT_IP = "203.0.113.7";

// The shape `quickwit-telemetry` serializes (`TelemetryPayload`).
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
      services: ["searcher", "control-plane", "metastore_read_replica"],
      features: ["s3_storage", "otlp"],
    },
    events: [
      { unixtime: 1760000000, type: "run_command" },
      { unixtime: 1760000060, type: "end_command", return_code: 1 },
    ],
    num_dropped_events: 0,
  };
}

function makeRequest(body, { method = "POST", path = "/" } = {}) {
  return new Request(`https://telemetry.quickwit.io${path}`, {
    method,
    headers: {
      "Content-Type": "application/json",
      "CF-Connecting-IP": CLIENT_IP,
      "X-Forwarded-For": CLIENT_IP,
    },
    body: typeof body === "string" ? body : JSON.stringify(body),
  });
}

describe("telemetry relay", () => {
  let intakeCalls;
  let intakeStatus;
  const originalFetch = globalThis.fetch;

  beforeEach(() => {
    intakeCalls = [];
    intakeStatus = 202;
    globalThis.fetch = async (url, init) => {
      intakeCalls.push({ url, init });
      return new Response(null, { status: intakeStatus });
    };
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  it("forwards one log per event to the Datadog logs intake", async () => {
    const response = await relay.fetch(makeRequest(makePayload()), ENV);

    assert.equal(response.status, 202);
    assert.equal(intakeCalls.length, 1);
    const { url, init } = intakeCalls[0];
    assert.equal(url, ENV.DD_LOGS_URL);
    assert.deepEqual(init.headers, {
      "Content-Type": "application/json",
      "DD-API-KEY": ENV.DD_API_KEY,
    });
    const expectedInstallId = createHmac("sha256", ENV.INSTALL_ID_SECRET)
      .update(HASHED_HOST_USERNAME)
      .digest("hex");
    const commonAttributes = {
      ddsource: "quickwit-telemetry",
      service: "quickwit-telemetry",
      client: {
        session_uuid: "0d6e4079-e2c8-4bd5-a1a1-a6bce5ad1c29",
        os: "linux",
        arch: "x86_64",
        kubernetes: true,
        install_id: expectedInstallId,
      },
      quickwit: {
        version: "0.9.1",
        services: ["control-plane", "metastore_read_replica", "searcher"],
        features: ["otlp", "s3_storage"],
      },
      num_dropped_events: 0,
    };
    assert.deepEqual(JSON.parse(init.body), [
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

  it("never forwards the client IP address or the unkeyed host hash", async () => {
    await relay.fetch(makeRequest(makePayload()), ENV);

    const forwarded = JSON.stringify(intakeCalls[0]);
    assert.ok(!forwarded.includes(CLIENT_IP));
    assert.ok(!forwarded.includes(HASHED_HOST_USERNAME));
  });

  it("drops fields that are not part of the documented payload", async () => {
    const payload = makePayload();
    payload.client_info.email = "someone@example.com";
    payload.quickwit_info.index_ids = ["secret-index"];
    payload.events[0].query = "secret query";
    payload.extra = "extra";

    const response = await relay.fetch(makeRequest(payload), ENV);

    assert.equal(response.status, 202);
    const forwarded = intakeCalls[0].init.body;
    for (const value of ["someone@example.com", "secret-index", "secret query", "extra"]) {
      assert.ok(!forwarded.includes(value), `${value} was forwarded`);
    }
  });

  it("keys the install ID with the secret", async () => {
    const installId = await computeInstallId("secret-a", HASHED_HOST_USERNAME);

    assert.match(installId, /^[0-9a-f]{64}$/);
    assert.notEqual(installId, await computeInstallId("secret-b", HASHED_HOST_USERNAME));
    assert.equal(installId, await computeInstallId("secret-a", HASHED_HOST_USERNAME));
  });

  it("accepts a payload without events and forwards nothing", async () => {
    const payload = makePayload();
    payload.events = [];

    const response = await relay.fetch(makeRequest(payload), ENV);

    assert.equal(response.status, 202);
    assert.equal(intakeCalls.length, 0);
  });

  const invalidPayloads = {
    "malformed JSON": "{",
    "a list": [],
    "missing client_info": { ...makePayload(), client_info: undefined },
    "an invalid session UUID": (() => {
      const payload = makePayload();
      payload.client_info.session_uuid = "not-a-uuid";
      return payload;
    })(),
    "a host hash that is not an MD5": (() => {
      const payload = makePayload();
      payload.client_info.hashed_host_username = "myhost:root";
      return payload;
    })(),
    "a free-form feature name": (() => {
      const payload = makePayload();
      payload.quickwit_info.features = ["Index my-logs on s3://bucket"];
      return payload;
    })(),
    "an unknown event type": (() => {
      const payload = makePayload();
      payload.events = [{ unixtime: 1760000000, type: "search", query: "secret" }];
      return payload;
    })(),
    "an end_command without return code": (() => {
      const payload = makePayload();
      payload.events = [{ unixtime: 1760000000, type: "end_command" }];
      return payload;
    })(),
    "too many events": (() => {
      const payload = makePayload();
      payload.events = Array.from({ length: 11 }, () => ({ unixtime: 1, type: "running" }));
      return payload;
    })(),
  };

  for (const [description, payload] of Object.entries(invalidPayloads)) {
    it(`rejects ${description}`, async () => {
      const response = await relay.fetch(makeRequest(payload), ENV);

      assert.equal(response.status, 400);
      assert.equal(intakeCalls.length, 0);
    });
  }

  it("rejects payloads larger than 16 KiB", async () => {
    const payload = makePayload();
    payload.padding = "x".repeat(17 * 1024);

    const response = await relay.fetch(makeRequest(payload), ENV);

    assert.equal(response.status, 413);
    assert.equal(intakeCalls.length, 0);
  });

  it("only accepts POST requests on /", async () => {
    const getResponse = await relay.fetch(
      new Request("https://telemetry.quickwit.io/", { method: "GET" }),
      ENV,
    );
    assert.equal(getResponse.status, 405);

    const otherPathResponse = await relay.fetch(
      makeRequest(makePayload(), { path: "/api/v1/ingest" }),
      ENV,
    );
    assert.equal(otherPathResponse.status, 404);
    assert.equal(intakeCalls.length, 0);
  });

  it("fails without forwarding when a setting is missing", async () => {
    const { INSTALL_ID_SECRET: _secret, ...envWithoutSecret } = ENV;
    const originalConsoleError = console.error;
    console.error = () => {};
    try {
      const response = await relay.fetch(makeRequest(makePayload()), envWithoutSecret);

      assert.equal(response.status, 500);
      assert.equal(intakeCalls.length, 0);
    } finally {
      console.error = originalConsoleError;
    }
  });

  it("returns 502 when the Datadog logs intake fails", async () => {
    intakeStatus = 403;
    const originalConsoleError = console.error;
    console.error = () => {};
    try {
      const response = await relay.fetch(makeRequest(makePayload()), ENV);

      assert.equal(response.status, 502);
    } finally {
      console.error = originalConsoleError;
    }
  });
});
