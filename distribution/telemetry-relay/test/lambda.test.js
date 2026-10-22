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
import { afterEach, beforeEach, describe, it } from "node:test";

import { createHandler } from "../src/lambda.js";

const SETTINGS = {
  DD_API_KEY: "test-api-key",
  DD_LOGS_URL: "https://http-intake.logs.example.com/api/v2/logs",
  INSTALL_ID_SECRET: "test-secret",
};
const ORIGIN_VERIFY_SECRET = "test-origin-secret";
const CLIENT_IP = "198.51.100.23";
const PAYLOAD = {
  client_info: {
    session_uuid: "0d6e4079-e2c8-4bd5-a1a1-a6bce5ad1c29",
    os: "linux",
    arch: "aarch64",
    hashed_host_username: "5d41402abc4b2a76b9719d911017c592",
    kubernetes: false,
  },
  quickwit_info: { version: "0.9.1", services: ["searcher"], features: ["otlp"] },
  events: [{ unixtime: 1760000000, type: "running" }],
  num_dropped_events: 0,
};

function makeEvent({ base64 = false, headers = {} } = {}) {
  const body = JSON.stringify(PAYLOAD);
  return {
    rawPath: "/",
    rawQueryString: "",
    headers: {
      "content-type": "application/json",
      "x-forwarded-for": CLIENT_IP,
      "x-origin-verify": ORIGIN_VERIFY_SECRET,
      ...headers,
    },
    requestContext: {
      domainName: "abc123.execute-api.us-east-1.amazonaws.com",
      http: { method: "POST", path: "/", sourceIp: CLIENT_IP },
    },
    body: base64 ? Buffer.from(body).toString("base64") : body,
    isBase64Encoded: base64,
  };
}

describe("lambda handler", () => {
  let intakeCalls;
  const originalFetch = globalThis.fetch;
  const handler = createHandler({
    loadSettings: async () => SETTINGS,
    originVerifySecret: ORIGIN_VERIFY_SECRET,
  });

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

  it("relays the request", async () => {
    const result = await handler(makeEvent());

    assert.equal(result.statusCode, 202);
    assert.equal(intakeCalls[0].init.headers["DD-API-KEY"], SETTINGS.DD_API_KEY);
  });

  it("decodes base64 bodies", async () => {
    const result = await handler(makeEvent({ base64: true }));

    assert.equal(result.statusCode, 202);
  });

  it("rejects requests that don't come from CloudFront", async () => {
    const result = await handler(makeEvent({ headers: { "x-origin-verify": "guess" } }));

    assert.equal(result.statusCode, 403);
    assert.equal(intakeCalls.length, 0);
  });

  it("passes the location on, but not the client IP or the origin secret", async () => {
    const headers = { "cloudfront-viewer-country": "FR", "cloudfront-viewer-city": "Lyon" };

    await handler(makeEvent({ headers }));

    const [log] = JSON.parse(intakeCalls[0].init.body);
    assert.deepEqual(log.location, { country: "FR", city: "Lyon" });
    const forwarded = JSON.stringify(intakeCalls);
    assert.ok(!forwarded.includes(CLIENT_IP));
    assert.ok(!forwarded.includes(ORIGIN_VERIFY_SECRET));
  });
});
