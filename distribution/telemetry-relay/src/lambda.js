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

import { timingSafeEqual } from "node:crypto";

import relay from "./relay.js";

// Requests without CloudFront's secret came straight to API Gateway and are rejected, so that
// nobody can bypass CloudFront or forge its location headers.
const ORIGIN_VERIFY_HEADER = "x-origin-verify";
// Only these headers reach the relay, never the client IP address.
const FORWARDED_HEADERS = [
  "content-length",
  "content-type",
  "cloudfront-viewer-country",
  "cloudfront-viewer-city",
];

// The AWS SDK ships with the Lambda runtime: importing it lazily keeps the relay testable without
// it.
async function loadSettingsFromSsm() {
  const ddApiKeyParameter = process.env.DD_API_KEY_PARAMETER;
  const installIdSecretParameter = process.env.INSTALL_ID_SECRET_PARAMETER;
  if (!ddApiKeyParameter || !installIdSecretParameter) {
    throw new Error("DD_API_KEY_PARAMETER and INSTALL_ID_SECRET_PARAMETER must be set");
  }
  const { GetParametersCommand, SSMClient } = await import("@aws-sdk/client-ssm");
  const output = await new SSMClient({}).send(
    new GetParametersCommand({
      Names: [ddApiKeyParameter, installIdSecretParameter],
      WithDecryption: true,
    }),
  );
  const invalidParameters = output.InvalidParameters ?? [];
  if (invalidParameters.length > 0) {
    throw new Error(`missing SSM parameters: ${invalidParameters.join(", ")}`);
  }
  const parameters = output.Parameters ?? [];
  const values = new Map(parameters.map((parameter) => [parameter.Name, parameter.Value]));
  return {
    DD_API_KEY: values.get(ddApiKeyParameter),
    DD_LOGS_URL: process.env.DD_LOGS_URL,
    INSTALL_ID_SECRET: values.get(installIdSecretParameter),
  };
}

function toRequest(event) {
  const method = event.requestContext.http.method;
  const query = event.rawQueryString ? `?${event.rawQueryString}` : "";
  const headers = {};
  for (const name of FORWARDED_HEADERS) {
    if (event.headers?.[name] !== undefined) {
      headers[name] = event.headers[name];
    }
  }
  const hasBody = method !== "GET" && method !== "HEAD" && event.body !== undefined;
  const body = hasBody
    ? Buffer.from(event.body, event.isBase64Encoded ? "base64" : "utf8")
    : undefined;
  return new Request(`https://${event.requestContext.domainName}${event.rawPath}${query}`, {
    method,
    headers,
    body,
  });
}

function isFromCloudFront(event, originVerifySecret) {
  const header = event.headers?.[ORIGIN_VERIFY_HEADER];
  if (header === undefined) {
    return false;
  }
  const expected = Buffer.from(originVerifySecret);
  const actual = Buffer.from(header);
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

export function createHandler({ loadSettings, originVerifySecret }) {
  let settingsPromise = null;

  return async function handler(event) {
    if (!originVerifySecret) {
      console.error("ORIGIN_VERIFY_SECRET must be set");
      return { statusCode: 500, body: "relay is misconfigured\n" };
    }
    if (!isFromCloudFront(event, originVerifySecret)) {
      return { statusCode: 403, body: "forbidden\n" };
    }
    if (settingsPromise === null) {
      settingsPromise = loadSettings();
    }
    let settings;
    try {
      settings = await settingsPromise;
    } catch (error) {
      settingsPromise = null;
      console.error(`failed to load relay settings: ${error.message}`);
      return { statusCode: 500, body: "relay is misconfigured\n" };
    }
    const response = await relay.fetch(toRequest(event), settings);
    return {
      statusCode: response.status,
      headers: Object.fromEntries(response.headers),
      body: await response.text(),
    };
  };
}

export const handler = createHandler({
  loadSettings: loadSettingsFromSsm,
  originVerifySecret: process.env.ORIGIN_VERIFY_SECRET,
});
