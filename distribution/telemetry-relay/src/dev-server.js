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

// Runs the relay locally with Node.js, for testing against a local Quickwit node.

import { createServer } from "node:http";
import { Readable } from "node:stream";

import relay from "./relay.js";

const port = Number(process.env.PORT || 8787);
const env = {
  DD_API_KEY: process.env.DD_API_KEY,
  DD_LOGS_URL: process.env.DD_LOGS_URL,
  INSTALL_ID_SECRET: process.env.INSTALL_ID_SECRET,
};

const server = createServer(async (nodeRequest, nodeResponse) => {
  try {
    const hasBody = nodeRequest.method !== "GET" && nodeRequest.method !== "HEAD";
    const headers = {};
    for (const name of ["content-length", "content-type"]) {
      if (nodeRequest.headers[name] !== undefined) {
        headers[name] = nodeRequest.headers[name];
      }
    }
    const request = new Request(`http://localhost:${port}${nodeRequest.url}`, {
      method: nodeRequest.method,
      headers,
      body: hasBody ? Readable.toWeb(nodeRequest) : undefined,
      duplex: "half",
    });
    const response = await relay.fetch(request, env);
    nodeResponse.writeHead(response.status, Object.fromEntries(response.headers));
    nodeResponse.end(Buffer.from(await response.arrayBuffer()));
  } catch (error) {
    console.error(error);
    nodeResponse.writeHead(500);
    nodeResponse.end();
  }
});

server.listen(port, "127.0.0.1", () => {
  console.log(`telemetry relay listening on http://127.0.0.1:${port}`);
});
