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

import { render, screen, waitFor } from "@testing-library/react";
import { act } from "react";
import { Client } from "../services/client";
import ClusterView from "./ClusterView";

jest.mock("../services/client");

let container = null;
beforeEach(() => {
  // setup a DOM element as a render target
  container = document.createElement("div");
  document.body.appendChild(container);
});

afterEach(() => {
  // cleanup on exiting
  container.remove();
  container = null;
});

test("renders ClusterStateView", async () => {
  const clusterState = {
    state: {
      seed_addrs: [],
      node_states: {
        "node-green-uCdq/1656700092": {
          key_values: {
            available_services: {
              value: "searcher",
              version: 3,
            },
            grpc_address: {
              value: "127.0.0.1:7281",
              version: 2,
            },
            heartbeat: {
              value: "24",
              version: 27,
            },
          },
          max_version: 27,
        },
      },
    },
    live_nodes: [],
    dead_nodes: [],
  };
  Client.prototype.cluster.mockImplementation(() =>
    Promise.resolve(clusterState),
  );

  await act(async () => {
    render(<ClusterView />, container);
  });

  await waitFor(() =>
    expect(screen.getByText(/node-green-uCdq/)).toBeInTheDocument(),
  );
});
