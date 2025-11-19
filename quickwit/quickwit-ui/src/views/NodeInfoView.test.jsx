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
import NodeInfoView from "./NodeInfoView";

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

test("renders NodeInfoView", async () => {
  const cluster = {
    cluster_id: "my cluster id",
  };
  Client.prototype.cluster.mockImplementation(() => Promise.resolve(cluster));

  const config = {
    node_id: "my-node-id",
  };
  Client.prototype.config.mockImplementation(() => Promise.resolve(config));

  const buildInfo = {
    version: "0.3.2",
  };
  Client.prototype.buildInfo.mockImplementation(() =>
    Promise.resolve(buildInfo),
  );
  await act(async () => {
    render(<NodeInfoView />, container);
  });

  await waitFor(() =>
    expect(screen.getByText(/my-node-id/)).toBeInTheDocument(),
  );
});
