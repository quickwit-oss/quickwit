// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

import { render, unmountComponentAtNode } from "react-dom";
import { waitFor } from "@testing-library/react";
import { screen } from '@testing-library/dom';
import ClusterView from './ClusterView';
import { act } from "react-dom/test-utils";
import { Client } from "../services/client";

jest.mock('../services/client');
const mockedUsedNavigate = jest.fn();
jest.mock('react-router-dom', () => ({
  ...jest.requireActual('react-router-dom'),
  useNavigate: () => mockedUsedNavigate,
}));

let container = null;
beforeEach(() => {
  // setup a DOM element as a render target
  container = document.createElement("div");
  document.body.appendChild(container);
});

afterEach(() => {
  // cleanup on exiting
  unmountComponentAtNode(container);
  container.remove();
  container = null;
});

test('renders ClusterStateView', async () => {
  const clusterState = {
      "state": {
        "seed_addrs": [],
        "node_states": {
          "node-green-uCdq/1656700092": {
            "key_values": {
              "available_services": {
                "value": "searcher",
                "version": 3
              },
              "grpc_address": {
                "value": "127.0.0.1:7281",
                "version": 2
              },
              "heartbeat": {
                "value": "24",
                "version": 27
              }
            },
            "max_version": 27
          }
        }
      },
      "live_nodes": [],
      "dead_nodes": []
  };
  Client.prototype.cluster.mockImplementation(() => Promise.resolve(clusterState));

  await act(async () => {
    render(<ClusterView />, container);
  });

  await waitFor(() => expect(screen.getByText(/node-green-uCdq/)).toBeInTheDocument());
});
