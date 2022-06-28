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
import ClusterMembersView from './ClusterMembersView';
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

test('renders ClusterMembersView', async () => {
  const members = {
    'members': [
      {
        'id': 'my-new-fresh-node-id',
        'listen_address': '127:0.0.1',
        'is_self': true,
      }
  ]};
  Client.prototype.clusterMembers.mockImplementation(() => Promise.resolve(members));

  await act(async () => {
    render(<ClusterMembersView />, container);
  });

  await waitFor(() => expect(screen.getByText(/my-new-fresh-node-id/)).toBeInTheDocument());
});
