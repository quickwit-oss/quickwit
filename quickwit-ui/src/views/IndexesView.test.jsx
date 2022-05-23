// Copyright (C) 2021 Quickwit, Inc.
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
import {screen} from '@testing-library/dom'
import IndexesView from './IndexesView';
import { act } from "react-dom/test-utils";
import {Client} from "../services/client";

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

test('renders IndexesView', async () => {
  const indexes = [{
    index_id: 'my-new-fresh-index',
    index_uri: 'my-uri',
    indexing_settings: {
      timestamp_field: 'timestamp'
    },
    search_settings: {},
    sources: [],
    create_timestamp: 1000,
    update_timestamp: 1000,
    doc_mapping: {
      store: false,
      field_mappings: [],
      tag_fields: [],
      dynamic_mapping: false,
    }
  }];
  Client.prototype.listIndexes.mockResolvedValueOnce(() => indexes);

  await act(async () => {
    render(<IndexesView />, container);
  });

  expect(screen.getByText(indexes[0].index_id)).toBeInTheDocument();
});