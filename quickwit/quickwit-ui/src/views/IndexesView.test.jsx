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

import { render, screen } from "@testing-library/react";
import { act } from "react";
import { Client } from "../services/client";
import IndexesView from "./IndexesView";

jest.mock("../services/client");
const mockedUsedNavigate = jest.fn();
jest.mock("react-router", () => ({
  ...jest.requireActual("react-router"),
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
  container.remove();
  container = null;
});

test("renders IndexesView", async () => {
  const indexes = [
    {
      index_config: {
        index_id: "my-new-fresh-index",
        index_uri: "my-uri",
        indexing_settings: {
          timestamp_field: "timestamp",
        },
        search_settings: {},
        doc_mapping: {
          store: false,
          field_mappings: [],
          tag_fields: [],
          dynamic_mapping: false,
        },
      },
      sources: [],
      create_timestamp: 1000,
      update_timestamp: 1000,
    },
  ];
  Client.prototype.listIndexes.mockResolvedValueOnce(() => indexes);

  await act(async () => {
    render(<IndexesView />, container);
  });

  expect(
    screen.getByText(indexes[0].index_config.index_id),
  ).toBeInTheDocument();
});
