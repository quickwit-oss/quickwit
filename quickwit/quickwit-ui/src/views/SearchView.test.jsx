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
import SearchView from "./SearchView";

jest.mock("../services/client");
const mockedUsedNavigate = jest.fn();
jest.mock("react-router", () => ({
  ...jest.requireActual("react-router"),
  useLocation: () => ({
    pathname: "/search",
    search:
      "index_id=my-new-fresh-index-idmax_hits=10&start_timestamp=1460554590&end_timestamp=1460554592&sort_by_field=-timestamp",
  }),
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

test("renders SearchView", async () => {
  const index = {
    metadata: {
      index_config: {
        index_id: "my-new-fresh-index-id",
        index_uri: "my-new-fresh-index-uri",
        indexing_settings: {},
        doc_mapping: {
          field_mappings: [
            {
              name: "timestamp",
              type: "i64",
            },
          ],
        },
      },
    },
    splits: [],
  };
  Client.prototype.getIndex.mockImplementation(() => Promise.resolve(index));
  Client.prototype.listIndexes.mockImplementation(() =>
    Promise.resolve([index.metadata]),
  );

  const searchResponse = {
    num_hits: 2,
    hits: [
      { body: "INFO This is an info log" },
      { body: "WARN This is a warn log" },
    ],
    elapsed_time_micros: 10,
    errors: [],
  };
  Client.prototype.search.mockImplementation(() =>
    Promise.resolve(searchResponse),
  );

  await act(async () => {
    render(<SearchView />, container);
  });

  await waitFor(() =>
    expect(screen.getByText(/This is an info log/)).toBeInTheDocument(),
  );
});
