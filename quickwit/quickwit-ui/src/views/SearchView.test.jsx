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

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { act } from "react";
import { Client } from "../services/client";
import SearchView from "./SearchView";

jest.mock("../services/client");
const mockedUsedNavigate = jest.fn();
let mockSearchParams;
jest.mock("react-router", () => ({
  ...jest.requireActual("react-router"),
  useLocation: () => ({
    pathname: "/search",
    search: mockSearchParams,
  }),
  useNavigate: () => mockedUsedNavigate,
}));

beforeEach(() => {
  mockSearchParams = "?index_id=my-new-fresh-index-id";
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
  Client.prototype.search.mockImplementation((request) =>
    Promise.resolve(
      request.aggregation
        ? { ...searchResponse, hits: [], aggregations: {} }
        : searchResponse,
    ),
  );
});

test("renders SearchView", async () => {
  await act(async () => {
    render(<SearchView />);
  });

  await waitFor(() =>
    expect(screen.getByText(/This is an info log/)).toBeInTheDocument(),
  );
});

test("changes the hit limit and keeps it when switching search modes", async () => {
  render(<SearchView />);
  await screen.findByText(/This is an info log/);

  const maxHits = screen.getByRole("combobox", { name: "Max hits" });
  expect(maxHits).toHaveValue("20");
  expect(Client.prototype.search).toHaveBeenLastCalledWith(
    expect.objectContaining({ maxHits: 20 }),
    null,
  );

  fireEvent.change(maxHits, { target: { value: "50" } });
  await screen.findByText(/This is an info log/);
  expect(Client.prototype.search).toHaveBeenLastCalledWith(
    expect.objectContaining({ maxHits: 50 }),
    null,
  );
  expect(mockedUsedNavigate).toHaveBeenLastCalledWith(
    expect.stringContaining("max_hits=50"),
  );

  fireEvent.click(screen.getByRole("tab", { name: "Aggregation" }));
  await screen.findByText("no result to display");
  expect(
    screen.queryByRole("combobox", { name: "Max hits" }),
  ).not.toBeInTheDocument();

  fireEvent.click(screen.getByRole("tab", { name: "Search" }));
  await screen.findByText(/This is an info log/);
  expect(screen.getByRole("combobox", { name: "Max hits" })).toHaveValue("50");
  expect(Client.prototype.search).toHaveBeenLastCalledWith(
    expect.objectContaining({ maxHits: 50, aggregation: false }),
    null,
  );
});

test.each([
  ["100", 100],
  ["10", 20],
  ["0", 20],
  ["1000", 20],
  ["50oops", 20],
])("restores max_hits=%s from a search URL as %i", async (value, expected) => {
  mockSearchParams += `&max_hits=${value}`;
  render(<SearchView />);
  await screen.findByText(/This is an info log/);

  expect(Client.prototype.search).toHaveBeenLastCalledWith(
    expect.objectContaining({ maxHits: expected }),
    null,
  );
  expect(screen.getByRole("combobox", { name: "Max hits" })).toHaveValue(
    String(expected),
  );
});
