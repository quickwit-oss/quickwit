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
import { createMemoryRouter, RouterProvider } from "react-router";
import SideBar from "../components/SideBar";
import { LocalStorageProvider } from "../providers/LocalStorageProvider";
import { Client } from "../services/client";
import { EMPTY_SEARCH_REQUEST } from "../utils/models";
import SearchView from "./SearchView";

let mockEditorSearchAction = null;

jest.mock("@monaco-editor/react", () => {
  const React = require("react");
  return {
    Editor: ({ onMount }) => {
      React.useEffect(() => {
        const editor = {
          addAction: (action) => {
            if (action.id === "SEARCH") {
              mockEditorSearchAction = action.run;
            }
          },
          layout: jest.fn(),
        };
        const monaco = {
          KeyCode: { F9: 1, Enter: 2 },
          KeyMod: { CtrlCmd: 4 },
          languages: {
            getLanguages: () => [],
            register: jest.fn(),
            setMonarchTokensProvider: jest.fn(),
            registerCompletionItemProvider: jest.fn(),
            setLanguageConfiguration: jest.fn(),
          },
        };
        onMount?.(editor, monaco);
      }, []);
      return null;
    },
  };
});

jest.mock("../services/client");
const searchUrl = "/search?index_id=my-new-fresh-index-id";

function renderSearch(initialEntry = searchUrl) {
  const router = createMemoryRouter(
    [
      { path: "/search", element: <SearchView /> },
      { path: "/indexes", element: <SideBar /> },
    ],
    { initialEntries: [initialEntry] },
  );
  const view = render(
    <LocalStorageProvider>
      <RouterProvider router={router} />
    </LocalStorageProvider>,
  );
  return { ...view, router };
}

beforeEach(() => {
  localStorage.clear();
  mockEditorSearchAction = null;
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
    renderSearch();
  });

  await waitFor(() =>
    expect(screen.getByText(/This is an info log/)).toBeInTheDocument(),
  );
});

test("changes the hit limit and keeps it when switching search modes", async () => {
  const { router } = renderSearch();
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
  expect(
    new URLSearchParams(router.state.location.search).get("ui_max_hits"),
  ).toBe("50");
  expect(
    screen.getByRole("button", { name: /[?&]max_hits=50/ }),
  ).toBeInTheDocument();

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
  ["ui_max_hits=100", 100],
  ["max_hits=100", 20],
  ["max_hits=10", 20],
  ["ui_max_hits=0", 20],
  ["ui_max_hits=1000", 20],
  ["ui_max_hits=50oops", 20],
])(
  "uses %s from a search URL as a hit limit of %i",
  async (param, expected) => {
    renderSearch(`${searchUrl}&${param}`);
    await screen.findByText(/This is an info log/);

    expect(Client.prototype.search).toHaveBeenLastCalledWith(
      expect.objectContaining({ maxHits: expected }),
      null,
    );
    expect(screen.getByRole("combobox", { name: "Max hits" })).toHaveValue(
      String(expected),
    );
  },
);

test("reruns searches with the hit limits from Back and Forward", async () => {
  Client.prototype.search.mockImplementation(async ({ maxHits }) => ({
    num_hits: 1,
    hits: [{ body: `Result for limit ${maxHits}` }],
    elapsed_time_micros: 10,
    errors: [],
  }));

  const { router } = renderSearch();
  await screen.findByText(/Result for limit 20/);
  fireEvent.change(screen.getByRole("combobox", { name: "Max hits" }), {
    target: { value: "50" },
  });
  await screen.findByText(/Result for limit 50/);

  await act(() => router.navigate(-1));
  expect(await screen.findByText(/Result for limit 20/)).toBeInTheDocument();
  expect(screen.getByRole("combobox", { name: "Max hits" })).toHaveValue("20");

  await act(() => router.navigate(1));
  expect(await screen.findByText(/Result for limit 50/)).toBeInTheDocument();
  expect(screen.getByRole("combobox", { name: "Max hits" })).toHaveValue("50");
});

test("opens a legacy last search at 20 hits and saves new selections", async () => {
  localStorage.setItem(
    "lastSearchRequest",
    JSON.stringify({
      ...EMPTY_SEARCH_REQUEST,
      indexId: "my-new-fresh-index-id",
      maxHits: 100,
    }),
  );
  const { unmount } = renderSearch("/indexes");
  fireEvent.click(screen.getByRole("link", { name: "Query editor" }));
  await screen.findByText(/This is an info log/);
  const maxHits = screen.getByRole("combobox", { name: "Max hits" });
  expect(maxHits).toHaveValue("20");
  fireEvent.change(maxHits, { target: { value: "100" } });
  await screen.findByText(/This is an info log/);
  unmount();

  renderSearch("/indexes");
  fireEvent.click(screen.getByRole("link", { name: "Query editor" }));
  await screen.findByText(/This is an info log/);
  expect(screen.getByRole("combobox", { name: "Max hits" })).toHaveValue("100");
});

test("keeps an explicit hit limit when index metadata loads slowly", async () => {
  let resolveIndex;
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
  Client.prototype.getIndex.mockImplementation(
    () =>
      new Promise((resolve) => {
        resolveIndex = resolve;
      }),
  );
  Client.prototype.search.mockImplementation(async ({ maxHits }) => ({
    num_hits: 1,
    hits: [{ body: `Result for limit ${maxHits}` }],
    elapsed_time_micros: 10,
    errors: [],
  }));

  const { router } = renderSearch();
  fireEvent.change(screen.getByRole("combobox", { name: "Max hits" }), {
    target: { value: "50" },
  });
  await waitFor(() =>
    expect(Client.prototype.search).toHaveBeenCalledWith(
      expect.objectContaining({ maxHits: 50 }),
      null,
    ),
  );

  await act(async () => {
    resolveIndex(index);
  });

  expect(await screen.findByText(/Result for limit 50/)).toBeInTheDocument();
  await waitFor(() => expect(Client.prototype.search).toHaveBeenCalledTimes(2));
  expect(Client.prototype.search).toHaveBeenLastCalledWith(
    expect.objectContaining({ maxHits: 50 }),
    null,
  );
  expect(screen.getByRole("combobox", { name: "Max hits" })).toHaveValue("50");
  expect(
    new URLSearchParams(router.state.location.search).get("ui_max_hits"),
  ).toBe("50");

  await act(() => router.navigate(-1));
  expect(await screen.findByText(/Result for limit 20/)).toBeInTheDocument();
  expect(screen.getByRole("combobox", { name: "Max hits" })).toHaveValue("20");
});

test("keeps the selected hit limit when running search via editor shortcut", async () => {
  Client.prototype.search.mockImplementation(async ({ maxHits }) => ({
    num_hits: 1,
    hits: [{ body: `Result for limit ${maxHits}` }],
    elapsed_time_micros: 10,
    errors: [],
  }));

  const { router } = renderSearch();
  await screen.findByText(/Result for limit 20/);

  fireEvent.change(screen.getByRole("combobox", { name: "Max hits" }), {
    target: { value: "50" },
  });
  await screen.findByText(/Result for limit 50/);

  expect(mockEditorSearchAction).not.toBeNull();
  await act(async () => {
    mockEditorSearchAction();
  });

  await waitFor(() => expect(Client.prototype.search).toHaveBeenCalledTimes(3));
  expect(Client.prototype.search).toHaveBeenLastCalledWith(
    expect.objectContaining({ maxHits: 50 }),
    null,
  );
  expect(screen.getByRole("combobox", { name: "Max hits" })).toHaveValue("50");
  expect(
    new URLSearchParams(router.state.location.search).get("ui_max_hits"),
  ).toBe("50");
  expect(screen.getByText(/Result for limit 50/)).toBeInTheDocument();
});
