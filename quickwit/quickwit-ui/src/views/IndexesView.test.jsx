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
import { DEFAULT_INDEX_CONFIG_YAML } from "../components/CreateIndexDialog";
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

test("renders IndexesView", async () => {
  Client.prototype.listIndexes.mockResolvedValue(indexes);

  await act(async () => {
    render(<IndexesView />, container);
  });

  expect(
    screen.getByText(indexes[0].index_config.index_id),
  ).toBeInTheDocument();
});

test("opens the create index dialog with the default config", async () => {
  Client.prototype.listIndexes.mockResolvedValue(indexes);

  await act(async () => {
    render(<IndexesView />, container);
  });

  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name: /create index/i }));
  });

  // The Monaco editor is mocked and renders its value as plain text. Compare
  // raw `textContent`: `toHaveTextContent` collapses the YAML indentation.
  expect(screen.getByRole("dialog").textContent).toContain(
    DEFAULT_INDEX_CONFIG_YAML,
  );
});

test("creates an index and refetches the index list", async () => {
  Client.prototype.listIndexes.mockResolvedValue(indexes);
  Client.prototype.createIndex.mockResolvedValue(indexes[0]);

  await act(async () => {
    render(<IndexesView />, container);
  });
  expect(Client.prototype.listIndexes).toHaveBeenCalledTimes(1);

  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name: /create index/i }));
  });
  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name: "Create" }));
  });

  expect(Client.prototype.createIndex).toHaveBeenCalledWith(
    DEFAULT_INDEX_CONFIG_YAML,
  );
  expect(Client.prototype.listIndexes).toHaveBeenCalledTimes(2);
  // The dialog fades out, so it only leaves the DOM once the transition ends.
  await waitFor(() =>
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument(),
  );
});

test("keeps the dialog open and displays the error when creation fails", async () => {
  Client.prototype.listIndexes.mockResolvedValue(indexes);
  Client.prototype.createIndex.mockRejectedValue({
    status: 400,
    message: "index `my-index` already exists",
  });

  await act(async () => {
    render(<IndexesView />, container);
  });

  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name: /create index/i }));
  });
  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name: "Create" }));
  });

  expect(
    screen.getByText("index `my-index` already exists"),
  ).toBeInTheDocument();
  expect(screen.getByRole("dialog")).toBeInTheDocument();
  expect(Client.prototype.listIndexes).toHaveBeenCalledTimes(1);
});
