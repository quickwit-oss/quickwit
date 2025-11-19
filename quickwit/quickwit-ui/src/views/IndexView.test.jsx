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
import { BrowserRouter } from "react-router";
import { Client } from "../services/client";
import IndexView from "./IndexView";

jest.mock("../services/client");
jest.mock("react-router", () => ({
  ...jest.requireActual("react-router"),
  useParams: () => ({
    indexId: "my-new-fresh-index-id",
  }),
}));

test("renders IndexView", async () => {
  const index = {
    metadata: {
      index_config: {
        index_uri: "my-new-fresh-index-uri",
      },
    },
    splits: [],
  };
  Client.prototype.getIndex.mockImplementation(() => Promise.resolve(index));

  await act(async () => {
    render(<IndexView />, { wrapper: BrowserRouter });
  });

  await waitFor(() =>
    expect(screen.getByText(/my-new-fresh-index-uri/)).toBeInTheDocument(),
  );
});
