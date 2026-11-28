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

import { describe, expect, it, jest } from "@jest/globals";
import { EMPTY_SEARCH_REQUEST, SearchRequest } from "../utils/models";
import { Client } from "./client";

describe("Client unit test", () => {
  it("Should construct correct search URL", async () => {
    // Mocking the fetch function to simulate network requests
    const mockFetch = jest.fn((_url: string, _options?: unknown) =>
      Promise.resolve({ ok: true, json: () => Promise.resolve({}) }),
    );
    (global as any).fetch = mockFetch;

    const searchRequest: SearchRequest = {
      indexId: "my-new-fresh-index-id",
      query: "severity_error:ERROR",
      startTimestamp: 100,
      endTimestamp: 200,
      maxHits: 50,
      sortByField: {
        field_name: "timestamp",
        order: "Desc",
      },
      aggregation: false,
      aggregationConfig: {
        metric: null,
        term: null,
        histogram: null,
      },
    };

    const client = new Client();
    expect(client.buildSearchBody(searchRequest, null)).toBe(
      '{"query":"severity_error:ERROR","max_hits":50,"start_timestamp":100,"end_timestamp":200,"sort_by_field":"+timestamp"}',
    );

    await client.search(searchRequest, null);
    const expectedUrl = `${client.apiRoot()}my-new-fresh-index-id/search`;
    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(expectedUrl, expect.any(Object));
  });

  it("requests no hits for an aggregation even with a hit limit selected", () => {
    const client = new Client();
    const request: SearchRequest = {
      ...EMPTY_SEARCH_REQUEST,
      maxHits: 50,
      aggregation: true,
      aggregationConfig: {
        ...EMPTY_SEARCH_REQUEST.aggregationConfig,
        metric: { type: "avg", field: "duration" },
      },
    };
    expect(JSON.parse(client.buildSearchBody(request, null))).toEqual({
      query: "*",
      max_hits: 0,
      aggs: { metric: { avg: { field: "duration" } } },
    });
  });
});
