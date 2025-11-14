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

import { Aggregation, SearchRequest, SortByField, SortOrder } from "./models";

export function hasSearchParams(historySearch: string): boolean {
  const searchParams = new URLSearchParams(historySearch);

  return (
    searchParams.has("index_id") ||
    searchParams.has("query") ||
    searchParams.has("start_timestamp") ||
    searchParams.has("end_timestamp")
  );
}

export function parseSearchUrl(historySearch: string): SearchRequest {
  const searchParams = new URLSearchParams(historySearch);
  const startTimestampString = searchParams.get("start_timestamp");
  let startTimestamp = null;
  const startTimeStampParsedInt = parseInt(startTimestampString || "", 10);
  if (!Number.isNaN(startTimeStampParsedInt)) {
    startTimestamp = startTimeStampParsedInt;
  }
  let endTimestamp = null;
  const endTimestampString = searchParams.get("end_timestamp");
  const endTimestampParsedInt = parseInt(endTimestampString || "", 10);
  if (!Number.isNaN(endTimestampParsedInt)) {
    endTimestamp = endTimestampParsedInt;
  }
  let indexId = null;
  const indexIdParam = searchParams.get("index_id");
  if (indexIdParam !== null && indexIdParam.length > 0) {
    indexId = searchParams.get("index_id");
  }
  let sortByField = null;
  const sortByFieldParam = searchParams.get("sort_by_field");
  if (sortByFieldParam !== null) {
    if (sortByFieldParam.startsWith("+")) {
      const order: SortOrder = "Desc";
      sortByField = { field_name: sortByFieldParam.substring(1), order: order };
    } else if (sortByFieldParam.startsWith("-")) {
      const order: SortOrder = "Asc";
      sortByField = { field_name: sortByFieldParam.substring(1), order: order };
    } else {
      const order: SortOrder = "Desc";
      sortByField = { field_name: sortByFieldParam, order: order };
    }
  }
  const aggregationParam = searchParams.get("aggregation");
  const aggregation = parseAggregation(aggregationParam);
  return {
    indexId: indexId,
    query: searchParams.get("query") || "",
    maxHits: 10,
    startTimestamp: startTimestamp,
    endTimestamp: endTimestamp,
    sortByField: sortByField,
    aggregation: aggregationParam != null,
    aggregationConfig: aggregation,
  };
}

function parseAggregation(param: string | null): Aggregation {
  const empty: Aggregation = {
    metric: null,
    term: null,
    histogram: null,
  };
  if (param !== null) {
    try {
      const aggregation: Aggregation = JSON.parse(param);
      return aggregation;
    } catch {
      // ignore malformed param
    }
  }
  return empty;
}

export function toUrlSearchRequestParams(
  request: SearchRequest,
): URLSearchParams {
  const params = new URLSearchParams();
  params.append("query", request.query || "*");
  // We have to set the index ID in url params as it's not present in the UI path params.
  // This enables the react app to be able to get index ID from url params
  // if the user enter directly the UI url.
  params.append("index_id", request.indexId || "");
  if (request.maxHits) {
    params.append("max_hits", request.maxHits.toString());
  }
  if (request.startTimestamp) {
    params.append("start_timestamp", request.startTimestamp.toString());
  }
  if (request.endTimestamp) {
    params.append("end_timestamp", request.endTimestamp.toString());
  }
  if (request.sortByField) {
    params.append("sort_by_field", serializeSortByField(request.sortByField));
  }
  if (request.aggregation) {
    params.append(
      "aggregation",
      JSON.stringify(request.aggregationConfig, (_, val) => {
        if (val == null) {
          return undefined;
        } else {
          return val;
        }
      }),
    );
  }
  return params;
}

export function serializeSortByField(sortByField: SortByField): string {
  const order = sortByField.order === "Desc" ? "+" : "-";
  return `${order}${sortByField.field_name}`;
}
