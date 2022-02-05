import { SearchRequest } from "./models";

export function hasSearchParams(historySearch: string): boolean {
  const searchParams = new URLSearchParams(historySearch);

  return searchParams.has('indexId') || searchParams.has('query')
    || searchParams.has('startTimestamp') || searchParams.has('endTimestamp');
}

export function parseSearchUrl(historySearch: string): SearchRequest {
  const searchParams = new URLSearchParams(historySearch);
  const startTimestampString = searchParams.get("startTimestamp");
  let startTimestamp = null;
  if (startTimestampString != null) {
    startTimestamp = parseInt(startTimestampString);
  }
  const endTimestampString = searchParams.get("endTimestamp");
  let endTimestamp = null;
  if (endTimestampString != null) {
    endTimestamp = parseInt(endTimestampString);
  }
  return {
    indexId: searchParams.get("indexId"),
    query: searchParams.get("query") || "",
    numHits: 10,
    startTimestamp: startTimestamp,
    endTimestamp: endTimestamp,
  };
}

export function toUrlSearchRequestParams(request: SearchRequest): URLSearchParams {
  const params = new URLSearchParams();
  params.append("query", request.query);
  params.append("indexId", request.indexId || "");
  if (request.numHits) {
    params.append("numHits", request.numHits.toString());
  }
  if (request.startTimestamp) {
    params.append(
      "startTimestamp",
      request.startTimestamp.toString()
    );
  }
  if (request.endTimestamp) {
    params.append("endTimestamp", request.endTimestamp.toString());
  }
  return params;
}