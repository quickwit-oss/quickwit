import { SearchRequest } from "./models";

export function hasSearchParams(historySearch: string): boolean {
  const searchParams = new URLSearchParams(historySearch);

  return searchParams.has('index_id') || searchParams.has('query')
    || searchParams.has('start_timestamp') || searchParams.has('end_timestamp');
}

export function parseSearchUrl(historySearch: string): SearchRequest {
  const searchParams = new URLSearchParams(historySearch);
  const startTimestampString = searchParams.get("start_timestamp");
  let startTimestamp = null;
  const startTimeStampParsedInt = parseInt(startTimestampString || "");
  if (!isNaN(startTimeStampParsedInt)) {
    startTimestamp = startTimeStampParsedInt
  }
  let endTimestamp = null;
  const endTimestampString = searchParams.get("end_timestamp");
  const endTimestampParsedInt = parseInt(endTimestampString || "");
  if (!isNaN(endTimestampParsedInt)) {
    endTimestamp = endTimestampParsedInt
  }
  let indexId = null;
  const indexIdParam = searchParams.get("index_id");
  if (indexIdParam !== null && indexIdParam.length > 0) {
    indexId = searchParams.get("index_id");
  }
  return {
    indexId: indexId,
    query: searchParams.get("query") || "",
    maxHits: 10,
    startTimestamp: startTimestamp,
    endTimestamp: endTimestamp,
  };
}

export function toUrlSearchRequestParams(request: SearchRequest): URLSearchParams {
  const params = new URLSearchParams();
  params.append("query", request.query);
  // We have to set the index ID in url params as it's not present in the UI path params.
  // This enables the react app to be able to get index ID from url params 
  // if the user enter directly the UI url.
  params.append("index_id", request.indexId || "");
  if (request.maxHits) {
    params.append("max_hits", request.maxHits.toString());
  }
  if (request.startTimestamp) {
    params.append(
      "start_timestamp",
      request.startTimestamp.toString()
    );
  }
  if (request.endTimestamp) {
    params.append("end_timestamp", request.endTimestamp.toString());
  }
  return params;
}