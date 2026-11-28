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

import { useEffect, useMemo, useRef, useState } from "react";
import { useLocation, useNavigate, useNavigationType } from "react-router";
import ApiUrlFooter from "../components/ApiUrlFooter";
import { IndexSideBar } from "../components/IndexSideBar";
import {
  FullBoxContainer,
  ViewUnderAppBarBox,
} from "../components/LayoutUtils";
import { QueryEditorActionBar } from "../components/QueryActionBar";
import { AggregationEditor } from "../components/QueryEditor/AggregationEditor";
import { QueryEditor } from "../components/QueryEditor/QueryEditor";
import SearchResult from "../components/SearchResult/SearchResult";
import { useLocalStorage } from "../providers/LocalStorageProvider";
import { Client } from "../services/client";
import {
  EMPTY_SEARCH_REQUEST,
  Index,
  IndexMetadata,
  ResponseError,
  SearchRequest,
  SearchResponse,
} from "../utils/models";
import {
  hasSearchParams,
  parseSearchUrl,
  toUrlSearchRequestParams,
} from "../utils/urls";

function updateSearchRequestWithIndex(
  index: Index | null,
  searchRequest: SearchRequest,
) {
  // If we have a timestamp field, order by desc on the timestamp field.
  if (index?.metadata.index_config.doc_mapping.timestamp_field) {
    searchRequest.sortByField = {
      field_name: index?.metadata.index_config.doc_mapping.timestamp_field,
      order: "Desc",
    };
  } else {
    searchRequest.sortByField = null;
    searchRequest.startTimestamp = null;
    searchRequest.endTimestamp = null;
  }
  if (index?.metadata.index_config.index_id) {
    searchRequest.indexId = index?.metadata.index_config.index_id;
  }
}

function SearchView() {
  const location = useLocation();
  const navigate = useNavigate();
  const navigationType = useNavigationType();
  const previousLocationKey = useRef(location.key);
  const [index, setIndex] = useState<null | Index>(null);
  const prevIndexIdRef = useRef<string | null>(null);
  const [searchResponse, setSearchResponse] = useState<null | SearchResponse>(
    null,
  );
  const [searchError, setSearchError] = useState<null | ResponseError>(null);
  const [queryRunning, setQueryRunning] = useState(false);
  const [searchRequest, setSearchRequest] = useState<SearchRequest>(
    hasSearchParams(location.search)
      ? parseSearchUrl(location.search)
      : EMPTY_SEARCH_REQUEST,
  );
  const pendingUrlSearch = useRef(searchRequest.indexId ? searchRequest : null);
  const searchSequence = useRef(0);
  const requestedIndexId = searchRequest.indexId;
  const updateLastSearchRequest = useLocalStorage().updateLastSearchRequest;
  const quickwitClient = useMemo(() => new Client(), []);

  const runSearch = (request: SearchRequest, updateUrl = true) => {
    if (!request.indexId) {
      return;
    }

    const updatedSearchRequest = { ...request };
    if (pendingUrlSearch.current) {
      pendingUrlSearch.current =
        index?.metadata.index_config.index_id !==
          updatedSearchRequest.indexId &&
        pendingUrlSearch.current.indexId === updatedSearchRequest.indexId
          ? updatedSearchRequest
          : null;
    }
    const sequence = ++searchSequence.current;
    console.log("Run search...", updatedSearchRequest);
    updateSearchRequestWithIndex(index, updatedSearchRequest);
    setSearchRequest(updatedSearchRequest);
    setQueryRunning(true);
    setSearchError(null);
    if (updateUrl) {
      navigate(
        "/search?" + toUrlSearchRequestParams(updatedSearchRequest).toString(),
      );
    }
    const timestamp_field =
      index?.metadata.index_config.doc_mapping.timestamp_field || null;
    quickwitClient.search(updatedSearchRequest, timestamp_field).then(
      (response) => {
        if (sequence !== searchSequence.current) {
          return;
        }
        updateLastSearchRequest(updatedSearchRequest);
        setSearchResponse(response);
        setQueryRunning(false);
      },
      (error) => {
        if (sequence !== searchSequence.current) {
          return;
        }
        setQueryRunning(false);
        setSearchError(error);
        console.error("Error when running search request", error);
      },
    );
  };
  const onIndexMetadataUpdate = (indexMetadata: IndexMetadata | null) => {
    setSearchRequest((previousRequest) => {
      updateSearchRequestWithIndex(index, previousRequest);
      return {
        ...previousRequest,
        indexId:
          indexMetadata === null ? null : indexMetadata.index_config.index_id,
      };
    });
  };
  const onSearchRequestUpdate = (searchRequest: SearchRequest) => {
    setSearchRequest(searchRequest);
  };

  useEffect(() => {
    if (location.key === previousLocationKey.current) {
      return;
    }
    previousLocationKey.current = location.key;
    if (navigationType !== "POP") {
      return;
    }

    const request = parseSearchUrl(location.search);
    pendingUrlSearch.current = null;
    if (
      request.indexId &&
      request.indexId === index?.metadata.index_config.index_id
    ) {
      runSearch(request, false);
      return;
    }

    // The previous search can finish while the restored index is loading.
    searchSequence.current += 1;
    setSearchResponse(null);
    setSearchError(null);
    setSearchRequest(request);
    setQueryRunning(Boolean(request.indexId));
    if (request.indexId) {
      pendingUrlSearch.current = request;
    } else {
      setIndex(null);
    }
  }, [location.key]);

  useEffect(() => {
    const indexId = index?.metadata.index_config.index_id ?? null;
    if (prevIndexIdRef.current !== indexId) {
      setSearchResponse(null);
    }
    if (indexId && pendingUrlSearch.current?.indexId === indexId) {
      const request = pendingUrlSearch.current;
      pendingUrlSearch.current = null;
      runSearch(request, false);
    } else if (
      indexId &&
      prevIndexIdRef.current === null &&
      indexId === requestedIndexId
    ) {
      runSearch(searchRequest);
    }
    prevIndexIdRef.current = indexId;
  }, [index]);
  useEffect(() => {
    if (!requestedIndexId) {
      return;
    }

    if (
      index !== null &&
      index.metadata.index_config.index_id === requestedIndexId
    ) {
      return;
    }
    // If index id is changing, it's better to reset timestamps as the time unit may be different
    // between indexes.
    if (
      !pendingUrlSearch.current &&
      prevIndexIdRef.current !== null &&
      prevIndexIdRef.current !== index?.metadata.index_config.index_id
    ) {
      searchRequest.startTimestamp = null;
      searchRequest.endTimestamp = null;
    }
    let active = true;
    quickwitClient.getIndex(requestedIndexId).then(
      (fetchedIndex) => {
        if (active) {
          setIndex(fetchedIndex);
        }
      },
      (error) => {
        if (active) {
          setQueryRunning(false);
          setSearchError(error);
        }
      },
    );
    return () => {
      active = false;
    };
  }, [requestedIndexId, quickwitClient, index]);

  const searchParams = toUrlSearchRequestParams(searchRequest, "api");
  return (
    <ViewUnderAppBarBox sx={{ flexDirection: "row" }}>
      <IndexSideBar
        indexMetadata={index === null ? null : index.metadata}
        onIndexMetadataUpdate={onIndexMetadataUpdate}
      />
      <FullBoxContainer sx={{ padding: 0 }}>
        <FullBoxContainer>
          <QueryEditorActionBar
            searchRequest={searchRequest}
            onSearchRequestUpdate={onSearchRequestUpdate}
            runSearch={runSearch}
            index={index}
            queryRunning={queryRunning}
          />
          <QueryEditor
            searchRequest={searchRequest}
            onSearchRequestUpdate={onSearchRequestUpdate}
            runSearch={runSearch}
            index={index}
            queryRunning={queryRunning}
          />
          <AggregationEditor
            searchRequest={searchRequest}
            onSearchRequestUpdate={onSearchRequestUpdate}
            runSearch={runSearch}
            index={index}
            queryRunning={queryRunning}
          />
          <SearchResult
            queryRunning={queryRunning}
            searchError={searchError}
            searchResponse={searchResponse}
            index={index}
          />
        </FullBoxContainer>
        {index !== null &&
          ApiUrlFooter(
            `api/v1/${index?.metadata.index_config.index_id}/search?${searchParams.toString()}`,
          )}
      </FullBoxContainer>
    </ViewUnderAppBarBox>
  );
}

export default SearchView;
