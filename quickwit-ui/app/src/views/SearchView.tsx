// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

import { useEffect, useState } from 'react';
import { IndexSideBar } from '../components/IndexSideBar';
import { ViewUnderAppBarBox, FullBoxContainer } from '../components/LayoutUtils';
import { QueryEditorActionBar } from '../components/QueryActionBar';
import { QueryEditor } from '../components/QueryEditor/QueryEditor';
import SearchResult from '../components/SearchResult/SearchResult';
import { Client } from '../services/client';
import { EMPTY_SEARCH_REQUEST, IndexMetadata, SearchRequest, SearchResponse } from '../utils/models';

function SearchView() {
  const [indexMetadata, setIndexMetadata] = useState<null | IndexMetadata>(null);
  const [searchRequest, setSearchRequest] = useState<SearchRequest>(EMPTY_SEARCH_REQUEST);
  const [searchResponse, setSearchResponse] = useState<null | SearchResponse>(null);
  const [queryRunning, setQueryRunning] = useState(false);
  const quickwitClient = new Client();
  const runSearch = () => {
    console.log('Run search with:', searchRequest);
    setQueryRunning(true);
    quickwitClient.search(searchRequest).then((response) => {
      setSearchResponse(response);
      setQueryRunning(false);
    }, (error) => {
      setQueryRunning(false);
      console.error('Error when running search request', error);
    });
  }
  const onIndexMetadataUpdate = (indexMetadata: IndexMetadata) => {
    setIndexMetadata(indexMetadata);
  }
  const onSearchRequestUpdate = (searchRequest: SearchRequest) => {
    setSearchRequest(searchRequest);
  }
  useEffect(() => {
    if (indexMetadata == null) {
      return;
    }
    setSearchRequest(previousRequest => {
      return {...previousRequest, indexId: indexMetadata.index_id}; 
    });
  }, [indexMetadata]);
  useEffect(() => {
    if (searchRequest.indexId == null) {
      return;
    }
  }, [searchRequest]);

  return (
      <ViewUnderAppBarBox sx={{ flexDirection: 'row'}}>
        <IndexSideBar indexMetadata={indexMetadata} onIndexMetadataUpdate={onIndexMetadataUpdate}/>
        <FullBoxContainer>
          <QueryEditorActionBar
            searchRequest={searchRequest}
            onSearchRequestUpdate={onSearchRequestUpdate}
            runSearch={runSearch}
            indexMetadata={indexMetadata}
            queryRunning={queryRunning} />
          <QueryEditor
            searchRequest={searchRequest}
            onSearchRequestUpdate={onSearchRequestUpdate}
            runSearch={runSearch}
            indexMetadata={indexMetadata}
            queryRunning={queryRunning} />
          <SearchResult
            queryRunning={queryRunning}
            searchResponse={searchResponse}
            indexMetadata={indexMetadata} />
        </FullBoxContainer>
      </ViewUnderAppBarBox>
  );
}

export default SearchView;

function parseUrl(historySearch: string): SearchRequest {
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
  const debug = (searchParams.get("debug") === "true");
  const cacheLevelString = searchParams.get("cacheLevel");
  let cacheLevel = 0;
  if (cacheLevelString != null) {
    cacheLevel = parseInt(cacheLevelString);
    if (Number.isNaN(cacheLevel)) {
      cacheLevel = 0;
    }
  }
  return {
    indexId: null,
    query: searchParams.get("query") || "",
    numHits: 10,
    startTimestamp: startTimestamp,
    endTimestamp: endTimestamp,
  };
}

function toUrlQueryParams(request: SearchRequest): URLSearchParams {
  const params = new URLSearchParams();
  params.append("query", request.query);
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