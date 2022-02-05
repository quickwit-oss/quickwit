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

import { useEffect, useMemo, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import ApiUrlFooter from '../components/ApiUrlFooter';
import { IndexSideBar } from '../components/IndexSideBar';
import { ViewUnderAppBarBox, FullBoxContainer } from '../components/LayoutUtils';
import { QueryEditorActionBar } from '../components/QueryActionBar';
import { QueryEditor } from '../components/QueryEditor/QueryEditor';
import SearchResult from '../components/SearchResult/SearchResult';
import { useLocalStorage } from '../providers/LocalStorageProvider';
import { Client } from '../services/client';
import { EMPTY_SEARCH_REQUEST, IndexMetadata, SearchRequest, SearchResponse } from '../utils/models';
import { hasSearchParams, parseSearchUrl, toUrlSearchRequestParams } from '../utils/urls';

function SearchView() {
  const location = useLocation();
  const navigate = useNavigate();
  const [indexMetadata, setIndexMetadata] = useState<null | IndexMetadata>(null);
  const [searchResponse, setSearchResponse] = useState<null | SearchResponse>(null);
  const [queryRunning, setQueryRunning] = useState(false);
  const [searchRequest, setSearchRequest] = useState<SearchRequest>(hasSearchParams(location.search) ? parseSearchUrl(location.search) : EMPTY_SEARCH_REQUEST);
  const updateLastSearchRequest = useLocalStorage().updateLastSearchRequest;
  const quickwitClient = useMemo(() => new Client(), []);
  const runSearch = () => {
    console.log('Run search...', searchRequest);
    setQueryRunning(true);
    quickwitClient.search(searchRequest).then((response) => {
      navigate('/search?' + toUrlSearchRequestParams(searchRequest).toString());
      updateLastSearchRequest(searchRequest);
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
    // TODO: fix this shit.
    if (indexMetadata === null || indexMetadata.index_id === null || indexMetadata.index_id === undefined) {
      return;
    }
    setSearchRequest(previousRequest => {
      return {...previousRequest, indexId: indexMetadata.index_id}; 
    });
  }, [indexMetadata]);
  useEffect(() => {
    console.log('searchRequest', searchRequest);
    // Fix this shit.
    if (searchRequest.indexId == null || searchRequest.indexId == '') {
      return;
    }
    if (indexMetadata === null || indexMetadata === undefined) {
      console.log('update meta', searchRequest.indexId);
      quickwitClient.getIndex(searchRequest.indexId).then((fetchedIndexMetadata) => {
        console.log("set meta", searchRequest);
        setIndexMetadata(fetchedIndexMetadata);
      });
    }
  }, [searchRequest, quickwitClient]);

  return (
      <ViewUnderAppBarBox sx={{ flexDirection: 'row'}}>
        <IndexSideBar indexMetadata={indexMetadata} onIndexMetadataUpdate={onIndexMetadataUpdate}/>
        <FullBoxContainer sx={{ padding: 0}}>
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
          { ApiUrlFooter('api/v1/search?' + toUrlSearchRequestParams(searchRequest).toString()) }
        </FullBoxContainer>
      </ViewUnderAppBarBox>
  );
}

export default SearchView;
