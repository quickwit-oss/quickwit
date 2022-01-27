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

import { Box } from '@mui/system';
import { useEffect, useState } from 'react';
import { IndexSideBar } from '../components/IndexSideBar';
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
      <Box flexDirection="row" display="flex" sx={{marginTop: '48px', height: 'calc(100% - 48px)', width: '100%'}}>
        <IndexSideBar indexMetadata={indexMetadata} onIndexMetadataUpdate={onIndexMetadataUpdate}/>
        <Box sx={{ py: 1, px: 1, height: '100%', width: '100%', display: 'flex', flexDirection: 'column' }}>
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
        </Box>
      </Box>
  );
}

export default SearchView;
