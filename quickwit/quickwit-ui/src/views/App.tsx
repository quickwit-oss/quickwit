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

import TopBar from '../components/TopBar';
import { CssBaseline, ThemeProvider } from '@mui/material';
import SideBar from '../components/SideBar';
import { Navigate, Route, Routes } from 'react-router-dom';
import SearchView from './SearchView';
import IndexesView from './IndexesView';
import { theme } from '../utils/theme';
import IndexView from './IndexView';
import { FullBoxContainer } from '../components/LayoutUtils';
import { LocalStorageProvider } from '../providers/LocalStorageProvider';
import ClusterView from './ClusterView';
import NodeInfoView from './NodeInfoView';
import ApiView from './ApiView';

function App() {
  return (
    <ThemeProvider theme={theme}>
      <LocalStorageProvider>
        <FullBoxContainer sx={{flexDirection: 'row', p: 0}}>
          <CssBaseline />
          <TopBar />
          <SideBar />
          <Routes>
            <Route path="/" element={<Navigate to="/search" />} />
            <Route path="search" element={<SearchView />} />
            <Route path="indexes" element={<IndexesView />} />
            <Route path="indexes/:indexId" element={<IndexView />} />
            <Route path="cluster" element={<ClusterView />} />
            <Route path="node-info" element={<NodeInfoView />} />
            <Route path="api-playground" element={<ApiView />} />
          </Routes>
        </FullBoxContainer>
      </LocalStorageProvider>
    </ThemeProvider>
  );
}

export default App;
