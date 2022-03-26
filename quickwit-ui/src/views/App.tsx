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

import TopBar from '../components/TopBar';
import { CssBaseline, ThemeProvider } from '@mui/material';
import SideBar from '../components/SideBar';
import { Navigate, Route, Routes } from 'react-router-dom';
import SearchView from './SearchView';
import IndexesView from './IndexesView';
import ClusterMembersView from './ClusterMembersView';
import { theme } from '../utils/theme';
import IndexView from './IndexView';
import { FullBoxContainer } from '../components/LayoutUtils';
import { LocalStorageProvider } from '../providers/LocalStorageProvider';

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
            <Route path="cluster/members" element={<ClusterMembersView />} />
          </Routes>
        </FullBoxContainer>
      </LocalStorageProvider>
    </ThemeProvider>
  );
}

export default App;
