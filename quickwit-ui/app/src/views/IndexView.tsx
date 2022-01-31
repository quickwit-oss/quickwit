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

import { Box, styled, Typography, Breadcrumbs, Link, Tab } from '@mui/material';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { Client } from '../services/client';
import Loader from '../components/Loader';
import { useParams } from 'react-router-dom';
import { IndexMetadata } from '../utils/models';
import { TabContext, TabList, TabPanel } from '@mui/lab';
import { IndexSummary } from '../components/IndexSummary';
import { JsonEditor } from '../components/JsonEditor';
import { ViewUnderAppBarBox, FullBoxContainer, QBreadcrumbs } from '../components/LayoutUtils';

export type ErrorResult = {
  error: string;
}

const CustomTabPanel = styled(TabPanel)`
padding-left: 0;
padding-right: 0;
height: 100%;
`

function IndexView() {
  let { indexId } = useParams();

  const [loading, setLoading] = useState(false)
  const [, setLoadingError] = useState<ErrorResult | null>(null)
  const [tabIndex, setTabIndex] = useState('1');
  const [indexMetadata, setIndexMetadata] = useState<IndexMetadata>()
  const quickwitClient = useMemo(() => new Client(), []);

  const handleTabIndexChange = (event: React.SyntheticEvent, newValue: string) => {
    setTabIndex(newValue);
  };

  const fetchIndexMetadata = useCallback(() => {
    setLoading(true);
    if (indexId === undefined) {
      console.warn("`indexId` should always be set.");
      return;
    } else {
      quickwitClient.getIndex(indexId).then(
        (indexMetadata) => {
          setLoadingError(null);
          setLoading(false);
          setIndexMetadata(indexMetadata);
        },
        (error) => {
          setLoading(false);
          setLoadingError({error: error});
        }
      );
    }
  }, [indexId, quickwitClient]);

  const renderFetchIndexResult = () => {
    if (loading || indexMetadata === undefined) {
      return <Loader />;
    } else {
      return <Box sx={{ display: 'flex', flexDirection: 'column', height: 'calc(100% - 48px)' }}>
        <TabContext value={tabIndex}>
          <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
            <TabList onChange={handleTabIndexChange} aria-label="lab API tabs example">
              <Tab label="Summary" value="1" />
              <Tab label="Sources" value="2" />
              <Tab label="Doc Mapping" value="3" />
              <Tab label="Indexing settings" value="4" />
              <Tab label="Search settings" value="5" />
              <Tab label="Splits" value="6" />
            </TabList>
          </Box>
          <CustomTabPanel value="1">
            <IndexSummary indexMetadata={indexMetadata} />
          </CustomTabPanel>
          <CustomTabPanel value="2">
            <JsonEditor content={indexMetadata.sources} />
          </CustomTabPanel>
          <CustomTabPanel value="3">
            <JsonEditor content={indexMetadata.doc_mapping} />
          </CustomTabPanel>
          <CustomTabPanel value="4">
            <JsonEditor content={indexMetadata.indexing_settings} />
          </CustomTabPanel>
          <CustomTabPanel value="5">
            <JsonEditor content={indexMetadata.search_settings} />
          </CustomTabPanel>
          <CustomTabPanel value="6">
            <JsonEditor content={[]} />
          </CustomTabPanel>
        </TabContext>
      </Box>
    }
  }

  useEffect(() => {
    fetchIndexMetadata();
  }, [fetchIndexMetadata]);

  return (
    <ViewUnderAppBarBox>
      <FullBoxContainer>
        <QBreadcrumbs aria-label="breadcrumb">
          <Link underline="hover" color="inherit" href="/indexes">
            <Typography color="text.primary">Indexes</Typography>
          </Link>
          <Typography color="text.primary">{indexId}</Typography>
        </QBreadcrumbs>
        { renderFetchIndexResult() }
      </FullBoxContainer>
    </ViewUnderAppBarBox>
  );
}

export default IndexView;
