// Copyright (C) 2022 Quickwit, Inc.
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

import { TabContext, TabList, TabPanel } from '@mui/lab';
import { Box, Tab, Typography, styled } from '@mui/material';
import { useEffect, useMemo, useState } from 'react';
import ApiUrlFooter from '../components/ApiUrlFooter';
import { JsonEditor } from '../components/JsonEditor';
import { ViewUnderAppBarBox, FullBoxContainer, QBreadcrumbs } from '../components/LayoutUtils';
import Loader from '../components/Loader';
import { Client } from '../services/client';
import { QuickwitBuildInfo } from '../utils/models';

const CustomTabPanel = styled(TabPanel)`
padding-left: 0;
padding-right: 0;
height: 100%;
`;

function NodeInfoView() {
  const [loadingCounter, setLoadingCounter] = useState(2);
  const [nodeId, setNodeId] = useState<string>("");
  // eslint-disable-next-line
  const [nodeConfig, setNodeConfig] = useState<null | Record<string, any>>(null);
  const [buildInfo, setBuildInfo] = useState<null | QuickwitBuildInfo>(null);
  const [tabIndex, setTabIndex] = useState('1');
  const quickwitClient = useMemo(() => new Client(), []);

  const urlByTab: Record<string, string> = {
    '1': 'api/v1/config',
    '2': 'api/v1/build',
  }

  const handleTabIndexChange = (_: React.SyntheticEvent, newValue: string) => {
    setTabIndex(newValue);
  };

  useEffect(() => {
    quickwitClient.cluster().then(
      (cluster) => {
        setNodeId(cluster.node_id);
      },
      (error) => {
        console.log('Error when fetching cluster info:', error);
      }
    )
  });
  useEffect(() => {
    setLoadingCounter(2);
    quickwitClient.buildInfo().then(
      (fetchedBuildInfo) => {
        setLoadingCounter(prevCounter => prevCounter - 1);
        setBuildInfo(fetchedBuildInfo);
      },
      (error) => {
        setLoadingCounter(prevCounter => prevCounter - 1);
        console.log('Error when fetching build info: ', error);
      }
    );
    quickwitClient.config().then(
      (fetchedConfig) => {
        setLoadingCounter(prevCounter => prevCounter - 1);
        setNodeConfig(fetchedConfig);
      },
      (error) => {
        setLoadingCounter(prevCounter => prevCounter - 1);
        console.log('Error when fetching node config: ', error);
      }
    );
  }, [quickwitClient]);

  const renderResult = () => {
    if (loadingCounter !== 0) {
      return <Loader />;
    } else {
      return <FullBoxContainer sx={{ px: 0 }}>
        <TabContext value={tabIndex}>
          <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
            <TabList onChange={handleTabIndexChange} aria-label="Index tabs">
              <Tab label="Node config" value="1" />
              <Tab label="Build info" value="2" />
            </TabList>
          </Box>
          <CustomTabPanel value="1">
            <JsonEditor content={nodeConfig} resizeOnMount={false} />
          </CustomTabPanel>
          <CustomTabPanel value="2">
            <JsonEditor content={buildInfo} resizeOnMount={false} />
          </CustomTabPanel>
        </TabContext>
      </FullBoxContainer>
    }
  }

  return (
    <ViewUnderAppBarBox>
      <FullBoxContainer>
        <QBreadcrumbs aria-label="breadcrumb">
          <Typography color="text.primary">Node ID: {nodeId} (self)</Typography>
        </QBreadcrumbs>
        { renderResult() }
      </FullBoxContainer>
      { ApiUrlFooter(urlByTab[tabIndex] || '') }
    </ViewUnderAppBarBox>
  );
}

export default NodeInfoView;
