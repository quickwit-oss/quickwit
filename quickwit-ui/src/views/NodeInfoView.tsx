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

import { Typography } from '@mui/material';
import { useEffect, useMemo, useState } from 'react';
import ApiUrlFooter from '../components/ApiUrlFooter';
import { JsonEditor } from '../components/JsonEditor';
import { ViewUnderAppBarBox, FullBoxContainer, QBreadcrumbs } from '../components/LayoutUtils';
import Loader from '../components/Loader';
import ErrorResponseDisplay from '../components/ResponseErrorDisplay';
import { Client } from '../services/client';
import { QuickwitBuildInfo, ResponseError } from '../utils/models';


function NodeInfoView() {
  const [loading, setLoading] = useState(false);
  const [nodeId, setNodeId] = useState<string>("");
  // eslint-disable-next-line
  const [nodeConfig, setNodeConfig] = useState<null | Record<string, any>>(null);
  const [configResponseError, setConfigResponseError] = useState<ResponseError | null>(null);
  const [buildInfo, setBuildInfo] = useState<null | QuickwitBuildInfo>(null);
  const [buildInfoResponseError, setBuildInfoResponseError] = useState<ResponseError | null>(null);
  const quickwitClient = useMemo(() => new Client(), []);

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
    setLoading(true);
    quickwitClient.buildInfo().then(
      (fetchedBuildInfo) => {
        setBuildInfoResponseError(null);
        setLoading(false);
        setBuildInfo(fetchedBuildInfo);
      },
      (error) => {
        setLoading(false);
        setBuildInfoResponseError(error);
      }
    );
    quickwitClient.config().then(
      (fetchedConfig) => {
        setConfigResponseError(null);
        setLoading(false);
        setNodeConfig(fetchedConfig);
      },
      (error) => {
        setLoading(false);
        setConfigResponseError(error);
      }
    );
  }, [quickwitClient]);

  const renderConfigResult = () => {
    if (configResponseError !== null) {
      return ErrorResponseDisplay(configResponseError);
    }
    if (loading || nodeConfig == null) {
      return <Loader />;
    }
    return <JsonEditor content={nodeConfig} resizeOnMount={false} />
  }

  const renderBuildInfoResult = () => {
    if (buildInfoResponseError !== null) {
      return ErrorResponseDisplay(buildInfoResponseError);
    }
    if (loading || buildInfo == null) {
      return <Loader />;
    }
    return <JsonEditor content={buildInfo} resizeOnMount={false} />
  }

  return (
    <ViewUnderAppBarBox>
      <FullBoxContainer>
        <QBreadcrumbs aria-label="breadcrumb">
          <Typography color="text.primary">Node ID: {nodeId} (self)</Typography>
        </QBreadcrumbs>
        <Typography mt={2} color="text.secondary">Build Info</Typography>
        <FullBoxContainer sx={{ px: 0 }}>
          { renderBuildInfoResult() }
        </FullBoxContainer>
        <Typography color="text.secondary">Config</Typography>
        <FullBoxContainer sx={{ px: 0 }}>
          { renderConfigResult() }
        </FullBoxContainer>
      </FullBoxContainer>
      { ApiUrlFooter('api/v1/config') }
    </ViewUnderAppBarBox>
  );
}

export default NodeInfoView;
