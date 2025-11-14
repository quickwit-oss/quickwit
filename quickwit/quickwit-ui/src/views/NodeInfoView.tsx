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

import { TabContext, TabList, TabPanel } from "@mui/lab";
import { Box, styled, Tab, Typography } from "@mui/material";
import { useEffect, useMemo, useState } from "react";
import ApiUrlFooter from "../components/ApiUrlFooter";
import { JsonEditor } from "../components/JsonEditor";
import {
  FullBoxContainer,
  QBreadcrumbs,
  ViewUnderAppBarBox,
} from "../components/LayoutUtils";
import Loader from "../components/Loader";
import { Client } from "../services/client";
import { QuickwitBuildInfo } from "../utils/models";

const CustomTabPanel = styled(TabPanel)`
  padding-left: 0;
  padding-right: 0;
  height: 100%;
`;

function NodeInfoView() {
  const [loadingCounter, setLoadingCounter] = useState(2);
  const [nodeId, setNodeId] = useState<string>("");
  const [nodeConfig, setNodeConfig] = useState<null | Record<string, any>>(
    null,
  );
  const [buildInfo, setBuildInfo] = useState<null | QuickwitBuildInfo>(null);
  const [tabIndex, setTabIndex] = useState("1");
  const quickwitClient = useMemo(() => new Client(), []);

  const urlByTab: Record<string, string> = {
    "1": "api/v1/config",
    "2": "api/v1/version",
  };

  const handleTabIndexChange = (_: React.SyntheticEvent, newValue: string) => {
    setTabIndex(newValue);
  };

  useEffect(() => {
    quickwitClient.cluster().then(
      (cluster) => {
        setNodeId(cluster.node_id);
      },
      (error) => {
        console.log("Error when fetching cluster info:", error);
      },
    );
  });
  useEffect(() => {
    setLoadingCounter(2);
    quickwitClient.buildInfo().then(
      (fetchedBuildInfo) => {
        setLoadingCounter((prevCounter) => prevCounter - 1);
        setBuildInfo(fetchedBuildInfo);
      },
      (error) => {
        setLoadingCounter((prevCounter) => prevCounter - 1);
        console.log("Error when fetching build info: ", error);
      },
    );
    quickwitClient.config().then(
      (fetchedConfig) => {
        setLoadingCounter((prevCounter) => prevCounter - 1);
        setNodeConfig(fetchedConfig);
      },
      (error) => {
        setLoadingCounter((prevCounter) => prevCounter - 1);
        console.log("Error when fetching node config: ", error);
      },
    );
  }, [quickwitClient]);

  const renderResult = () => {
    if (loadingCounter !== 0) {
      return <Loader />;
    } else {
      return (
        <FullBoxContainer sx={{ px: 0 }}>
          <TabContext value={tabIndex}>
            <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
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
      );
    }
  };

  return (
    <ViewUnderAppBarBox>
      <FullBoxContainer>
        <QBreadcrumbs aria-label="breadcrumb">
          <Typography color="text.primary">Node ID: {nodeId} (self)</Typography>
        </QBreadcrumbs>
        {renderResult()}
      </FullBoxContainer>
      {ApiUrlFooter(urlByTab[tabIndex] || "")}
    </ViewUnderAppBarBox>
  );
}

export default NodeInfoView;
