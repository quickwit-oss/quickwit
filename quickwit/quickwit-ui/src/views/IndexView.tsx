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
import Link, { LinkProps } from "@mui/material/Link";
import React, { useCallback, useEffect, useMemo, useState } from "react";
import { Link as RouterLink, useParams } from "react-router";
import ApiUrlFooter from "../components/ApiUrlFooter";
import { IndexSummary } from "../components/IndexSummary";
import { JsonEditor } from "../components/JsonEditor";
import {
  FullBoxContainer,
  QBreadcrumbs,
  ViewUnderAppBarBox,
} from "../components/LayoutUtils";
import Loader from "../components/Loader";
import { Client } from "../services/client";
import { Index } from "../utils/models";

export type ErrorResult = {
  error: string;
};

const CustomTabPanel = styled(TabPanel)`
  padding-left: 0;
  padding-right: 0;
  height: 100%;
`;

// NOTE : https://mui.com/material-ui/react-breadcrumbs/#integration-with-react-router
interface LinkRouterProps extends LinkProps {
  to: string;
  replace?: boolean;
}

function LinkRouter(props: LinkRouterProps) {
  return <Link {...props} component={RouterLink} />;
}

function IndexView() {
  const { indexId } = useParams();
  const [loading, setLoading] = useState(false);
  const [, setLoadingError] = useState<ErrorResult | null>(null);
  const [tabIndex, setTabIndex] = useState("1");
  const [index, setIndex] = useState<Index>();
  const quickwitClient = useMemo(() => new Client(), []);

  const handleTabIndexChange = (_: React.SyntheticEvent, newValue: string) => {
    setTabIndex(newValue);
  };

  const fetchIndex = useCallback(() => {
    setLoading(true);
    if (indexId === undefined) {
      console.warn("`indexId` should always be set.");
      return;
    } else {
      quickwitClient.getIndex(indexId).then(
        (fetchedIndex) => {
          setLoadingError(null);
          setLoading(false);
          setIndex(fetchedIndex);
        },
        (error) => {
          setLoading(false);
          setLoadingError({ error: error });
        },
      );
    }
  }, [indexId, quickwitClient]);

  const renderFetchIndexResult = () => {
    if (loading || index === undefined) {
      return <Loader />;
    } else {
      // TODO: remove this css with magic number `48px`.
      return (
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            height: "calc(100% - 48px)",
          }}
        >
          <TabContext value={tabIndex}>
            <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
              <TabList onChange={handleTabIndexChange} aria-label="Index tabs">
                <Tab label="Summary" value="1" />
                <Tab label="Sources" value="2" />
                <Tab label="Doc Mapping" value="3" />
                <Tab label="Indexing settings" value="4" />
                <Tab label="Search settings" value="5" />
                <Tab label="Retention settings" value="6" />
                <Tab label="Splits" value="7" />
              </TabList>
            </Box>
            <CustomTabPanel value="1">
              <IndexSummary index={index} />
            </CustomTabPanel>
            <CustomTabPanel value="2">
              <JsonEditor
                content={index.metadata.sources}
                resizeOnMount={false}
              />
            </CustomTabPanel>
            <CustomTabPanel value="3">
              <JsonEditor
                content={index.metadata.index_config.doc_mapping}
                resizeOnMount={false}
              />
            </CustomTabPanel>
            <CustomTabPanel value="4">
              <JsonEditor
                content={index.metadata.index_config.indexing_settings}
                resizeOnMount={false}
              />
            </CustomTabPanel>
            <CustomTabPanel value="5">
              <JsonEditor
                content={index.metadata.index_config.search_settings}
                resizeOnMount={false}
              />
            </CustomTabPanel>
            <CustomTabPanel value="6">
              <JsonEditor
                content={index.metadata.index_config.retention || {}}
                resizeOnMount={false}
              />
            </CustomTabPanel>
            <CustomTabPanel value="7">
              <JsonEditor content={index.splits} resizeOnMount={false} />
            </CustomTabPanel>
          </TabContext>
        </Box>
      );
    }
  };

  useEffect(() => {
    fetchIndex();
  }, [fetchIndex]);

  return (
    <ViewUnderAppBarBox>
      <FullBoxContainer>
        <QBreadcrumbs aria-label="breadcrumb">
          <LinkRouter underline="hover" color="inherit" to="/indexes">
            <Typography color="text.primary">Indexes</Typography>
          </LinkRouter>
          <Typography color="text.primary">{indexId}</Typography>
        </QBreadcrumbs>
        {renderFetchIndexResult()}
      </FullBoxContainer>
      {ApiUrlFooter("api/v1/indexes/" + indexId)}
    </ViewUnderAppBarBox>
  );
}

export default IndexView;
