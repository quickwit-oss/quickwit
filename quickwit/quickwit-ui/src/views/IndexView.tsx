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
import { useCallback, useEffect, useMemo, useState } from "react";
import { Link as RouterLink, useParams, useSearchParams } from "react-router";
import ApiUrlFooter from "../components/ApiUrlFooter";
import { IndexSummary } from "../components/IndexSummary";
import { JsonEditor } from "../components/JsonEditor";
import { JsonEditorEditable } from "../components/JsonEditorEditable";
import {
  FullBoxContainer,
  QBreadcrumbs,
  ViewUnderAppBarBox,
} from "../components/LayoutUtils";
import Loader from "../components/Loader";
import { Client } from "../services/client";
import { useJsonSchema } from "../services/jsonShema";
import { Index, IndexMetadata } from "../utils/models";

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
  const { loading, updating, index, updateIndexConfig } = useIndex(indexId);
  const [searchParams, setSearchParams] = useSearchParams();

  const validTabs = [
    "summary",
    "sources",
    "doc-mapping",
    "indexing-settings",
    "search-settings",
    "retention-settings",
    "splits",
  ] as const;

  type TabValue = (typeof validTabs)[number];

  const isValidTab = (value: string | null): value is TabValue => {
    return validTabs.includes(value as TabValue);
  };

  const tabFromUrl = searchParams.get("tab");
  const tab = isValidTab(tabFromUrl) ? tabFromUrl : "summary";

  const setTab = (newTab: TabValue) => {
    setSearchParams({ tab: newTab });
  };

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
          <TabContext value={tab}>
            <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
              <TabList
                onChange={(_, newTab) => setTab(newTab)}
                aria-label="Index tabs"
              >
                <Tab label="Summary" value="summary" />
                <Tab label="Sources" value="sources" />
                <Tab label="Doc Mapping" value="doc-mapping" />
                <Tab label="Indexing settings" value="indexing-settings" />
                <Tab label="Search settings" value="search-settings" />
                <Tab label="Retention settings" value="retention-settings" />
                <Tab label="Splits" value="splits" />
              </TabList>
            </Box>
            <CustomTabPanel value="summary">
              <SummaryTab index={index} />
            </CustomTabPanel>
            <CustomTabPanel value="sources">
              <SourcesTab index={index} />
            </CustomTabPanel>
            <CustomTabPanel value="doc-mapping">
              <DocMappingTab index={index} />
            </CustomTabPanel>
            <CustomTabPanel value="indexing-settings">
              <IndexingSettingsTab
                index={index}
                updateIndexConfig={updateIndexConfig}
                updating={updating}
              />
            </CustomTabPanel>
            <CustomTabPanel value="search-settings">
              <SearchSettingsTab
                index={index}
                updateIndexConfig={updateIndexConfig}
                updating={updating}
              />
            </CustomTabPanel>
            <CustomTabPanel value="retention-settings">
              <RetentionSettingsTab
                index={index}
                updateIndexConfig={updateIndexConfig}
                updating={updating}
              />
            </CustomTabPanel>
            <CustomTabPanel value="splits">
              <SplitsTab index={index} />
            </CustomTabPanel>
          </TabContext>
        </Box>
      );
    }
  };

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

function SummaryTab({ index }: { index: Index }) {
  return <IndexSummary index={index} />;
}

function SourcesTab({ index }: { index: Index }) {
  const jsonSchema =
    useJsonSchema(
      "#/components/schemas/IndexMetadataV0_8/properties/sources",
    ) ?? undefined;

  return (
    <JsonEditor
      content={index.metadata.sources}
      resizeOnMount={false}
      jsonSchema={jsonSchema}
    />
  );
}

function DocMappingTab({ index }: { index: Index }) {
  const jsonSchema =
    useJsonSchema("#/components/schemas/DocMapping") ?? undefined;
  return (
    <JsonEditor
      content={index.metadata.index_config.doc_mapping}
      resizeOnMount={false}
      jsonSchema={jsonSchema}
    />
  );
}

function IndexingSettingsTab({
  index,
  updateIndexConfig,
  updating,
}: {
  index: Index;
  updateIndexConfig: (indexConfig: IndexMetadata["index_config"]) => void;
  updating: boolean;
}) {
  const jsonSchema =
    useJsonSchema("#/components/schemas/IndexingSettings") ?? undefined;

  const initialValue = index.metadata.index_config.indexing_settings;
  const [edited, setEdited] = useState<any | null>(null);
  const pristine =
    edited === null || JSON.stringify(edited) === JSON.stringify(initialValue);

  return (
    <JsonEditorEditable
      content={initialValue}
      resizeOnMount={false}
      jsonSchema={jsonSchema}
      onContentEdited={setEdited}
      onSave={() =>
        updateIndexConfig({
          ...index.metadata.index_config,
          indexing_settings: edited,
        } as IndexMetadata["index_config"])
      }
      pristine={pristine}
      saving={updating}
    />
  );
}

function SearchSettingsTab({
  index,
  updateIndexConfig,
  updating,
}: {
  index: Index;
  updateIndexConfig: (indexConfig: IndexMetadata["index_config"]) => void;
  updating: boolean;
}) {
  const jsonSchema =
    useJsonSchema("#/components/schemas/SearchSettings") ?? undefined;

  const initialValue = index.metadata.index_config.search_settings;
  const [edited, setEdited] = useState<any | null>(null);
  const pristine =
    edited === null || JSON.stringify(edited) === JSON.stringify(initialValue);

  return (
    <JsonEditorEditable
      content={initialValue}
      resizeOnMount={false}
      jsonSchema={jsonSchema}
      onContentEdited={setEdited}
      onSave={() =>
        updateIndexConfig({
          ...index.metadata.index_config,
          search_settings: edited,
        } as IndexMetadata["index_config"])
      }
      pristine={pristine}
      saving={updating}
    />
  );
}

function RetentionSettingsTab({
  index,
  updateIndexConfig,
  updating,
}: {
  index: Index;
  updateIndexConfig: (indexConfig: IndexMetadata["index_config"]) => void;
  updating: boolean;
}) {
  const jsonSchema =
    useJsonSchema("#/components/schemas/RetentionPolicy") ?? undefined;

  const initialValue = index.metadata.index_config.retention || {};
  const [edited, setEdited] = useState<any | null>(null);
  const pristine =
    edited === null || JSON.stringify(edited) === JSON.stringify(initialValue);

  return (
    <JsonEditorEditable
      content={initialValue}
      resizeOnMount={false}
      jsonSchema={jsonSchema}
      onContentEdited={setEdited}
      onSave={() =>
        updateIndexConfig({
          ...index.metadata.index_config,
          retention: edited,
        } as IndexMetadata["index_config"])
      }
      pristine={pristine}
      saving={updating}
    />
  );
}

function SplitsTab({ index }: { index: Index }) {
  const splitShema = useJsonSchema("#/components/schemas/Split");
  const jsonSchema =
    (splitShema && {
      ...splitShema,
      $ref: undefined,
      type: "array",
      items: { $ref: "#/components/schemas/Split" },
    }) ??
    undefined;

  return (
    <JsonEditor
      content={index.splits}
      resizeOnMount={false}
      jsonSchema={jsonSchema}
    />
  );
}

/**
 * Fetches and manages index data
 */
const useIndex = (indexId: string | undefined) => {
  const quickwitClient = useMemo(() => new Client(), []);

  const onError = useMemo(
    () => (err: unknown) => alert((err as any)?.message ?? err?.toString()),
    [],
  );

  const [index, setIndex] = useState<Index>();
  const [updating, setUpdating] = useState(false);

  useEffect(() => {
    if (!indexId) return;

    const abortController = new AbortController();

    quickwitClient
      .getIndex(indexId)
      .then((index) => {
        if (!abortController.signal.aborted) setIndex(index);
      })
      .catch(onError);

    return () => abortController.abort();
  }, [indexId, quickwitClient, onError]);

  const updateIndexConfig = useCallback(
    (indexConfig: IndexMetadata["index_config"]) => {
      setUpdating(true);

      quickwitClient
        .updateIndexConfig(indexConfig.index_id, indexConfig)
        .then((metadata) => {
          setIndex((i) =>
            i?.metadata.index_config.index_id === metadata.index_config.index_id
              ? { ...i, metadata }
              : i,
          );
        })
        .catch(onError)
        .finally(() => setUpdating(false));
    },
    [quickwitClient, onError],
  );

  if (!indexId) return { loading: false, index: undefined };

  if (index?.metadata.index_config.index_id !== indexId)
    return { loading: true };

  return { loading: false, updating, index: index, updateIndexConfig };
};
