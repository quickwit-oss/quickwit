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

import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import { Box, Button, Tab, Tabs } from "@mui/material";
import { SearchComponentProps } from "../utils/SearchComponentProps";
import { TimeRangeSelect } from "./TimeRangeSelect";

export function QueryEditorActionBar(props: SearchComponentProps) {
  const timestamp_field_name =
    props.index?.metadata.index_config.doc_mapping.timestamp_field;
  const shouldDisplayTimeRangeSelect = timestamp_field_name ?? false;

  const handleChange = (_event: React.SyntheticEvent, newTab: number) => {
    const updatedSearchRequest = {
      ...props.searchRequest,
      aggregation: newTab !== 0,
    };
    props.onSearchRequestUpdate(updatedSearchRequest);
    props.runSearch(updatedSearchRequest);
  };

  return (
    <Box sx={{ display: "flex" }}>
      <Box sx={{ flexGrow: 0, padding: "10px" }}>
        <Button
          onClick={() => props.runSearch(props.searchRequest)}
          variant="contained"
          startIcon={<PlayArrowIcon />}
          disableElevation
          sx={{ flexGrow: 1 }}
          disabled={props.queryRunning || !props.searchRequest.indexId}
        >
          Run
        </Button>
      </Box>
      <Box sx={{ flexGrow: 0 }}>
        <Box sx={{ borderBottom: 1, borderColor: "divider", flexGrow: 1 }}>
          <Tabs
            value={Number(props.searchRequest.aggregation)}
            onChange={handleChange}
          >
            <Tab label="Search" />
            <Tab label="Aggregation" />
          </Tabs>
        </Box>
      </Box>
      <Box sx={{ flexGrow: 1 }}></Box>
      {shouldDisplayTimeRangeSelect && (
        <TimeRangeSelect
          timeRange={{
            startTimestamp: props.searchRequest.startTimestamp,
            endTimestamp: props.searchRequest.endTimestamp,
          }}
          onUpdate={(timeRange) => {
            props.runSearch({ ...props.searchRequest, ...timeRange });
          }}
          disabled={props.queryRunning || !props.searchRequest.indexId}
        />
      )}
    </Box>
  );
}
