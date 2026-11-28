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
import { Box, Button, Tab, Tabs, TextField } from "@mui/material";
import { MAX_HITS_OPTIONS } from "../utils/models";
import { SearchComponentProps } from "../utils/SearchComponentProps";
import { TimeRangeSelect } from "./TimeRangeSelect";

export function QueryEditorActionBar(props: SearchComponentProps) {
  const timestamp_field_name =
    props.index?.metadata.index_config.doc_mapping.timestamp_field;
  const shouldDisplayTimeRangeSelect = timestamp_field_name ?? false;
  const searchDisabled = props.queryRunning || !props.searchRequest.indexId;

  const handleChange = (_event: React.SyntheticEvent, newTab: number) => {
    const updatedSearchRequest = {
      ...props.searchRequest,
      aggregation: newTab !== 0,
    };
    props.onSearchRequestUpdate(updatedSearchRequest);
    props.runSearch(updatedSearchRequest);
  };

  return (
    <Box sx={{ display: "flex", flexWrap: "wrap" }}>
      <Box sx={{ flexGrow: 0, padding: "10px" }}>
        <Button
          onClick={() => props.runSearch(props.searchRequest)}
          variant="contained"
          startIcon={<PlayArrowIcon />}
          disableElevation
          sx={{ flexGrow: 1 }}
          disabled={searchDisabled}
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
      {!props.searchRequest.aggregation && (
        <TextField
          id="max-hits"
          label="Max hits"
          select
          size="small"
          value={props.searchRequest.maxHits}
          onChange={(event) => {
            props.runSearch({
              ...props.searchRequest,
              maxHits: Number(event.target.value),
            });
          }}
          disabled={searchDisabled}
          slotProps={{ select: { native: true } }}
          sx={{ alignSelf: "center", minWidth: 96, mx: 1 }}
        >
          {MAX_HITS_OPTIONS.map((maxHits) => (
            <option key={maxHits} value={maxHits}>
              {maxHits}
            </option>
          ))}
        </TextField>
      )}
      {shouldDisplayTimeRangeSelect && (
        <TimeRangeSelect
          timeRange={{
            startTimestamp: props.searchRequest.startTimestamp,
            endTimestamp: props.searchRequest.endTimestamp,
          }}
          onUpdate={(timeRange) => {
            props.runSearch({ ...props.searchRequest, ...timeRange });
          }}
          disabled={searchDisabled}
        />
      )}
    </Box>
  );
}
