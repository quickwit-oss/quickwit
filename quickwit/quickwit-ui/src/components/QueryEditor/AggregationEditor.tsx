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

import { Box } from "@mui/material";
import FormControl from "@mui/material/FormControl";
import MenuItem from "@mui/material/MenuItem";
import Select, { SelectChangeEvent } from "@mui/material/Select";
import TextField from "@mui/material/TextField";
import { useEffect, useRef, useState } from "react";
import { HistogramAgg, TermAgg } from "../../utils/models";
import { SearchComponentProps } from "../../utils/SearchComponentProps";

export function AggregationEditor(props: SearchComponentProps) {
  return (
    <Box hidden={!props.searchRequest.aggregation}>
      <MetricKind
        searchRequest={props.searchRequest}
        onSearchRequestUpdate={props.onSearchRequestUpdate}
        runSearch={props.runSearch}
        index={props.index}
        queryRunning={props.queryRunning}
      />
      <AggregationKind
        searchRequest={props.searchRequest}
        onSearchRequestUpdate={props.onSearchRequestUpdate}
        runSearch={props.runSearch}
        index={props.index}
        queryRunning={props.queryRunning}
      />
    </Box>
  );
}

export function MetricKind(props: SearchComponentProps) {
  // TODO add percentiles
  const metricRef = useRef(props.searchRequest.aggregationConfig.metric);

  const handleTypeChange = (event: SelectChangeEvent) => {
    const value = event.target.value;
    const updatedMetric =
      value !== "count" ? { ...metricRef.current!, type: value } : null;
    const updatedAggregation = {
      ...props.searchRequest.aggregationConfig,
      metric: updatedMetric,
    };
    const updatedSearchRequest = {
      ...props.searchRequest,
      aggregationConfig: updatedAggregation,
    };
    props.onSearchRequestUpdate(updatedSearchRequest);
    metricRef.current = updatedMetric;
  };

  const handleNameChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const value = event.target.value;
    if (metricRef.current == null) {
      return;
    }
    const updatedMetric = { ...metricRef.current!, field: value };
    const updatedAggregation = {
      ...props.searchRequest.aggregationConfig,
      metric: updatedMetric,
    };
    const updatedSearchRequest = {
      ...props.searchRequest,
      aggregationConfig: updatedAggregation,
    };
    props.onSearchRequestUpdate(updatedSearchRequest);
    metricRef.current = updatedMetric;
  };

  return (
    <Box sx={{ m: 1, minWidth: 120, display: "flex", flexDirection: "row" }}>
      <FormControl variant="standard">
        <Select
          value={metricRef.current ? metricRef.current.type : "count"}
          onChange={handleTypeChange}
          sx={{ minHeight: "44px" }}
        >
          <MenuItem value="count">Count</MenuItem>
          <MenuItem value="avg">Average</MenuItem>
          <MenuItem value="sum">Sum</MenuItem>
          <MenuItem value="max">Max</MenuItem>
          <MenuItem value="min">Min</MenuItem>
        </Select>
      </FormControl>
      <FormControl variant="standard">
        <TextField
          variant="standard"
          label="Field"
          onChange={handleNameChange}
          sx={{
            marginLeft: "10px",
            ...(!metricRef.current && { display: "none" }),
          }}
        />
      </FormControl>
    </Box>
  );
}

export function AggregationKind(props: SearchComponentProps) {
  const defaultAgg = {
    histogram: {
      interval: "1d",
    },
  };
  const [aggregations, setAggregations] = useState<
    ({ term: TermAgg } | { histogram: HistogramAgg })[]
  >([defaultAgg]);

  useEffect(() => {
    // do the initial filling of parameters
    const aggregationConfig = props.searchRequest.aggregationConfig;
    if (
      aggregationConfig.histogram === null &&
      aggregationConfig.term === null
    ) {
      const initialAggregation = Object.assign({}, ...aggregations);
      const initialSearchRequest = {
        ...props.searchRequest,
        aggregationConfig: initialAggregation,
      };
      props.onSearchRequestUpdate(initialSearchRequest);
    }
  }, []); // Empty dependency array means this runs once after mount

  useEffect(() => {
    // Update search request whenever aggregations change
    const metric = props.searchRequest.aggregationConfig.metric;
    const updatedAggregation = Object.assign(
      {},
      { metric: metric },
      ...aggregations,
    );
    const updatedSearchRequest = {
      ...props.searchRequest,
      aggregationConfig: updatedAggregation,
    };
    props.onSearchRequestUpdate(updatedSearchRequest);
  }, [aggregations]);

  const handleAggregationChange = (pos: number, event: SelectChangeEvent) => {
    const value = event.target.value;
    setAggregations((agg) => {
      const newAggregations = [...agg];
      switch (value) {
        case "histogram": {
          newAggregations[pos] = {
            histogram: {
              interval: "1d",
            },
          };
          break;
        }
        case "term": {
          newAggregations[pos] = {
            term: {
              field: "",
              size: 10,
            },
          };
          break;
        }
        case "rm": {
          newAggregations.splice(pos, 1);
        }
      }
      return newAggregations;
    });
  };

  const handleHistogramChange = (pos: number, event: SelectChangeEvent) => {
    const value = event.target.value;
    setAggregations((agg) => {
      const newAggregations = [...agg];
      newAggregations[pos] = { histogram: { interval: value } };
      return newAggregations;
    });
  };

  const handleTermFieldChange = (
    pos: number,
    event: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>,
  ) => {
    const value = event.target.value;
    setAggregations((agg) => {
      const newAggregations = [...agg];
      const term = newAggregations[pos];
      if (isTerm(term)) {
        term.term.field = value;
      }
      return newAggregations;
    });
  };

  const handleTermCountChange = (
    pos: number,
    event: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>,
  ) => {
    const value = event.target.value;
    setAggregations((agg) => {
      const newAggregations = [...agg];
      const term = newAggregations[pos];
      if (isTerm(term)) {
        term.term.size = Number(value);
      }
      return newAggregations;
    });
  };

  function isHistogram(
    agg: { term: TermAgg } | { histogram: HistogramAgg } | undefined,
  ): agg is { histogram: HistogramAgg } {
    if (!agg) return false;
    return "histogram" in agg;
  }

  function isTerm(
    agg: { term: TermAgg } | { histogram: HistogramAgg } | undefined,
  ): agg is { term: TermAgg } {
    if (!agg) return false;
    return "term" in agg;
  }

  const getAggregationKind = (
    agg: { term: TermAgg } | { histogram: HistogramAgg } | undefined,
  ) => {
    if (isHistogram(agg)) {
      return "histogram";
    }
    if (isTerm(agg)) {
      return "term";
    }
    return "new";
  };

  const makeOptions = (
    pos: number,
    agg: ({ term: TermAgg } | { histogram: HistogramAgg })[],
  ) => {
    const options = [];
    if (pos >= agg.length) {
      options.push(
        <MenuItem value="new" key="new">
          Add aggregation
        </MenuItem>,
      );
    }
    let addHistogram = true;
    let addTerm = true;
    for (let i = 0; i < agg.length; i++) {
      if (i === pos) continue;
      if (getAggregationKind(agg[i]) === "histogram") addHistogram = false;
      if (getAggregationKind(agg[i]) === "term") addTerm = false;
    }
    if (addHistogram) {
      options.push(
        <MenuItem value="histogram" key="histogram">
          Histogram aggregation
        </MenuItem>,
      );
    }
    if (addTerm) {
      options.push(
        <MenuItem value="term" key="term">
          Term aggregation
        </MenuItem>,
      );
    }
    if (agg.length > 1) {
      options.push(
        <MenuItem value="rm" key="rm">
          Remove aggregation
        </MenuItem>,
      );
    }
    return options;
  };

  const drawAdditional = (
    pos: number,
    aggs: ({ term: TermAgg } | { histogram: HistogramAgg })[],
  ) => {
    const agg = aggs[pos];
    if (isHistogram(agg)) {
      return (
        <FormControl variant="standard">
          <Select
            value={agg.histogram.interval}
            onChange={(e) => handleHistogramChange(pos, e)}
            sx={{ marginLeft: "10px", minHeight: "44px" }}
          >
            <MenuItem value="10s">10 seconds</MenuItem>
            <MenuItem value="1m">1 minute</MenuItem>
            <MenuItem value="5m">5 minutes</MenuItem>
            <MenuItem value="10m">10 minutes</MenuItem>
            <MenuItem value="1h">1 hour</MenuItem>
            <MenuItem value="1d">1 day</MenuItem>
          </Select>
        </FormControl>
      );
    }
    if (isTerm(agg)) {
      return (
        <>
          <FormControl variant="standard">
            <TextField
              variant="standard"
              label="Field"
              onChange={(e) => handleTermFieldChange(pos, e)}
              sx={{ marginLeft: "10px" }}
            />
          </FormControl>
          <FormControl variant="standard">
            <TextField
              variant="standard"
              label="Return top"
              type="number"
              onChange={(e) => handleTermCountChange(pos, e)}
              value={agg.term.size}
              sx={{ marginLeft: "10px" }}
            />
          </FormControl>
        </>
      );
    }
    return null;
  };

  return (
    <>
      <Box sx={{ m: 1, minWidth: 120, display: "flex", flexDirection: "row" }}>
        <FormControl variant="standard">
          <Select
            value={getAggregationKind(aggregations[0])}
            onChange={(e) => handleAggregationChange(0, e)}
            sx={{ minHeight: "44px", width: "190px" }}
          >
            {makeOptions(0, aggregations)}
          </Select>
        </FormControl>
        {drawAdditional(0, aggregations)}
      </Box>
      <Box sx={{ m: 1, minWidth: 120, display: "flex", flexDirection: "row" }}>
        <FormControl
          variant="standard"
          sx={{ m: 1, minWidth: 120, display: "flex", flexDirection: "row" }}
        >
          <Select
            value={getAggregationKind(aggregations[1])}
            onChange={(e) => handleAggregationChange(1, e)}
            sx={{ minHeight: "44px", width: "190px" }}
          >
            {makeOptions(1, aggregations)}
          </Select>
          {drawAdditional(1, aggregations)}
        </FormControl>
      </Box>
    </>
  );
}
