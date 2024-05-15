// Copyright (C) 2024 Quickwit, Inc.
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

import { useRef, useState } from 'react';
import { SearchComponentProps } from '../../utils/SearchComponentProps';
import { TermAgg, HistogramAgg } from '../../utils/models';
import { Box } from '@mui/material';
import MenuItem from '@mui/material/MenuItem';
import FormControl from '@mui/material/FormControl';
import Select, { SelectChangeEvent } from '@mui/material/Select';
import TextField from '@mui/material/TextField';

export function AggregationEditor(props: SearchComponentProps) {
  return (
    <Box hidden={!props.searchRequest.aggregation}>
      <MetricKind
        searchRequest={props.searchRequest}
        onSearchRequestUpdate={props.onSearchRequestUpdate}
        runSearch={props.runSearch}
        index={props.index}
        queryRunning={props.queryRunning} />
      <AggregationKind
        searchRequest={props.searchRequest}
        onSearchRequestUpdate={props.onSearchRequestUpdate}
        runSearch={props.runSearch}
        index={props.index}
        queryRunning={props.queryRunning} />
    </Box>
  )
}

export function MetricKind(props: SearchComponentProps) {
  // TODO add percentiles
  const metricRef = useRef(props.searchRequest.aggregationConfig.metric);

  const handleTypeChange = (event: SelectChangeEvent) => {
    const value = event.target.value;
    const updatedMetric = value != "count" ? {...metricRef.current!, type: value} : null;
    const updatedAggregation = {...props.searchRequest.aggregationConfig, metric: updatedMetric};
    const updatedSearchRequest = {...props.searchRequest, aggregationConfig: updatedAggregation};
    props.onSearchRequestUpdate(updatedSearchRequest);
    metricRef.current = updatedMetric;
  };

  const handleNameChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const value = event.target.value;
    if (metricRef.current == null) {
      return;
    }
    const updatedMetric = {...metricRef.current!, field: value};
    const updatedAggregation = {...props.searchRequest.aggregationConfig, metric: updatedMetric};
    const updatedSearchRequest = {...props.searchRequest, aggregationConfig: updatedAggregation};
    props.onSearchRequestUpdate(updatedSearchRequest);
    metricRef.current = updatedMetric;
  };

  return (
    <Box>
      <FormControl variant="standard" sx={{ m: 1, minWidth: 120, display: 'flex', flexDirection: 'row', }}>
        <Select
          value={metricRef.current ? metricRef.current.type : "count"}
          onChange={handleTypeChange}
          sx={{ "min-height": "44px" }}
        >
          <MenuItem value="count">Count</MenuItem>
          <MenuItem value="avg">Average</MenuItem>
          <MenuItem value="sum">Sum</MenuItem>
          <MenuItem value="max">Max</MenuItem>
          <MenuItem value="min">Min</MenuItem>
        </Select>
        <TextField 
          variant="standard"
          label="Field"
          onChange={handleNameChange}
          sx={{ "margin-left": "10px", ... ( !metricRef.current && {display: "none"}) }}
        />
      </FormControl>
    </Box>
  )
}

export function AggregationKind(props: SearchComponentProps) {
  props;
  const defaultAgg  = {
    histogram: {
      interval: "auto",
    }
  };
  const [aggregations, setAggregations] = useState<({term: TermAgg} | {histogram: HistogramAgg})[]>(
          [defaultAgg]);
  
  const updateAggregationProp = (newAggregations: ({term: TermAgg} | {histogram: HistogramAgg})[]) => {
    const metric = props.searchRequest.aggregationConfig.metric;
    const updatedAggregation = Object.assign({}, {metric: metric}, ...newAggregations);
    const updatedSearchRequest = {...props.searchRequest, aggregationConfig: updatedAggregation};
    props.onSearchRequestUpdate(updatedSearchRequest);
  };

  const handleAggregationChange = (pos: number, event: SelectChangeEvent) => {
    const value = event.target.value;
    setAggregations((agg) => {
      const newAggregations = [...agg];
      switch(value) {
        case "histogram": {
          newAggregations[pos] = {
            histogram: {
              interval: "auto",
            }
          };
          break;
        }
        case "term": {
          newAggregations[pos] = {
            term: {
              field: "",
              size: 10,
            }
          };
          break;
        }
        case "rm": {
          console.log("av", newAggregations);
          newAggregations.splice(pos, 1);
          console.log("cesar", newAggregations);
        }
      }
      updateAggregationProp(newAggregations);
      return newAggregations;
    });
  };

  const handleHistogramChange = (pos: number, event: SelectChangeEvent) => {
    const value = event.target.value;
    setAggregations((agg) => {
      const newAggregations = [...agg];
      newAggregations[pos] = {histogram: {interval:value}};
      updateAggregationProp(newAggregations);
      return newAggregations;
    });
  }

  const handleTermFieldChange = (pos: number, event: React.ChangeEvent<HTMLInputElement|HTMLTextAreaElement>) => {
    const value = event.target.value;
    setAggregations((agg) => {
      const newAggregations = [...agg];
      const term = newAggregations[pos]
      if (isTerm(term)) {
        term.term.field = value;
      }
      updateAggregationProp(newAggregations);
      return newAggregations;
    });
  };

  const handleTermCountChange = (pos: number, event: React.ChangeEvent<HTMLInputElement|HTMLTextAreaElement>) => {
    const value = event.target.value;
    setAggregations((agg) => {
      const newAggregations = [...agg];
      const term = newAggregations[pos]
      if (isTerm(term)) {
        term.term.size = Number(value);
      }
      updateAggregationProp(newAggregations);
      return newAggregations;
    });
  };

  function isHistogram(agg: {term: TermAgg} | {histogram: HistogramAgg} | undefined): agg is {histogram: HistogramAgg} {
    if (!agg) return false;
    return Object.hasOwn(agg, "histogram")
  }

  function isTerm(agg: {term: TermAgg} | {histogram: HistogramAgg} | undefined): agg is {term: TermAgg} {
    if (!agg) return false;
    return Object.hasOwn(agg, "term")
  }

  const getAggregationKind = (agg: {term: TermAgg} | {histogram: HistogramAgg} | undefined) => {
    if (isHistogram(agg)) {
      return "histogram";
    }
    if (isTerm(agg)) {
      return "term";
    }
    return "new";
  };

  const makeOptions = (pos: number, agg: ({term: TermAgg} | {histogram: HistogramAgg})[]) => {
    const options = [];
    if (pos >= agg.length) {
      options.push((
          <MenuItem value="new">Add aggregation</MenuItem>
      ))
    }
    let addHistogram = true;
    let addTerm = true;
    for(let i = 0; i < agg.length; i++) {
      if (i == pos) continue;
      if (getAggregationKind(agg[i]) === "histogram") addHistogram = false;
      if (getAggregationKind(agg[i]) === "term") addTerm = false;
    }
    if (addHistogram) {
      options.push((<MenuItem value="histogram">Histogram aggregation</MenuItem>))
    }
    if (addTerm) {
      options.push((<MenuItem value="term">Term aggregation</MenuItem>));
    }
    if (agg.length > 1) {
      options.push((
          <MenuItem value="rm">Remove aggregation</MenuItem>
      ))
    }
    return options;
  }

  // do the initial filling of parameters
  const aggregationConfig = props.searchRequest.aggregationConfig;
  if (aggregationConfig.histogram === null && aggregationConfig.term === null) {
    const initialAggregation = Object.assign({}, ...aggregations);
    const initialSearchRequest = {...props.searchRequest, aggregationConfig: initialAggregation};
    props.onSearchRequestUpdate(initialSearchRequest);
  }

  const drawAdditional = (pos: number, aggs: ({term: TermAgg} | {histogram: HistogramAgg})[]) => {
    const agg = aggs[pos]
    if (isHistogram(agg)) {
      return (
        <Select
          value={agg.histogram.interval}
          onChange={(e) => handleHistogramChange(pos, e)}
          sx={{ "margin-left": "10px", "min-height": "44px" }}
        >
          <MenuItem value="auto">Auto</MenuItem>
          <MenuItem value="10s">10 seconds</MenuItem>
          <MenuItem value="1m">1 minute</MenuItem>
          <MenuItem value="5m">5 minutes</MenuItem>
          <MenuItem value="10m">10 minutes</MenuItem>
          <MenuItem value="1h">1 hour</MenuItem>
          <MenuItem value="1d">1 day</MenuItem>
        </Select>
      );
    }
    if (isTerm(agg)) {
      return [(
          <TextField 
            variant="standard"
            label="Field"
            onChange={(e) => handleTermFieldChange(pos, e)}
            sx={{ "margin-left": "10px" }}
          />
        ),(
          <TextField 
            variant="standard"
            label="Return top"
            type="number"
            onChange={(e) => handleTermCountChange(pos, e)}
            value={agg.term.size}
            sx={{ "margin-left": "10px" }}
          />
        )]
    }
    return (null);
  }

  return (
    <Box>
      <FormControl variant="standard" sx={{ m: 1, minWidth: 120, display: 'flex', flexDirection: 'row', }}>
        <Select
          value={getAggregationKind(aggregations[0])}
          onChange={(e) => handleAggregationChange(0, e)}
          sx={{ "min-height": "44px", width: "190px" }}
        >
          { makeOptions(0, aggregations) }
        </Select>
        {drawAdditional(0, aggregations)}
      </FormControl>
      <FormControl variant="standard" sx={{ m: 1, minWidth: 120, display: 'flex', flexDirection: 'row', }}>
        <Select
          value={getAggregationKind(aggregations[1])}
          onChange={(e) => handleAggregationChange(1, e)}
          sx={{ "min-height": "44px", width: "190px" }}
        >
          { makeOptions(1, aggregations) }
        </Select>
        {drawAdditional(1, aggregations)}
      </FormControl>
    </Box>
  )
}
