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

import { Box, Typography } from "@mui/material";
import { useMemo } from "react";
import { NumericFormat } from "react-number-format";
import { Index, ResponseError, SearchResponse } from "../../utils/models";
import Loader from "../Loader";
import ErrorResponseDisplay from "../ResponseErrorDisplay";
import { AggregationResult } from "./AggregationResult";
import { ResultTable } from "./ResultTable";

function HitCount({ searchResponse }: { searchResponse: SearchResponse }) {
  return (
    <Box>
      <Typography variant="body2" color="textSecondary">
        <NumericFormat
          displayType="text"
          value={searchResponse.num_hits}
          thousandSeparator=","
        />{" "}
        hits found in&nbsp;
        <NumericFormat
          decimalScale={2}
          displayType="text"
          value={searchResponse.elapsed_time_micros / 1000000}
          thousandSeparator=","
        />{" "}
        seconds
      </Typography>
    </Box>
  );
}

interface SearchResultProps {
  queryRunning: boolean;
  index: null | Index;
  searchResponse: null | SearchResponse;
  searchError: null | ResponseError;
}

export default function SearchResult(props: SearchResultProps) {
  const result = useMemo(() => {
    if (props.searchResponse == null || props.index == null) {
      return null;
    } else if (props.searchResponse.aggregations === undefined) {
      return (
        <ResultTable
          searchResponse={props.searchResponse}
          index={props.index}
        />
      );
    } else {
      return <AggregationResult searchResponse={props.searchResponse} />;
    }
  }, [props.searchResponse, props.index]);

  if (props.queryRunning) {
    return <Loader />;
  }

  if (props.searchError !== null) {
    return ErrorResponseDisplay(props.searchError);
  }

  if (props.searchResponse == null || props.index == null) {
    return <></>;
  }

  return (
    <Box sx={{ pt: 1, flexGrow: "1", flexBasis: "0%", overflow: "hidden" }}>
      <Box
        sx={{
          height: "100%",
          flexDirection: "column",
          flexGrow: 1,
          display: "flex",
        }}
      >
        <Box
          sx={{
            flexShrink: 0,
            display: "flex",
            flexGrow: 0,
            flexBasis: "auto",
          }}
        >
          <HitCount searchResponse={props.searchResponse} />
        </Box>
        <Box
          sx={{
            pt: 2,
            flexGrow: 1,
            flexBasis: "0%",
            minHeight: 0,
            display: "flex",
            flexDirection: "column",
          }}
        >
          {result}
        </Box>
      </Box>
    </Box>
  );
}
