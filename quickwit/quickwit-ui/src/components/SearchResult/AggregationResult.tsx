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

import { BarChart } from "@mui/x-charts/BarChart";
import { LineChart } from "@mui/x-charts/LineChart";
import {
  extractAggregationResults,
  HistogramResult,
  ParsedAggregationResult,
  SearchResponse,
  TermResult,
} from "../../utils/models";

function isHistogram(agg: ParsedAggregationResult): agg is HistogramResult {
  return agg != null && "timestamps" in agg;
}

function isTerm(agg: ParsedAggregationResult): agg is TermResult {
  return Array.isArray(agg);
}

export function AggregationResult({
  searchResponse,
}: {
  searchResponse: SearchResponse;
}) {
  const result = extractAggregationResults(searchResponse.aggregations);
  if (isHistogram(result)) {
    const xAxis: React.ComponentProps<typeof LineChart>["xAxis"] = [
      {
        data: result.timestamps,
        valueFormatter: (date: number) => {
          return new Date(date).toISOString();
        },
      },
    ];
    const series: React.ComponentProps<typeof LineChart>["series"] =
      result.data.map((line) => {
        return {
          curve: "monotoneX",
          label: line.name,
          data: line.value,
        };
      });
    // we don't customize colors because we would need a full palette.
    return <LineChart xAxis={xAxis} series={series} yAxis={[{ min: 0 }]} />;
  } else if (isTerm(result)) {
    return (
      <BarChart
        series={[
          { data: result.map((entry) => entry.value), color: "#004BD9A5" },
        ]}
        xAxis={[{ data: result.map((entry) => entry.term), scaleType: "band" }]}
        margin={{ top: 10, bottom: 30, left: 40, right: 10 }}
      />
    );
  } else {
    return <p>no result to display</p>;
  }
}
