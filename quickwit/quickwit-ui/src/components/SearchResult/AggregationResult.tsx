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

import { SearchResponse, HistogramResult, TermResult, ParsedAggregationResult, extractAggregationResults } from "../../utils/models";
import { LineChart } from '@mui/x-charts/LineChart';
import { BarChart } from '@mui/x-charts/BarChart';
import { CurveType } from '@mui/x-charts/models/seriesType/line';

function isHistogram(agg: ParsedAggregationResult): agg is HistogramResult {
  return agg != null && Object.prototype.hasOwnProperty.call(agg, "timestamps");
}

function isTerm(agg: ParsedAggregationResult): agg is TermResult {
  return Array.isArray(agg);
}

export function AggregationResult({searchResponse}: {searchResponse: SearchResponse}) {
  const result = extractAggregationResults(searchResponse.aggregations);
  if (isHistogram(result)) {
    const xAxis = [{
      data: result.timestamps,
      valueFormatter: (date: number) => {
        return new Date(date).toISOString()
      },
    }];
    const series = result.data.map((line) => {
       const curve: CurveType = "monotoneX";
       return {
          curve,
          label: line.name,
          data: line.value,
       };
    });
    series;
    return (
    <LineChart
        xAxis={xAxis}
        series={series}
        yAxis={[{min: 0}]}
      />
    )
  } else if (isTerm(result)) {
    return (<BarChart
      series={[{ data: result.map(entry => entry.value)}]}
      xAxis={[{ data: result.map(entry => entry.term), scaleType: 'band' }]}
      margin={{ top: 10, bottom: 30, left: 40, right: 10 }}
    />)
  } else {
    return (<p>no result to display</p>);
  }
}
