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

import { Cluster, Index, IndexMetadata, QuickwitBuildInfo, SearchRequest, SearchResponse, SplitMetadata } from "../utils/models";
import { serializeSortByField } from "../utils/urls";

export class Client {
  private readonly _host: string

  constructor(host?: string) {
    if (!host) {
      this._host = window.location.origin
    } else {
      this._host = host
    }
  }

  apiRoot(): string {
    return this._host + "/api/v1/";
  }

  async search(request: SearchRequest, timestamp_field: string | null): Promise<SearchResponse> {
    // TODO: improve validation of request.
    if (request.indexId === null || request.indexId === undefined) {
      throw Error("Search request must have and index id.")
    }
    const url = `${this.apiRoot()}${request.indexId}/search`;
    const body = this.buildSearchBody(request, timestamp_field);
    return this.fetch(url, this.defaultGetRequestParams(), body);
  }

  async cluster(): Promise<Cluster> {
    return await this.fetch(`${this.apiRoot()}cluster`, this.defaultGetRequestParams());
  }

  async buildInfo(): Promise<QuickwitBuildInfo> {
    return await this.fetch(`${this.apiRoot()}version`, this.defaultGetRequestParams());
  }

  // eslint-disable-next-line
  async config(): Promise<Record<string, any>> {
    return await this.fetch(`${this.apiRoot()}config`, this.defaultGetRequestParams());
  }
  //
  // Index management API
  //
  async getIndex(indexId: string): Promise<Index> {
    const [metadata, splits] = await Promise.all([
      this.getIndexMetadata(indexId),
      this.getAllSplits(indexId)
    ]);
    return {
      metadata: metadata,
      splits: splits
    }
  }

  async getIndexMetadata(indexId: string): Promise<IndexMetadata> {
    return this.fetch(`${this.apiRoot()}indexes/${indexId}`, {});
  }

  async getAllSplits(indexId: string): Promise<Array<SplitMetadata>> {
    // TODO: restrieve all the splits.
    const results: {splits: Array<SplitMetadata>} = await this.fetch(`${this.apiRoot()}indexes/${indexId}/splits?limit=10000`, {});

    return results['splits'];
  }

  async listIndexes(): Promise<Array<IndexMetadata>> {
    return this.fetch(`${this.apiRoot()}indexes`, {});
  }

  async fetch<T>(url: string, params: RequestInit, body: string|null = null): Promise<T> {
    if (body !== null) {
      params.method = "POST";
      params.body = body;
      params.headers = {...params.headers, "content-type": "application/json"};
    }
    const response = await fetch(url, params);
    if (response.ok) {
      return response.json() as Promise<T>;
    }
    const message = await response.text();
    return await Promise.reject({
      message: message,
      status: response.status
    });
  }

  private defaultGetRequestParams(): RequestInit {
    return {
      method: "GET",
      headers: { Accept: "application/json" },
      mode: "cors",
      cache: "default",
    }
  }

  buildSearchBody(request: SearchRequest, timestamp_field: string | null): string {
    /* eslint-disable  @typescript-eslint/no-explicit-any */
    const body: any = {
      // TODO: the trim should be done in the backend.
      query: request.query.trim() || "*",
    };

    if (request.aggregation) {
      const qw_aggregation = this.buildAggregation(request, timestamp_field);
      body["aggs"] = qw_aggregation;
      body["max_hits"] = 0;
    } else {
      body["max_hits"] = 20;
    }
    if (request.startTimestamp) {
      body["start_timestamp"] = request.startTimestamp;
    }
    if (request.endTimestamp) {
      body["end_timestamp"] = request.endTimestamp;
    }
    if (request.sortByField) {
      body["sort_by_field"] = serializeSortByField(request.sortByField);
    }
    return JSON.stringify(body);
  }

  /* eslint-disable  @typescript-eslint/no-explicit-any */
  buildAggregation(request: SearchRequest, timestamp_field: string | null): any {
    let aggregation = undefined;
    if (request.aggregationConfig.metric) {
      const metric = request.aggregationConfig.metric;
      aggregation = {
        metric: {
          [metric.type]: {
            field: metric.field
          }
        }
      }
    }
    if (request.aggregationConfig.histogram && timestamp_field) {
      const histogram = request.aggregationConfig.histogram;
      const interval = histogram.interval
      let extended_bounds;
      if (request.startTimestamp && request.endTimestamp) {
	// extended_bounds are ms
        extended_bounds = {
          min: request.startTimestamp * 1000,
          max: request.endTimestamp * 1000,
        };
      } else {
        extended_bounds = undefined;
      }
      aggregation = {
        histo_agg: {
          aggs: aggregation,
          date_histogram: {
            field: timestamp_field,
            fixed_interval: interval,
            min_doc_count: 0,
            extended_bounds: extended_bounds,
          }
        }
      }
    }
    if (request.aggregationConfig.term) {
      const term = request.aggregationConfig.term;
      aggregation = {
        term_agg: {
          aggs: aggregation,
          terms: {
            field: term.field,
            size: term.size,
            order: {
              _count: "desc"
            },
            min_doc_count: 1,
          }
        }
      }
    }
    return aggregation;
  }
}
