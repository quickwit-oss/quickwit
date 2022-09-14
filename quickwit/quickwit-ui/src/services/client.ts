// Copyright (C) 2022 Quickwit, Inc.
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

  async search(request: SearchRequest): Promise<SearchResponse> {
    // TODO: improve validation of request.
    if (request.indexId === null || request.indexId === undefined) {
      throw Error("Search request must have and index id.")
    }
    const url = this.buildSearchUrl(request);
    return this.fetch(url.toString(), this.defaultGetRequestParams());
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
    const splits: Array<SplitMetadata> = await this.fetch(`${this.apiRoot()}indexes/${indexId}/splits`, {});

    return splits;
  }

  async listIndexes(): Promise<Array<IndexMetadata>> {
    return this.fetch(`${this.apiRoot()}indexes`, {});
  }

  async fetch<T>(url: string, params: RequestInit): Promise<T> {
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
      mode: "no-cors",
      cache: "default",
    }
  }

  buildSearchUrl(request: SearchRequest): URL {
    const url: URL = new URL(`${request.indexId}/search`, this.apiRoot());
    // TODO: the trim should be done in the backend.
    url.searchParams.append("query", request.query.trim() || "*");
    url.searchParams.append("max_hits", "20");
    if (request.startTimestamp) {
      url.searchParams.append(
        "start_timestamp",
        request.startTimestamp.toString()
      );
    }
    if (request.endTimestamp) {
      url.searchParams.append(
        "end_timestamp",
        request.endTimestamp.toString()
      );
    }
    if (request.sortByField) {
      url.searchParams.append(
        "sort_by_field",
        serializeSortByField(request.sortByField)
      );
    }
    return url;
  }
}
