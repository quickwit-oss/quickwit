// Copyright (C) 2021 Quickwit, Inc.
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

import { GH_ARCHIVE_SEARCH_RESPONSE, HDFS_LOGS_SEARCH_RESPONSE, INDEXES_METADATA, INDEXES_SPLITS as INDEXES_SPLITS_METADATA, WIKIPEDIA_SEARCH_RESPONSE } from "../utils/fake_data";
import { Index, IndexMetadata, MemberList, SearchRequest, SearchResponse, SplitMetadata } from "../utils/models";

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

  // /cluster/members
  async clusterMembers(): Promise<MemberList> {
    await new Promise(resolve => setTimeout(resolve, 500));
    return {
      "members": [
        {
          id: "1",
          listen_address: "localhost:7280",
          is_self: true,
        },
        {
          id: "2",
          listen_address: "localhost:7281",
          is_self: false,
        },
        {
          id: "3",
          listen_address: "localhost:7282",
          is_self: false,
        },
        {
          id: "4",
          listen_address: "localhost:7283",
          is_self: false,
        },
      ]
    }
  }

  async search(request: SearchRequest): Promise<SearchResponse> {
    // TODO: improve validation of request.
    if (request.indexId === null || request.indexId === undefined) {
      throw Error("Search request must have and index id.")
    }

    // TODO: call the API.
    // const url = this.buildSearchUrl(request);
    // const requestParams = this.defaultGetRequestParams();
    // const httpRequest = new Request(url.toString(), requestParams);
    await new Promise(resolve => setTimeout(resolve, 500));

    // Some fake data.
    if (request.indexId === 'wikipedia') {
      return WIKIPEDIA_SEARCH_RESPONSE;
    } else if (request.indexId === 'hdfs-logs') {
      return HDFS_LOGS_SEARCH_RESPONSE;
    } else if (request.indexId === 'gh-archive') {
      return GH_ARCHIVE_SEARCH_RESPONSE;
    }
    return {
      count: 0,
      numMicrosecs: 1,
      hits: []
    };
  }

  async getIndex(indexId: string): Promise<Index> {
    // TODO: call the API.
    await new Promise(resolve => setTimeout(resolve, 300));
    const metadata = INDEXES_METADATA.filter(metadata => metadata.index_id === indexId)[0];
    // @ts-ignore:next-line
    const splits: SplitMetadata[] = INDEXES_SPLITS_METADATA[metadata.index_id];
    return {
      // @ts-ignore:next-line
      metadata: metadata,
      splits: splits
    }
  }

  async listIndexes(): Promise<IndexMetadata[]> {
    // TODO: call the API.
    await new Promise(resolve => setTimeout(resolve, 300));
    return INDEXES_METADATA;
  }

  // @ts-ignore:next-line
  private defaultGetRequestParams(): RequestInit{
    return {
      method: "GET",
      headers: { Accept: "application/json" },
      mode: "no-cors",
      cache: "default",
    }
  }

  // @ts-ignore:next-line
  private buildSearchUrl(request: SearchRequest): URL {
    const url: URL = new URL("search", this.apiRoot());
    url.searchParams.append("query", request.query);
    url.searchParams.append("numHits", "20");
    if (request.startTimestamp) {
      url.searchParams.append(
        "startTimestamp",
        request.startTimestamp.toString()
      );
    }
    if (request.endTimestamp) {
      url.searchParams.append(
        "endTimestamp",
        request.endTimestamp.toString()
      );
    }
    return url;
  }
}