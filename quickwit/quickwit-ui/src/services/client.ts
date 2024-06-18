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

import { Cluster, Index, IndexMetadata, QuickwitBuildInfo, SearchRequest, SearchResponse, SplitMetadata } from "../utils/models";
import { serializeSortByField } from "../utils/urls";
import { BaseApiClient } from "./base";

export class Client extends BaseApiClient {

  async search(request: SearchRequest): Promise<SearchResponse> {
    // TODO: improve validation of request.
    if (request.indexId === null || request.indexId === undefined) {
      throw Error("Search request must have and index id.")
    }
    const url = this.buildSearchUrl(request);
    return this.fetch(url.toString(), this.getDefaultGetRequestParams());
  }

  async cluster(): Promise<Cluster> {
    return await this.fetch(`./cluster`, this.getDefaultGetRequestParams());
  }

  async buildInfo(): Promise<QuickwitBuildInfo> {
    return await this.fetch(`./version`, this.getDefaultGetRequestParams());
  }

  // eslint-disable-next-line
  async config(): Promise<Record<string, any>> {
    return await this.fetch(`./config`, this.getDefaultGetRequestParams());
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
    return this.fetch(`./indexes/${indexId}`, {});
  }

  async getAllSplits(indexId: string): Promise<Array<SplitMetadata>> {
    // TODO: restrieve all the splits.
    const results: {splits: Array<SplitMetadata>} = await this.fetch(`./indexes/${indexId}/splits?limit=10000`, {});

    return results['splits'];
  }

  async listIndexes(): Promise<Array<IndexMetadata>> {
    return this.fetch(`./indexes`, {});
  }

  buildSearchUrl(request: SearchRequest): URL {
    const url: URL = this.getURL(`./${request.indexId}/search`);
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
