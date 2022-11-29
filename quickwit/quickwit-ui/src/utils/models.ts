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

/* eslint-disable  @typescript-eslint/no-explicit-any */
export type RawDoc = Record<string, any>

export type FieldMapping = {
  description: string | null;
  name: string;
  type: string;
  stored: boolean | null;
  fast: boolean | null;
  indexed: boolean | null;
  // Specific datetime field attributes.
  output_format: string | null;
  field_mappings?: FieldMapping[];
}

export type Field = {
  // Json path (path segments concatenated as a string with dots between segments).
  json_path: string;
  // Json path of the field.
  path_segments: string[];
  field_mapping: FieldMapping;
}

export type Entry = {
  key: string;
  value: any;
}

export const DATE_TIME_WITH_SECONDS_FORMAT = "YYYY/MM/DD HH:mm:ss";

// Returns a flatten array of fields and nested fields found in the given `FieldMapping` array. 
export function getAllFields(field_mappings: Array<FieldMapping>): Field[] {
  const fields: Field[] = [];
  for (const field_mapping of field_mappings) {
    if (field_mapping.type === 'object' && field_mapping.field_mappings !== undefined) {
      for (const child_field_mapping of getAllFields(field_mapping.field_mappings)) {
        fields.push({json_path: field_mapping.name + '.' + child_field_mapping.json_path, path_segments: [field_mapping.name].concat(child_field_mapping.path_segments), field_mapping: child_field_mapping.field_mapping})
      }
    } else {
      fields.push({json_path: field_mapping.name, path_segments: [field_mapping.name], field_mapping: field_mapping});
    }
  }

  return fields;
}

export type DocMapping = {
  field_mappings: FieldMapping[];
  tag_fields: string[];
  store: boolean;
  dynamic_mapping: boolean;
  timestamp_field: string | null;
}

export type SortOrder = 'Asc' | 'Desc';

export type SortByField = {
  field_name: string,
  order: SortOrder
}

export type SearchRequest = {
  indexId: string | null;
  query: string;
  startTimestamp: number | null;
  endTimestamp: number | null;
  maxHits: number;
  sortByField: SortByField | null;
}

export const EMPTY_SEARCH_REQUEST: SearchRequest = {
  indexId: '',
  query: '',
  startTimestamp: null,
  endTimestamp: null,
  maxHits: 100,
  sortByField: null,
}

export type ResponseError = {
  status: number | null;
  message: string | null;
}

export type SearchResponse = {
  num_hits: number;
  hits: Array<RawDoc>;
  elapsed_time_micros: number;
  errors: Array<any> | undefined;
}

export type IndexConfig = {
  version: string;
  index_id: string;
  index_uri: string;
  doc_mapping: DocMapping;
  indexing_settings: object;
  search_settings: object;
  retention: object;
}

export type IndexMetadata = {
  index_config: IndexConfig;
  checkpoint: object;
  sources: object[] | undefined;
  create_timestamp: number;
}

export const EMPTY_INDEX_METADATA: IndexMetadata = {
  index_config: {
    version: '',
    index_uri: '',
    index_id: '',
    doc_mapping: {
      field_mappings: [],
      tag_fields: [],
      store: false,
      dynamic_mapping: false,
      timestamp_field: null
    },
    indexing_settings: {},
    search_settings: {},
    retention: {},
  },
  checkpoint: {},
  sources: undefined,
  create_timestamp: 0,
};

export type SplitMetadata = {
  split_id: string;
  split_state: string;
  num_docs: number;
  time_range: null | Range;
  update_timestamp: number;
  version: number;
  create_timestamp: number;
  tags: string[];
  demux_num_ops: number;
  footer_offsets: Range;
}

export type Range = {
  start: number;
  end: number;
}

export type Index = {
  metadata: IndexMetadata;
  splits: SplitMetadata[];
}

export type Cluster = {
  node_id: string,
  cluster_id: string,
  state: ClusterState,
}

export type ClusterState = {
  state: ClusterStateSnapshot;
  live_nodes: any[];
  dead_nodes: any[];
}

export type ClusterStateSnapshot = {
  seed_addrs: string[],
  node_states: Record<string, NodeState>,
}

export type NodeState = {
  key_values: KeyValues,
  max_version: number,
}

export type KeyValues = {
  available_services: KeyValue,
  grpc_address: KeyValue,
  heartbeat: KeyValue,
}

export type KeyValue = {
  value: any,
  version: number,
}

export type QuickwitBuildInfo = {
  commit_version_tag: string,
  cargo_pkg_version: string,
  cargo_build_target: string,
  commit_short_hash: string,
  commit_date: string,
  version: string,
}

export type NodeId = {
  id: string,
  grpc_address: string,
  self: boolean,
}
