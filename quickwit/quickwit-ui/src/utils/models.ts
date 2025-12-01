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

export type RawDoc = Record<string, any>;

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
};

export type Field = {
  // Json path (path segments concatenated as a string with dots between segments).
  json_path: string;
  // Json path of the field.
  path_segments: string[];
  field_mapping: FieldMapping;
};

export type Entry = {
  key: string;
  value: any;
};

export const DATE_TIME_WITH_SECONDS_FORMAT = "YYYY/MM/DD HH:mm:ss";
export const DATE_TIME_WITH_MILLISECONDS_FORMAT = "YYYY/MM/DD HH:mm:ss.SSS";

// Returns a flatten array of fields and nested fields found in the given `FieldMapping` array.
export function getAllFields(field_mappings: Array<FieldMapping>): Field[] {
  const fields: Field[] = [];
  for (const field_mapping of field_mappings) {
    if (
      field_mapping.type === "object" &&
      field_mapping.field_mappings !== undefined
    ) {
      for (const child_field_mapping of getAllFields(
        field_mapping.field_mappings,
      )) {
        fields.push({
          json_path: field_mapping.name + "." + child_field_mapping.json_path,
          path_segments: [field_mapping.name].concat(
            child_field_mapping.path_segments,
          ),
          field_mapping: child_field_mapping.field_mapping,
        });
      }
    } else {
      fields.push({
        json_path: field_mapping.name,
        path_segments: [field_mapping.name],
        field_mapping: field_mapping,
      });
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
};

export type SortOrder = "Asc" | "Desc";

export type SortByField = {
  field_name: string;
  order: SortOrder;
};

export type SearchRequest = {
  indexId: string | null;
  query: string;
  startTimestamp: number | null;
  endTimestamp: number | null;
  maxHits: number;
  sortByField: SortByField | null;
  aggregation: boolean;
  aggregationConfig: Aggregation;
};

export type Aggregation = {
  metric: Metric | null;
  term: TermAgg | null;
  histogram: HistogramAgg | null;
};

export type Metric = {
  type: string;
  field: string;
};

export type TermAgg = {
  field: string;
  size: number;
};

export type HistogramAgg = {
  interval: string;
};

export type ParsedAggregationResult = TermResult | HistogramResult | null;

export type TermResult = { term: string; value: number }[];

export type HistogramResult = {
  timestamps: Date[];
  data: { name: string | undefined; value: number[] }[];
};

export function extractAggregationResults(
  aggregation: any,
): ParsedAggregationResult {
  const extract_value = (entry: any) => {
    if ("metric" in entry) {
      return entry.metric.value || 0;
    } else {
      return entry.doc_count;
    }
  };
  if ("histo_agg" in aggregation) {
    const buckets = aggregation.histo_agg.buckets;
    const timestamps = buckets.map((entry: any) => entry.key);
    const value = buckets.map(extract_value);
    // we are in the "simple histogram" case
    return {
      timestamps,
      data: [{ name: undefined, value }],
    };
  } else if ("term_agg" in aggregation) {
    // we have a term aggregation, but maybe there is an histogram inside
    const term_buckets = aggregation.term_agg.buckets;
    if (term_buckets.length === 0) {
      return null;
    }
    if (term_buckets.length > 0 && "histo_agg" in term_buckets[0]) {
      // we have a term+histo aggregation
      const timestamps_set: Set<number> = new Set();
      term_buckets.forEach((bucket: any) =>
        bucket.histo_agg.buckets.forEach((entry: any) =>
          timestamps_set.add(entry.key),
        ),
      );
      const timestamps = [...timestamps_set];
      timestamps.sort();

      const data = term_buckets.map((bucket: any) => {
        const histo_buckets = bucket.histo_agg.buckets;
        const first_elem_key = histo_buckets[0].key;
        const last_elem_key = histo_buckets[histo_buckets.length - 1].key;
        const prefix_len = timestamps.indexOf(first_elem_key);
        const suffix_len =
          timestamps.length - timestamps.indexOf(last_elem_key) - 1;
        const value = Array(prefix_len)
          .fill(0)
          .concat(histo_buckets.map(extract_value), Array(suffix_len).fill(0));

        return { name: bucket.key, value };
      });
      return {
        timestamps: timestamps.map((date) => new Date(date)),
        data,
      };
    } else {
      return term_buckets.map((bucket: any) => {
        return {
          term: bucket.key,
          value: extract_value(bucket),
        };
      });
    }
  }
  // we are in neither case??
  return null;
}

export const EMPTY_SEARCH_REQUEST: SearchRequest = {
  indexId: "",
  query: "",
  startTimestamp: null,
  endTimestamp: null,
  maxHits: 100,
  sortByField: null,
  aggregation: false,
  aggregationConfig: {
    metric: null,
    term: null,
    histogram: null,
  },
};

export type ResponseError = {
  status: number | null;
  message: string | null;
};

export type SearchResponse = {
  num_hits: number;
  hits: Array<RawDoc>;
  elapsed_time_micros: number;
  errors: Array<any> | undefined;
  aggregations: any | undefined;
};

export type IndexConfig = {
  version: string;
  index_id: string;
  index_uri: string;
  doc_mapping: DocMapping;
  indexing_settings: object;
  search_settings: object;
  retention: object;
};

export type IndexMetadata = {
  index_config: IndexConfig;
  checkpoint: object;
  sources: object[] | undefined;
  create_timestamp: number;
};

export const EMPTY_INDEX_METADATA: IndexMetadata = {
  index_config: {
    version: "",
    index_uri: "",
    index_id: "",
    doc_mapping: {
      field_mappings: [],
      tag_fields: [],
      store: false,
      dynamic_mapping: false,
      timestamp_field: null,
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
  uncompressed_docs_size_in_bytes: number;
  time_range: null | Range;
  update_timestamp: number;
  version: number;
  create_timestamp: number;
  tags: string[];
  demux_num_ops: number;
  footer_offsets: Range;
};

export type Range = {
  start: number;
  end: number;
};

export type Index = {
  metadata: IndexMetadata;
  splits: SplitMetadata[];
};

export type Cluster = {
  node_id: string;
  cluster_id: string;
  state: ClusterState;
};

export type ClusterState = {
  state: ClusterStateSnapshot;
  live_nodes: any[];
  dead_nodes: any[];
};

export type ClusterStateSnapshot = {
  seed_addrs: string[];
  node_states: Record<string, NodeState>;
};

export type NodeState = {
  key_values: KeyValues;
  max_version: number;
};

export type KeyValues = {
  available_services: KeyValue;
  grpc_address: KeyValue;
  heartbeat: KeyValue;
};

export type KeyValue = {
  value: any;
  version: number;
};

export type QuickwitBuildInfo = {
  commit_version_tag: string;
  cargo_pkg_version: string;
  cargo_build_target: string;
  commit_short_hash: string;
  commit_date: string;
  version: string;
};

export type NodeId = {
  id: string;
  grpc_address: string;
  self: boolean;
};
