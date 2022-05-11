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

/* eslint-disable  @typescript-eslint/no-explicit-any */
export type RawDoc = Record<string, any>

export enum TimeUnit {
  MICRO_SECOND,
  MILLI_SECOND,
  SECOND,
}

export type FieldMapping = {
  name: string;
  type: string;
  field_mappings?: FieldMapping[];
}

export type FlattenField = {
  path: string[];
  name: string;
  type: string;
}

export type Entry = {
  key: string;
  value: any;
}

function getValueFromPath(path: string[], raw_doc: RawDoc): any {
  let value = raw_doc;
  for (const key of path) {
    if (key in value) {
      value = value[key];
    } else {
      return null;
    }
  }
  return value;
}

export function flattenEntries(doc_mapping: DocMapping, raw_doc: RawDoc): Entry[] {
  const flatten_fields = getFlattenFields(doc_mapping.field_mappings);
  const records = [];

  for (const flatten_field of flatten_fields) {
    const value = getValueFromPath(flatten_field.path, raw_doc);
    if (value !== null) {
      records.push({key: flatten_field.name, value: value});
    }
  }
  return records;
}

function getFlattenFields(field_mappings: FieldMapping[]): FlattenField[] {
  const fields: FlattenField[] = [];
  for (const field_mapping of field_mappings) {
    if (field_mapping.type === 'object' && field_mapping.field_mappings !== undefined) {
      for (const child_field of getFlattenFields(field_mapping.field_mappings)) {
        fields.push({name: field_mapping.name + '.' + child_field.name, path: [field_mapping.name].concat(child_field.path), type: child_field.type})
      }
    } else {
      fields.push({name: field_mapping.name, path: [field_mapping.name], type: field_mapping.type});
    }
  }
   
  return fields;
}

export function guessTimeUnit(index: Index): TimeUnit {
  if (index.splits.length === 0 || index.metadata.indexing_settings.timestamp_field === null) {
    return TimeUnit.MILLI_SECOND;
  }
  // Not possible in theory as guessTimeUnit function is 
  // called only on index with a timestamp field.
  if (index.splits.length === 0 || index.splits[0] === undefined || index.splits[0].time_range === null) {
    return TimeUnit.MILLI_SECOND;
  }
  const range_start_values = index.splits.map(split => split.time_range === null ? 0 : split.time_range.start);
  const time_range_start_max = Math.max(...range_start_values);
  // We expect a split time range to be between year between 1971 and 2070. 
  const seconds_in_one_hundred_years = 3600 * 24 * 365 * 100;
  if (time_range_start_max < seconds_in_one_hundred_years) {
    return TimeUnit.SECOND
  }
  if (time_range_start_max < seconds_in_one_hundred_years * 1000) {
    return TimeUnit.MILLI_SECOND
  }
  if (time_range_start_max < seconds_in_one_hundred_years * 1000 * 1000) {
    return TimeUnit.MICRO_SECOND
  }
  console.warn('Cannot guess correctly time unit, value `time_range_start_max` is too high, set to micro seconds', time_range_start_max);
  return TimeUnit.MICRO_SECOND
}

export function getAllFields(doc_mapping: DocMapping) {
  return getFlattenFields(doc_mapping.field_mappings);
} 

export type DocMapping = {
  field_mappings: FieldMapping[];
  tag_fields: string[];
  store: boolean;
}

export type SearchRequest = {
  indexId: string | null;
  query: string;
  startTimestamp: number | null;
  endTimestamp: number | null;
  maxHits: number;
}

export const EMPTY_SEARCH_REQUEST: SearchRequest = {
  indexId: '',
  query: '',
  startTimestamp: null,
  endTimestamp: null,
  maxHits: 100,
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

export type IndexMetadata = {
  index_id: string;
  index_uri: string;
  checkpoint: object;
  doc_mapping: DocMapping;
  indexing_settings: IndexingSettings;
  search_settings: object;
  sources: object[] | undefined;
  create_timestamp: number;
  update_timestamp: number;
}

export type IndexingSettings = {
  timestamp_field: null | string;
}

export const EMPTY_INDEX_METADATA: IndexMetadata = {
  index_id: '',
  index_uri: '',
  checkpoint: {},
  indexing_settings: {
    timestamp_field: null
  },
  search_settings: {},
  sources: [],
  create_timestamp: 0,
  update_timestamp: 0,
  doc_mapping: {
    store: false,
    field_mappings: [],
    tag_fields: []
  }
};

export type SplitMetadata = {
  split_id: string;
  split_state: string;
  num_docs: number;
  size_in_bytes: number;
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

export type Member = {
  id: string;
  listen_address: string;
  is_self: boolean;
}

export type MemberList = {
  members: Member[];
}
