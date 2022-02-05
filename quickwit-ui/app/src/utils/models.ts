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

export type RawDoc = Record<string, any>

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
  for (let key of path) {
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

  for (let flatten_field of flatten_fields) {
    const value = getValueFromPath(flatten_field.path, raw_doc);
    if (value !== null) {
      records.push({key: flatten_field.name, value: value});
    }
  }
  return records;
}

function getFlattenFields(field_mappings: FieldMapping[]): FlattenField[] {
  let fields: FlattenField[] = [];
  for (let field_mapping of field_mappings) {
    if (field_mapping.type === 'object' && field_mapping.field_mappings !== undefined) {
      for (let child_field of getFlattenFields(field_mapping.field_mappings)) {
        fields.push({name: field_mapping.name + '.' + child_field.name, path: [field_mapping.name].concat(child_field.path), type: child_field.type})
      }
    } else {
      fields.push({name: field_mapping.name, path: [field_mapping.name], type: field_mapping.type});
    }
  }
   
  return fields;
}

export function get_all_fields(doc_mapping: DocMapping) {
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
  numHits: number;
}

export const EMPTY_SEARCH_REQUEST: SearchRequest = {
  indexId: '',
  query: '',
  startTimestamp: null,
  endTimestamp: null,
  numHits: 100,
}

export type SearchResponse = {
  count: number;
  hits: Array<RawDoc>;
  numMicrosecs: number;
}

export type IndexMetadata = {
  index_id: string;
  index_uri: string;
  checkpoint: object;
  doc_mapping: DocMapping;
  indexing_settings: IndexingSettings;
  search_settings: object;
  sources: object[];
  create_timestamp: number;
  update_timestamp: number;
  num_docs: number;
  num_bytes: number;
  num_splits: number;
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
  num_docs: 0,
  num_bytes: 0,
  num_splits: 20,
  doc_mapping: {
    store: false,
    field_mappings: [],
    tag_fields: []
  }
};

export type Member = {
  id: string;
  listen_address: string;
  is_self: boolean;
}

export type MemberList = {
  members: Member[];
}