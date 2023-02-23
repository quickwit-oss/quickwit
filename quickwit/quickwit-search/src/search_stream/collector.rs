// Copyright (C) 2023 Quickwit, Inc.
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

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;

use tantivy::collector::{Collector, SegmentCollector};
use tantivy::columnar::{DynamicColumn, HasAssociatedColumnType};
use tantivy::fastfield::Column;
use tantivy::{DocId, Score, SegmentOrdinal, SegmentReader};

use crate::filters::{TimestampFilter, TimestampFilterBuilder};

#[derive(Clone)]
pub struct FastFieldSegmentCollector<Item: HasAssociatedColumnType> {
    fast_field_values: Vec<Item>,
    column_opt: Option<Column<Item>>,
    timestamp_filter_opt: Option<TimestampFilter>,
}

impl<Item: HasAssociatedColumnType> FastFieldSegmentCollector<Item> {
    pub fn new(
        column_opt: Option<Column<Item>>,
        timestamp_filter_opt: Option<TimestampFilter>,
    ) -> Self {
        Self {
            fast_field_values: Vec::new(),
            column_opt,
            timestamp_filter_opt,
        }
    }

    fn accept_document(&self, doc_id: DocId) -> bool {
        if let Some(ref timestamp_filter) = self.timestamp_filter_opt {
            return timestamp_filter.is_within_range(doc_id);
        }
        true
    }
}

impl<Item: HasAssociatedColumnType> SegmentCollector for FastFieldSegmentCollector<Item> {
    type Fruit = Vec<Item>;

    fn collect(&mut self, doc_id: DocId, _score: Score) {
        let Some(column) = self.column_opt.as_ref() else {
            return;
        };
        if !self.accept_document(doc_id) {
            return;
        }
        self.fast_field_values.extend(column.values_for_doc(doc_id));
    }

    fn harvest(self) -> Vec<Item> {
        self.fast_field_values
    }
}

#[derive(Clone)]
pub struct FastFieldCollector<Item: HasAssociatedColumnType> {
    pub fast_field_to_collect: String,
    pub timestamp_filter_builder_opt: Option<TimestampFilterBuilder>,
    pub _marker: PhantomData<Item>,
}

impl<Item: HasAssociatedColumnType> Collector for FastFieldCollector<Item>
where DynamicColumn: Into<Option<Column<Item>>>
{
    type Child = FastFieldSegmentCollector<Item>;
    type Fruit = Vec<Item>;

    fn for_segment(
        &self,
        _segment_ord: SegmentOrdinal,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let timestamp_filter_opt =
            if let Some(timestamp_filter_builder) = &self.timestamp_filter_builder_opt {
                timestamp_filter_builder.build(segment_reader)?
            } else {
                None
            };

        let column_opt: Option<Column<Item>> = segment_reader
            .fast_fields()
            .column_opt::<Item>(&self.fast_field_to_collect)?;

        Ok(FastFieldSegmentCollector::new(
            column_opt,
            timestamp_filter_opt,
        ))
    }

    fn requires_scoring(&self) -> bool {
        // We do not need BM25 scoring in Quickwit.
        false
    }

    fn merge_fruits(&self, segment_fruits: Vec<Vec<Item>>) -> tantivy::Result<Self::Fruit> {
        Ok(segment_fruits.into_iter().flatten().collect::<Vec<_>>())
    }
}

#[derive(Clone)]
pub struct PartionnedFastFieldCollector<Item, PartitionItem> {
    pub fast_field_to_collect: String,
    pub partition_by_fast_field: String,
    pub timestamp_filter_builder_opt: Option<TimestampFilterBuilder>,
    pub _marker: PhantomData<(Item, PartitionItem)>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct PartitionValues<Item, PartitionItem> {
    pub partition_value: PartitionItem,
    pub fast_field_values: Vec<Item>,
}

impl<Item: HasAssociatedColumnType, PartitionItem: HasAssociatedColumnType + Eq + Hash> Collector
    for PartionnedFastFieldCollector<Item, PartitionItem>
where
    DynamicColumn: Into<Option<Column<Item>>>,
    DynamicColumn: Into<Option<Column<PartitionItem>>>,
{
    type Child = PartitionedFastFieldSegmentCollector<Item, PartitionItem>;
    type Fruit = Vec<PartitionValues<Item, PartitionItem>>;

    fn for_segment(
        &self,
        _segment_ord: SegmentOrdinal,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let timestamp_filter_opt =
            if let Some(timestamp_filter_builder) = &self.timestamp_filter_builder_opt {
                timestamp_filter_builder.build(segment_reader)?
            } else {
                None
            };
        let column_opt: Option<Column<Item>> = segment_reader
            .fast_fields()
            .column_opt(&self.fast_field_to_collect)?;

        let partition_column_opt = segment_reader
            .fast_fields()
            .column_opt(self.partition_by_fast_field.as_str())?;

        Ok(PartitionedFastFieldSegmentCollector::new(
            column_opt,
            partition_column_opt,
            timestamp_filter_opt,
        ))
    }

    fn requires_scoring(&self) -> bool {
        // We do not need BM25 scoring in Quickwit.
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<HashMap<PartitionItem, Vec<Item>>>,
    ) -> tantivy::Result<Self::Fruit> {
        Ok(segment_fruits
            .into_iter()
            .flat_map(|e| e.into_iter())
            .map(|(partition_value, values)| PartitionValues {
                partition_value,
                fast_field_values: values,
            })
            .collect())
    }
}

#[derive(Clone)]
pub struct PartitionedFastFieldSegmentCollector<Item, PartitionItem> {
    fast_field_values: HashMap<PartitionItem, Vec<Item>>,
    fast_field_reader: Option<Column<Item>>,
    partition_by_fast_field_reader: Option<Column<PartitionItem>>,
    timestamp_filter_opt: Option<TimestampFilter>,
}

impl<Item, PartitionItem> PartitionedFastFieldSegmentCollector<Item, PartitionItem> {
    pub fn new(
        fast_field_reader: Option<Column<Item>>,
        partition_by_fast_field_reader: Option<Column<PartitionItem>>,
        timestamp_filter_opt: Option<TimestampFilter>,
    ) -> Self {
        Self {
            fast_field_values: HashMap::default(),
            fast_field_reader,
            partition_by_fast_field_reader,
            timestamp_filter_opt,
        }
    }

    fn accept_document(&self, doc_id: DocId) -> bool {
        if let Some(ref timestamp_filter) = self.timestamp_filter_opt {
            return timestamp_filter.is_within_range(doc_id);
        }
        true
    }
}

impl<Item: HasAssociatedColumnType, PartitionItem: HasAssociatedColumnType + Hash + Eq>
    SegmentCollector for PartitionedFastFieldSegmentCollector<Item, PartitionItem>
{
    type Fruit = HashMap<PartitionItem, Vec<Item>>;

    fn collect(&mut self, doc_id: DocId, _score: Score) {
        let Some(column) = self.fast_field_reader.as_ref() else { return };
        let Some(partition_column) = self.partition_by_fast_field_reader.as_ref() else { return };
        if !self.accept_document(doc_id) {
            return;
        }
        if let Some(partition) = partition_column.first(doc_id) {
            self.fast_field_values
                .entry(partition)
                .or_default()
                .extend(column.values_for_doc(doc_id));
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.fast_field_values
    }
}
