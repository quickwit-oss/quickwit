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

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use fastfield_codecs::Column;
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::fastfield::FastValue;
use tantivy::{DocId, Score, SegmentOrdinal, SegmentReader, TantivyError};

use crate::filters::{TimestampFilter, TimestampFilterBuilder};

#[derive(Clone)]
pub struct FastFieldSegmentCollector<Item: FastValue> {
    fast_field_values: Vec<Item>,
    fast_field_reader: Arc<dyn Column<Item>>,
    timestamp_filter_opt: Option<TimestampFilter>,
}

impl<Item: FastValue> FastFieldSegmentCollector<Item> {
    pub fn new(
        fast_field_reader: Arc<dyn Column<Item>>,
        timestamp_filter_opt: Option<TimestampFilter>,
    ) -> Self {
        Self {
            fast_field_values: vec![],
            fast_field_reader,
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

impl<Item: FastValue> SegmentCollector for FastFieldSegmentCollector<Item> {
    type Fruit = Vec<Item>;

    fn collect(&mut self, doc_id: DocId, _score: Score) {
        if !self.accept_document(doc_id) {
            return;
        }
        let fast_field_value = self.fast_field_reader.get_val(doc_id as u64);
        self.fast_field_values.push(fast_field_value);
    }

    fn harvest(self) -> Vec<Item> {
        self.fast_field_values
    }
}

#[derive(Clone)]
pub struct FastFieldCollector<Item: FastValue> {
    pub fast_field_to_collect: String,
    pub timestamp_filter_builder_opt: Option<TimestampFilterBuilder>,
    pub _marker: PhantomData<Item>,
}

impl<Item: FastValue> Collector for FastFieldCollector<Item> {
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

        let fast_field_reader =
            helpers::make_fast_field_reader::<Item>(segment_reader, &self.fast_field_to_collect)?;

        Ok(FastFieldSegmentCollector::new(
            fast_field_reader,
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
pub struct PartionnedFastFieldCollector<Item: FastValue, PartitionItem: FastValue> {
    pub fast_field_to_collect: String,
    pub partition_by_fast_field: String,
    pub timestamp_filter_builder_opt: Option<TimestampFilterBuilder>,
    pub _marker: PhantomData<(Item, PartitionItem)>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct PartitionValues<Item: FastValue, PartitionItem: FastValue> {
    pub partition_value: PartitionItem,
    pub fast_field_values: Vec<Item>,
}

impl<Item: FastValue, PartitionItem: FastValue + Eq + Hash> Collector
    for PartionnedFastFieldCollector<Item, PartitionItem>
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
        let fast_field_reader =
            helpers::make_fast_field_reader::<Item>(segment_reader, &self.fast_field_to_collect)?;

        let partition_by_fast_field_reader = helpers::make_fast_field_reader::<PartitionItem>(
            segment_reader,
            &self.partition_by_fast_field,
        )?;

        Ok(PartitionedFastFieldSegmentCollector::new(
            fast_field_reader,
            partition_by_fast_field_reader,
            timestamp_filter_opt,
        ))
    }

    fn requires_scoring(&self) -> bool {
        // We do not need BM25 scoring in Quickwit.
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<std::collections::HashMap<PartitionItem, Vec<Item>>>,
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
pub struct PartitionedFastFieldSegmentCollector<
    Item: FastValue,
    PartitionItem: FastValue + Eq + Hash,
> {
    fast_field_values: std::collections::HashMap<PartitionItem, Vec<Item>>,
    fast_field_reader: Arc<dyn Column<Item>>,
    partition_by_fast_field_reader: Arc<dyn Column<PartitionItem>>,
    timestamp_filter_opt: Option<TimestampFilter>,
}

impl<Item: FastValue, PartitionItem: FastValue + Eq + Hash>
    PartitionedFastFieldSegmentCollector<Item, PartitionItem>
{
    pub fn new(
        fast_field_reader: Arc<dyn Column<Item>>,
        partition_by_fast_field_reader: Arc<dyn Column<PartitionItem>>,
        timestamp_filter_opt: Option<TimestampFilter>,
    ) -> Self {
        Self {
            fast_field_values: std::collections::HashMap::default(),
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

impl<Item: FastValue, PartitionItem: FastValue + Hash + Eq> SegmentCollector
    for PartitionedFastFieldSegmentCollector<Item, PartitionItem>
{
    type Fruit = HashMap<PartitionItem, Vec<Item>>;

    fn collect(&mut self, doc_id: DocId, _score: Score) {
        if !self.accept_document(doc_id) {
            return;
        }
        let fast_field_value = self.fast_field_reader.get_val(doc_id as u64);
        let fast_field_partition = &self.partition_by_fast_field_reader.get_val(doc_id as u64);
        if let Some(values) = self.fast_field_values.get_mut(fast_field_partition) {
            values.push(fast_field_value);
        } else {
            self.fast_field_values
                .insert(*fast_field_partition, vec![fast_field_value]);
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.fast_field_values
    }
}

mod helpers {
    use std::sync::Arc;

    use super::*;

    pub fn make_fast_field_reader<T: FastValue>(
        segment_reader: &SegmentReader,
        fast_field_to_collect: &str,
    ) -> tantivy::Result<Arc<dyn Column<T>>> {
        let field = segment_reader
            .schema()
            .get_field(fast_field_to_collect)
            .ok_or_else(|| TantivyError::SchemaError("field does not exist".to_owned()))?;
        let fast_field_slice = segment_reader.fast_fields().fast_field_data(field, 0)?;
        let bytes = fast_field_slice.read_bytes()?;
        let column = fastfield_codecs::open(bytes)?;
        Ok(column)
    }
}
