use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use arrow::array::{Array, ArrayBuilder, StringArray, StringBuilder, UInt32Array, UInt32Builder};
use arrow::datatypes::DataType;
use hashbrown::hash_map::EntryRef;
use tantivy::columnar::Dictionary;

use crate::{PomskyArrowError, Result};

#[derive(Default)]
pub struct DictionaryBuilders {
    builders: HashMap<(String, DataType), DeltaDictionaryBuilder>,
}

impl DictionaryBuilders {
    pub fn get(&mut self, field_name: &str, data_type: &DataType) -> &mut DeltaDictionaryBuilder {
        self.builders
            .entry((field_name.to_owned(), data_type.clone()))
            .or_insert_with(DeltaDictionaryBuilder::new)
    }
}

// Maintains a mutable arrow dictionary for a single field
pub struct DeltaDictionaryBuilder {
    arrow_values_builder: StringBuilder,
    // We use a hashbrown hashmap here for the entry_ref method
    value_index_lookup: hashbrown::HashMap<String, u32>,
    ordinal_index_lookup: hashbrown::HashMap<u64, u32>,
    last_segment_ord: u32,
    cached_values_array: Option<Arc<StringArray>>,
}

impl DeltaDictionaryBuilder {
    fn new() -> Self {
        Self {
            arrow_values_builder: StringBuilder::new(),
            value_index_lookup: hashbrown::HashMap::new(),
            ordinal_index_lookup: hashbrown::HashMap::new(),
            last_segment_ord: u32::MAX,
            cached_values_array: None,
        }
    }

    pub fn encode(
        &mut self,
        segment_ord: u32,
        column_ordinals: &[Option<u64>],
        tantivy_dict: &Dictionary,
    ) -> Result<UInt32Array> {
        let mut unseen_ordinals: Vec<u64> = if self.last_segment_ord == segment_ord {
            // The segment is the same, so we can filter out any ordinals we've already seen to
            // avoid having to read them from the dictionary
            column_ordinals
                .iter()
                .flatten()
                .filter(|column_ordinal| !self.ordinal_index_lookup.contains_key(*column_ordinal))
                .copied()
                .collect()
        } else {
            // The segment changed, so we have to clear our ordinal to index map and can consider
            // all ordinals as unseen
            self.ordinal_index_lookup.clear();
            self.last_segment_ord = segment_ord;
            column_ordinals.iter().flatten().copied().collect()
        };

        // Remove any duplicates and sort the ordinals so we can efficiently read them from the
        // tantivy dictionary This is more efficient than using
        unseen_ordinals.sort_unstable();
        unseen_ordinals.dedup();

        // Read the values of the unseen ordinals in sorted-ordinal order and append them to the
        // dictionary if we didn't see their value in an earlier segment. The callback fires
        // once per requested ordinal in the same order, so appending values and assigning
        // indices stay in lockstep.
        let ordinal_to_index = &mut self.ordinal_index_lookup;
        let values = &mut self.arrow_values_builder;
        let values_to_index = &mut self.value_index_lookup;
        let mut unseen_iter = unseen_ordinals.iter();
        let mut callback_error = None;
        tantivy_dict
            .sorted_ords_to_term_cb(&unseen_ordinals, |term| {
                // Handling errors inside the callback is admittedly clunky, as there is no way to
                // shortcut out of the read once it's started
                if callback_error.is_some() {
                    return;
                }

                let term_str = match std::str::from_utf8(term) {
                    Ok(term_str) => term_str,
                    Err(error) => {
                        callback_error = Some(PomskyArrowError::Internal(format!(
                            "string fast-field dictionary term should be valid UTF-8: {error}"
                        )));
                        return;
                    }
                };

                // Look up the string value in the overall dict lookup to check if we saw that value
                // earlier in a different segment
                let index = match values_to_index.entry_ref(term_str) {
                    EntryRef::Occupied(occupied_entry) => *occupied_entry.get(),
                    EntryRef::Vacant(vacant_entry) => {
                        let index = match u32::try_from(values.len()) {
                            Ok(index) => index,
                            Err(_) => {
                                callback_error = Some(PomskyArrowError::Internal(
                                    "dictionary exceeded u32 indices".to_string(),
                                ));
                                return;
                            }
                        };

                        vacant_entry.insert(index);
                        values.append_value(term_str);

                        index
                    }
                };

                let ordinal = match unseen_iter.next() {
                    Some(ordinal) => *ordinal,
                    None => {
                        callback_error = Some(PomskyArrowError::Internal(
                            "dictionary callback returned more terms than requested ordinals"
                                .to_string(),
                        ));
                        return;
                    }
                };
                ordinal_to_index.insert(ordinal, index);
            })
            .map_err(|error| {
                PomskyArrowError::Internal(format!("read dictionary terms: {error}"))
            })?;

        if let Some(error) = callback_error {
            return Err(error);
        }

        // Finally map all the original ordinals to their actual arrow dictionary index
        let mut indices_builder = UInt32Builder::with_capacity(column_ordinals.len());
        for column_ordinal in column_ordinals {
            match column_ordinal {
                Some(ordinal) => match self.ordinal_index_lookup.get(ordinal) {
                    Some(index) => indices_builder.append_value(*index),
                    None => {
                        return Err(PomskyArrowError::Internal(format!(
                            "dictionary ordinal {ordinal} was not present in ordinal_index_lookup"
                        )));
                    }
                },
                None => indices_builder.append_null(),
            }
        }
        Ok(indices_builder.finish())
    }

    pub fn build_values_array(&mut self) -> Arc<StringArray> {
        match &self.cached_values_array {
            Some(cached_values_array)
                if self.arrow_values_builder.len() == cached_values_array.len() =>
            {
                return cached_values_array.clone();
            }
            _ => {}
        }

        let new_array = Arc::new(self.arrow_values_builder.finish_cloned());
        self.cached_values_array = Some(new_array.clone());
        new_array
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, UInt32Array};
    use arrow::datatypes::DataType;
    use tantivy::columnar::StrColumn;
    use tantivy::schema::{FAST, SchemaBuilder, TEXT};
    use tantivy::{Index, IndexWriter, TantivyDocument};

    use super::DictionaryBuilders;

    fn build_str_column(values: &[&str]) -> StrColumn {
        let mut schema_builder = SchemaBuilder::new();
        let field = schema_builder.add_text_field("field", FAST | TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000).unwrap();
        for value in values {
            let mut doc = TantivyDocument::default();
            doc.add_text(field, value);
            writer.add_document(doc).unwrap();
        }
        writer.commit().unwrap();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = &searcher.segment_readers()[0];
        segment_reader.fast_fields().str("field").unwrap().unwrap()
    }

    fn ordinals(str_column: &StrColumn, doc_ids: &[u32]) -> Vec<Option<u64>> {
        let mut ordinals = vec![None; doc_ids.len()];
        str_column.ords().first_vals(doc_ids, &mut ordinals);
        ordinals
    }

    fn assert_decoded_values(
        indices: &UInt32Array,
        values: &arrow::array::StringArray,
        expected: &[Option<&str>],
    ) {
        assert_eq!(indices.len(), expected.len());
        for (row_ord, expected_value) in expected.iter().enumerate() {
            match expected_value {
                Some(expected_value) => {
                    assert!(!indices.is_null(row_ord));
                    let value_index = indices.value(row_ord) as usize;
                    assert_eq!(values.value(value_index), *expected_value);
                }
                None => assert!(indices.is_null(row_ord)),
            }
        }
    }

    fn dictionary_values(values: &arrow::array::StringArray) -> Vec<&str> {
        (0..values.len())
            .map(|row_ord| values.value(row_ord))
            .collect()
    }

    #[test]
    fn only_encodes_used_values_in_dictionary() {
        let str_column = build_str_column(&["alpha", "beta", "gamma"]);
        let input_ordinals = ordinals(&str_column, &[0, 2]);

        let mut dictionary_builders = DictionaryBuilders::default();
        let dictionary_builder = dictionary_builders.get("field", &DataType::Utf8);
        let indices = dictionary_builder
            .encode(0, &input_ordinals, str_column.dictionary())
            .unwrap();
        let values = dictionary_builder.build_values_array();

        assert_decoded_values(&indices, &values, &[Some("alpha"), Some("gamma")]);
        assert_eq!(dictionary_values(&values), ["alpha", "gamma"]);
    }

    #[test]
    fn reuses_dictionary_entries_for_repeated_ordinals_in_same_segment() {
        let str_column = build_str_column(&["alpha", "beta", "gamma"]);
        let first_input_ordinals = ordinals(&str_column, &[0, 1]);
        let second_input_ordinals = ordinals(&str_column, &[2, 1]);

        let mut dictionary_builders = DictionaryBuilders::default();
        let dictionary_builder = dictionary_builders.get("field", &DataType::Utf8);
        let first_indices = dictionary_builder
            .encode(0, &first_input_ordinals, str_column.dictionary())
            .unwrap();
        let values = dictionary_builder.build_values_array();
        assert_eq!(dictionary_values(&values), ["alpha", "beta"]);

        let second_indices = dictionary_builder
            .encode(0, &second_input_ordinals, str_column.dictionary())
            .unwrap();
        let values = dictionary_builder.build_values_array();

        assert_decoded_values(&first_indices, &values, &[Some("alpha"), Some("beta")]);
        assert_decoded_values(&second_indices, &values, &[Some("gamma"), Some("beta")]);
        assert_eq!(first_indices.value(1), second_indices.value(1));
        assert_eq!(dictionary_values(&values), ["alpha", "beta", "gamma"]);
    }

    #[test]
    fn compare_entries_by_value_from_different_segments() {
        let first_str_column = build_str_column(&["alpha", "gamma", "epsilon"]);
        let second_str_column = build_str_column(&["alpha", "beta", "pi"]);
        let first_input_ordinals = ordinals(&first_str_column, &[0, 2]);
        let second_input_ordinals = ordinals(&second_str_column, &[0, 2]);

        let mut dictionary_builders = DictionaryBuilders::default();
        let dictionary_builder = dictionary_builders.get("field", &DataType::Utf8);
        let first_indices = dictionary_builder
            .encode(0, &first_input_ordinals, first_str_column.dictionary())
            .unwrap();
        let values = dictionary_builder.build_values_array();
        assert_eq!(dictionary_values(&values), ["alpha", "epsilon"]);

        let second_indices = dictionary_builder
            .encode(1, &second_input_ordinals, second_str_column.dictionary())
            .unwrap();
        let values = dictionary_builder.build_values_array();

        assert_decoded_values(&first_indices, &values, &[Some("alpha"), Some("epsilon")]);
        assert_decoded_values(&second_indices, &values, &[Some("alpha"), Some("pi")]);
        assert_eq!(first_indices.value(0), second_indices.value(0));
        assert_eq!(dictionary_values(&values), ["alpha", "epsilon", "pi"]);
    }

    #[test]
    fn reuse_existing_constructed_dictionary_where_possible() {
        let first_str_column = build_str_column(&["alpha", "beta"]);
        let second_str_column = build_str_column(&["beta", "alpha"]);
        let first_input_ordinals = ordinals(&first_str_column, &[0, 1]);
        let second_input_ordinals = ordinals(&second_str_column, &[0, 1]);

        let mut dictionary_builders = DictionaryBuilders::default();
        let dictionary_builder = dictionary_builders.get("field", &DataType::Utf8);
        dictionary_builder
            .encode(0, &first_input_ordinals, first_str_column.dictionary())
            .unwrap();
        let first_values = dictionary_builder.build_values_array();
        let second_indices = dictionary_builder
            .encode(1, &second_input_ordinals, second_str_column.dictionary())
            .unwrap();
        let second_values = dictionary_builder.build_values_array();

        assert_decoded_values(
            &second_indices,
            &second_values,
            &[Some("beta"), Some("alpha")],
        );
        assert!(Arc::ptr_eq(&first_values, &second_values));
    }
}
