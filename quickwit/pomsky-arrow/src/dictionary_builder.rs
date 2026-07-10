use std::collections::HashMap;
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
    dictionary_term_index_map: hashbrown::HashMap<String, u32>,
    segment_local_ordinal_index_map: hashbrown::HashMap<u64, u32>,
    current_segment_ord: Option<u32>,
    cached_values_array: Option<Arc<StringArray>>,
}

impl DeltaDictionaryBuilder {
    fn new() -> Self {
        Self {
            arrow_values_builder: StringBuilder::new(),
            dictionary_term_index_map: hashbrown::HashMap::new(),
            segment_local_ordinal_index_map: hashbrown::HashMap::new(),
            current_segment_ord: None,
            cached_values_array: None,
        }
    }

    pub fn build_values_array(&mut self) -> Arc<StringArray> {
        if let Some(cached_values_array) = &self.cached_values_array
            && cached_values_array.len() == self.arrow_values_builder.len()
        {
            return cached_values_array.clone();
        }

        let new_array = Arc::new(self.arrow_values_builder.finish_cloned());
        self.cached_values_array = Some(new_array.clone());
        new_array
    }

    pub fn translate_dictionary(
        &mut self,
        segment_ord: u32,
        column_ordinals: &[Option<u64>],
        tantivy_dict: &Dictionary,
    ) -> Result<UInt32Array> {
        let unique_new_ordinals = self.find_unique_new_ordinals(segment_ord, column_ordinals);

        // Check capacity before entering the dictionary callback
        self.check_dictionary_capacity(unique_new_ordinals.len())?;

        // Read ordinals from tantivy dict and add them to the dictionary if they don't already
        // exist. Updates segment local ordinal map with the indices for each of the new
        // ordinals
        self.add_all_new_entries(&unique_new_ordinals, tantivy_dict)?;

        Self::build_indices(column_ordinals, &self.segment_local_ordinal_index_map)
    }

    /// Finds the unique sorted list of ordinals that don't exist in the segment-local ordinal
    /// lookup map. We will need to do a value lookup on these to see if they are in the
    /// dictionary
    fn find_unique_new_ordinals(
        &mut self,
        segment_ord: u32,
        column_ordinals: &[Option<u64>],
    ) -> Vec<u64> {
        let mut unseen_ordinals: Vec<u64> = if self.current_segment_ord == Some(segment_ord) {
            // The segment is the same, so we can filter out any ordinals we've already seen to
            // avoid having to read them from the dictionary
            column_ordinals
                .iter()
                .flatten()
                .filter(|column_ordinal| {
                    !self
                        .segment_local_ordinal_index_map
                        .contains_key(*column_ordinal)
                })
                .copied()
                .collect()
        } else {
            // The segment changed, so we have to clear our ordinal to index map and can consider
            // all ordinals as unseen
            self.segment_local_ordinal_index_map.clear();
            self.current_segment_ord = Some(segment_ord);
            column_ordinals.iter().flatten().copied().collect()
        };

        // Remove any duplicates and sort the ordinals so we can efficiently read them from the
        // tantivy dictionary This is more efficient than using
        unseen_ordinals.sort_unstable();
        unseen_ordinals.dedup();

        unseen_ordinals
    }

    /// Checks whether the number of new ordinals could cause the internal dictionary to exceed u32
    /// entries. This is a little overly-cautious as we don't yet know if the values already exist
    /// in the dictionary and therefore won't actually cause us to exceed the limit, but it is more
    /// convenient to check here.
    fn check_dictionary_capacity(&self, unique_new_ordinals_len: usize) -> Result<()> {
        let dictionary_would_exceed_u32 = match self
            .arrow_values_builder
            .len()
            .checked_add(unique_new_ordinals_len)
        {
            Some(dictionary_len) => dictionary_len > u32::MAX as usize,
            None => true,
        };
        if dictionary_would_exceed_u32 {
            return Err(PomskyArrowError::Internal(
                "dictionary exceeded u32 indices".to_string(),
            ));
        }
        Ok(())
    }

    /// Read the values of the unseen ordinals in sorted-ordinal order and append them to the
    /// dictionary if we didn't see their value in an earlier segment. The callback fires
    /// once per requested ordinal in the same order, so appending values and assigning
    /// indices stay in lockstep.
    fn add_all_new_entries(
        &mut self,
        unique_new_ordinals: &[u64],
        tantivy_dict: &Dictionary,
    ) -> Result<()> {
        let mut new_ordinals_iter = unique_new_ordinals.iter();
        tantivy_dict
            .sorted_ords_to_term_cb(unique_new_ordinals, |term| {
                let index = self.add_tantivy_term(term);

                let ordinal = *new_ordinals_iter
                    .next()
                    .expect("dictionary callback returned more terms than requested ordinals");
                self.segment_local_ordinal_index_map.insert(ordinal, index);
            })
            .map_err(|error| {
                PomskyArrowError::Internal(format!("read dictionary terms: {error}"))
            })?;
        Ok(())
    }

    fn add_tantivy_term(&mut self, term: &[u8]) -> u32 {
        let term_str = std::str::from_utf8(term)
            .expect("string fast-field dictionary term should be valid UTF-8");

        // Look up the string value in the overall dict lookup to check if we saw that value
        // earlier in a different segment
        match self.dictionary_term_index_map.entry_ref(term_str) {
            EntryRef::Occupied(occupied_entry) => *occupied_entry.get(),
            EntryRef::Vacant(vacant_entry) => {
                let index = u32::try_from(self.arrow_values_builder.len())
                    .expect("dictionary exceeded u32 indices");

                self.arrow_values_builder.append_value(term_str);
                vacant_entry.insert(index);

                index
            }
        }
    }

    /// Map all the original ordinals to their actual arrow dictionary index and build the arrow
    /// indices array
    fn build_indices(
        column_ordinals: &[Option<u64>],
        ordinal_index_lookup: &hashbrown::HashMap<u64, u32>,
    ) -> Result<UInt32Array> {
        let mut indices_builder = UInt32Builder::with_capacity(column_ordinals.len());
        for column_ordinal in column_ordinals {
            match column_ordinal {
                Some(ordinal) => {
                    let Some(index) = ordinal_index_lookup.get(ordinal) else {
                        return Err(PomskyArrowError::Internal(format!(
                            "dictionary ordinal {ordinal} was not present in ordinal_index_lookup"
                        )));
                    };
                    indices_builder.append_value(*index)
                }
                None => indices_builder.append_null(),
            }
        }
        Ok(indices_builder.finish())
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
            .translate_dictionary(0, &input_ordinals, str_column.dictionary())
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
            .translate_dictionary(0, &first_input_ordinals, str_column.dictionary())
            .unwrap();
        let values = dictionary_builder.build_values_array();
        assert_eq!(dictionary_values(&values), ["alpha", "beta"]);

        let second_indices = dictionary_builder
            .translate_dictionary(0, &second_input_ordinals, str_column.dictionary())
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
            .translate_dictionary(0, &first_input_ordinals, first_str_column.dictionary())
            .unwrap();
        let values = dictionary_builder.build_values_array();
        assert_eq!(dictionary_values(&values), ["alpha", "epsilon"]);

        let second_indices = dictionary_builder
            .translate_dictionary(1, &second_input_ordinals, second_str_column.dictionary())
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
            .translate_dictionary(0, &first_input_ordinals, first_str_column.dictionary())
            .unwrap();
        let first_values = dictionary_builder.build_values_array();
        let second_indices = dictionary_builder
            .translate_dictionary(1, &second_input_ordinals, second_str_column.dictionary())
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
