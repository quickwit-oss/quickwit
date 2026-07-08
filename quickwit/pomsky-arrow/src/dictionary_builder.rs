use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use arrow::array::{Array, ArrayBuilder, StringArray, StringBuilder, UInt32Array, UInt32Builder};
use arrow::datatypes::DataType;
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
    value_index_lookup: HashMap<String, u32>,
    ordinal_index_lookup: HashMap<u64, u32>,
    last_segment_ord: u32,
    cached_values_array: Option<Arc<StringArray>>,
}

impl DeltaDictionaryBuilder {
    fn new() -> Self {
        Self {
            arrow_values_builder: StringBuilder::new(),
            value_index_lookup: HashMap::new(),
            ordinal_index_lookup: HashMap::new(),
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
        let mut unseen_ordinals: Vec<u64> = if self.last_segment_ord != segment_ord {
            // The segment changed, so we have to clear our ordinal to index map and can consider
            // all ordinals as unseen
            self.ordinal_index_lookup.clear();
            self.last_segment_ord = segment_ord;
            column_ordinals.iter().flatten().copied().collect()
        } else {
            // The segment is the same, so we can filter out any ordinals we've already seen to
            // avoid having to read them from the dictionary
            column_ordinals
                .iter()
                .flatten()
                .filter(|column_ordinal| !self.ordinal_index_lookup.contains_key(column_ordinal))
                .copied()
                .collect()
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
                let index = match values_to_index.entry(term_str.to_string()) {
                    Entry::Occupied(occupied_entry) => *occupied_entry.get(),
                    Entry::Vacant(vacant_entry) => {
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
