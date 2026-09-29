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

use std::fs::File;
use std::sync::Arc;

use arrow_array::types::{Float16Type, Float64Type, Int32Type};
use arrow_array::{
    ArrayRef, ArrowPrimitiveType, BinaryArray, BinaryViewArray, DictionaryArray,
    FixedSizeBinaryArray, FixedSizeListArray, Float16Array, Float32Array, Float64Array, Int32Array,
    LargeBinaryArray, LargeListArray, ListArray, PrimitiveArray, RecordBatch, StructArray,
};
use arrow_schema::Field;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use quickwit_doc_mapper::DocMapperBuilder;
use serde_json::json;
use tantivy::schema::Value;

use super::source::record_batch_to_ndjson_docs;
use super::testsuite::write_record_batch_as_parquet_file;

fn batch(array: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter([("value", array)]).unwrap()
}

fn nested(array: ArrayRef) -> ArrayRef {
    Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("inner", array.data_type().clone(), true)),
        array,
    )]))
}

#[test]
fn test_parquet_native_binary_preserves_default_bytes_mapping() {
    let values = vec![Some(&[1, 2][..]), None, Some(&[0, 255][..])];
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(BinaryArray::from(values.clone())),
        Arc::new(LargeBinaryArray::from(values.clone())),
        Arc::new(BinaryViewArray::from(values.clone())),
        Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 2).unwrap(),
        ),
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0), None, Some(1)]),
                Arc::new(BinaryArray::from(vec![&[1, 2][..], &[0, 255][..]])),
            )
            .unwrap(),
        ),
    ];
    let mapper = serde_json::from_value::<DocMapperBuilder>(json!({
        "field_mappings": [{"name": "value", "type": "bytes"}]
    }))
    .unwrap()
    .try_build()
    .unwrap();
    let field = mapper.schema().get_field("value").unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().join("binary.parquet");

    for array in arrays {
        let native_batch = batch(array);
        write_record_batch_as_parquet_file(&path, &native_batch, 3).unwrap();
        let decoded_batch = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
            .unwrap()
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        for record_batch in [&native_batch, &decoded_batch] {
            let docs = record_batch_to_ndjson_docs(record_batch).unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&docs[0]).unwrap(),
                json!({"value": "AQI="})
            );
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&docs[1]).unwrap(),
                json!({})
            );
            for (row, expected) in [(0, &[1, 2][..]), (2, &[0, 255][..])] {
                let (_, doc) = mapper.doc_from_json_bytes(&docs[row]).unwrap();
                assert_eq!(doc.get_first(field).unwrap().as_bytes().unwrap(), expected);
            }
        }
    }
}

#[test]
fn test_parquet_binary_nested_list_dictionary_and_empty_values() {
    let array = Arc::new(BinaryArray::from(vec![&[1, 2][..], &[][..]])) as ArrayRef;
    let field = Arc::new(Field::new("item", array.data_type().clone(), false));
    let array = Arc::new(FixedSizeListArray::try_new(field, 2, array, None).unwrap());
    let dictionary =
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0]), array).unwrap();
    let docs = record_batch_to_ndjson_docs(&batch(nested(Arc::new(dictionary)))).unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&docs[0]).unwrap(),
        json!({"value": {"inner": ["AQI=", ""]}})
    );
}

#[test]
fn test_parquet_rejects_nonfinite_float_widths_and_nested_values() {
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float16Array::from(vec![
                Some(<Float16Type as ArrowPrimitiveType>::Native::from_f64(value)),
                None,
            ])),
            Arc::new(Float32Array::from(vec![Some(value as f32), None])),
            Arc::new(Float64Array::from(vec![Some(value), None])),
        ];
        for array in arrays {
            let dictionary = Arc::new(
                DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0, 1]), array.clone())
                    .unwrap(),
            ) as ArrayRef;
            let field = Arc::new(Field::new("item", dictionary.data_type().clone(), true));
            let list =
                Arc::new(FixedSizeListArray::try_new(field, 2, dictionary.clone(), None).unwrap());
            for candidate in [array, dictionary, nested(list)] {
                let error = record_batch_to_ndjson_docs(&batch(candidate.clone())).unwrap_err();
                let details = format!("{error:#}");
                assert!(
                    details.contains("non-finite float"),
                    "{}: {details}",
                    candidate.data_type()
                );
                assert!(
                    details.contains("value") || details.contains("inner"),
                    "{details}"
                );
            }
        }
        for array in [
            Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>([Some(
                vec![Some(1.0), Some(value)],
            )])) as ArrayRef,
            Arc::new(LargeListArray::from_iter_primitive::<Float64Type, _, _>([
                Some(vec![Some(1.0), Some(value)]),
            ])),
        ] {
            let error = record_batch_to_ndjson_docs(&batch(nested(array))).unwrap_err();
            assert!(format!("{error:#}").contains("non-finite float"));
        }
    }
}

#[test]
fn test_parquet_masked_nonfinite_floats_remain_supported() {
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float16Array::from(vec![
                <Float16Type as ArrowPrimitiveType>::Native::from_f64(value),
                <Float16Type as ArrowPrimitiveType>::Native::from_f32(1.5),
            ])),
            Arc::new(Float32Array::from(vec![value as f32, 1.5])),
            Arc::new(Float64Array::from(vec![value, 1.5])),
        ];
        for array in arrays {
            let field = Arc::new(Field::new("inner", array.data_type().clone(), true));
            let parent = StructArray::new(
                vec![field.clone()].into(),
                vec![array.clone()],
                Some(vec![false, true].into()),
            );
            let docs = record_batch_to_ndjson_docs(&batch(Arc::new(parent))).unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&docs[0]).unwrap(),
                json!({})
            );
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&docs[1]).unwrap(),
                json!({"value": {"inner": 1.5}})
            );

            let offsets = ListArray::from_iter_primitive::<Float64Type, _, _>([
                Some(vec![Some(value)]),
                Some(vec![Some(1.5)]),
            ])
            .offsets()
            .clone();
            let list = ListArray::new(
                field.clone(),
                offsets,
                array.clone(),
                Some(vec![false, true].into()),
            );
            let fixed_list =
                FixedSizeListArray::new(field, 1, array.clone(), Some(vec![false, true].into()));
            for list in [Arc::new(list) as ArrayRef, Arc::new(fixed_list)] {
                let docs = record_batch_to_ndjson_docs(&batch(list)).unwrap();
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&docs[0]).unwrap(),
                    json!({})
                );
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&docs[1]).unwrap(),
                    json!({"value": [1.5]})
                );
            }

            let dictionary =
                DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![Some(1), None]), array)
                    .unwrap();
            let docs = record_batch_to_ndjson_docs(&batch(Arc::new(dictionary))).unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&docs[0]).unwrap(),
                json!({"value": 1.5})
            );
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&docs[1]).unwrap(),
                json!({})
            );
        }
        let nullable = Arc::new(Float64Array::new(
            vec![value, 1.5].into(),
            Some(vec![false, true].into()),
        ));
        let docs = record_batch_to_ndjson_docs(&batch(nullable.clone())).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&docs[0]).unwrap(),
            json!({})
        );
        let dictionary =
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0, 1]), nullable).unwrap();
        let docs = record_batch_to_ndjson_docs(&batch(Arc::new(dictionary))).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&docs[0]).unwrap(),
            json!({})
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&docs[1]).unwrap(),
            json!({"value": 1.5})
        );
    }
}

#[test]
fn test_parquet_dictionary_referenced_value_nulls() {
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let floats = Arc::new(Float64Array::from(vec![value, 1.5])) as ArrayRef;
        let field = Arc::new(Field::new("inner", floats.data_type().clone(), true));
        let parent = Arc::new(StructArray::new(
            vec![field.clone()].into(),
            vec![floats.clone()],
            Some(vec![false, true].into()),
        )) as ArrayRef;
        let list = Arc::new(FixedSizeListArray::new(
            field,
            1,
            floats.clone(),
            Some(vec![false, true].into()),
        )) as ArrayRef;
        macro_rules! check_dictionary_keys {
            ($($key_type:ident),+) => {$(
                for (values, expected) in [
                    (parent.clone(), json!({"value": {"inner": 1.5}})),
                    (list.clone(), json!({"value": [1.5]})),
                ] {
                    let dictionary = DictionaryArray::<arrow_array::types::$key_type>::try_new(
                        PrimitiveArray::from_iter_values([0, 1]), values,
                    ).unwrap();
                    let docs = record_batch_to_ndjson_docs(&batch(Arc::new(dictionary))).unwrap();
                    assert_eq!(serde_json::from_slice::<serde_json::Value>(&docs[0]).unwrap(), json!({}));
                    assert_eq!(serde_json::from_slice::<serde_json::Value>(&docs[1]).unwrap(), expected);
                }
                let dictionary = DictionaryArray::<arrow_array::types::$key_type>::try_new(
                    PrimitiveArray::from_iter_values([0, 1]), floats.clone(),
                ).unwrap();
                let error = record_batch_to_ndjson_docs(&batch(Arc::new(dictionary))).unwrap_err();
                assert!(format!("{error:#}").contains("non-finite float"));
            )+};
        }
        check_dictionary_keys!(
            Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type,
            UInt64Type
        );
    }
}

#[test]
fn test_parquet_finite_floats_and_nulls_remain_supported() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Float16Array::from(vec![
            Some(<Float16Type as ArrowPrimitiveType>::Native::from_f32(1.5)),
            None,
        ])),
        Arc::new(Float32Array::from(vec![Some(1.5), None])),
        Arc::new(Float64Array::from(vec![Some(1.5), None])),
    ];
    for array in arrays {
        let docs = record_batch_to_ndjson_docs(&batch(array)).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&docs[0]).unwrap(),
            json!({"value": 1.5})
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&docs[1]).unwrap(),
            json!({})
        );
    }
}
