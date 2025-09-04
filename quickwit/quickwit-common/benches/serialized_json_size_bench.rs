use binggan::{InputGroup, black_box};
use quickwit_common::serialized_json_size::{serialized_json_size, serialized_json_size_approx};
use serde_json::json;

fn mixed_json() -> serde_json::Value {
    json!({
        "id": 123,
        "name": "example",
        "active": true,
        "tags": ["alpha", "beta", "gamma"],
        "nested": {"a": [{"x":"nice"},{"x":2}], "b": {"y": 3.9, "z": null}}
    })
}

fn sample_text_heavy() -> serde_json::Value {
    let mut obj = serde_json::Map::with_capacity(10);
    obj.insert("field_1".to_string(), json!("hi"));
    obj.insert("field_2".to_string(), json!("ok"));
    obj.insert("field_3".to_string(), json!("foo"));
    obj.insert("field_4".to_string(), json!("bar baz"));
    obj.insert(
        "field_5".to_string(),
        json!(
            "this is a longer sample text to act as the large case; it is plain and readable, \
             without filler, but long enough to be meaningfully bigger than the short ones."
        ),
    );
    serde_json::Value::Object(obj)
}

fn sample_numbers() -> serde_json::Value {
    let mut obj = serde_json::Map::with_capacity(10);
    obj.insert("field_1".to_string(), json!(10));
    obj.insert("field_2".to_string(), json!(-10));
    obj.insert("field_3".to_string(), json!(-10.0));
    obj.insert("field_4".to_string(), json!(112343.42353));

    serde_json::Value::Object(obj)
}

fn main() {
    let inputs = vec![
        ("mixed", mixed_json()),
        ("text_heavy", sample_text_heavy()),
        ("numbers", sample_numbers()),
    ];
    let mut group: InputGroup<serde_json::Value, usize> = InputGroup::new_with_inputs(inputs);
    group.throughput(|s| serde_json::to_string(s).unwrap().len());

    group.register("exact", |v| serialized_json_size(black_box(v)).unwrap());
    group.register("approx", |v| serialized_json_size_approx(black_box(v)));

    group.run();
}
