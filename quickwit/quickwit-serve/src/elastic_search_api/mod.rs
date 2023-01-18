use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

mod api_specs;

/// Serializes an `Option<&[Serialize]>` with
/// `Some(value)` to a comma separated string of values.
/// Used to serialize values within the query string
pub(crate) fn to_simple_list<S, T>(
    value: &Option<Vec<T>>,
    serializer: S,
) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
where
    S: Serializer,
    T: Serialize,
{
    let vec = &value
        .as_ref()
        .expect("attempt to serialize Option::None value");

    let serialized = vec
        .iter()
        .map(|v| serde_json::to_string(v).unwrap())
        .collect::<Vec<_>>();

    let target = serialized
        .iter()
        .map(|s| s.trim_matches('"'))
        .collect::<Vec<_>>()
        .join(",");

    serializer.serialize_str(&target)
}

pub(crate) fn from_simple_list<'de, D, T>(deserializer: D) -> Result<Option<Vec<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned, // TODO find fix for Deserialize<'a>
{
    let str_sequence = String::deserialize(deserializer)?;

    let list = str_sequence
        .trim_matches(',')
        .split(',')
        .map(|item| serde_json::from_str::<T>(item))
        .collect::<Result<Vec<_>, _>>()
        .map_err(serde::de::Error::custom)?;
    Ok(Some(list))
}

/// Control how the total number of hits should be tracked.
///
/// When set to `Track` with a value `true`, the response will always track the number of hits that
/// match the query accurately.
///
/// When set to `Count` with an integer value `n`, the response accurately tracks the total
/// hit count that match the query up to `n` documents.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TrackTotalHits {
    /// Whether to accurately track the number of hits that match the query accurately
    Track(bool),
    /// Accurately track the number of hits up to the specified value
    Count(i64),
}

impl From<bool> for TrackTotalHits {
    fn from(b: bool) -> Self {
        TrackTotalHits::Track(b)
    }
}

impl From<i64> for TrackTotalHits {
    fn from(i: i64) -> Self {
        TrackTotalHits::Count(i)
    }
}
