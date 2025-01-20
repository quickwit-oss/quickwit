use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_proto::cloudprem::{
    CloudPremError, CloudPremResult, CloudPremService, ContentKv, Event, FetchOneRequest,
    FetchOneResponse, ListRequest, ListResponse, PingRequest, PingResponse,
};
use quickwit_proto::search::{CountHits, Hit, SearchRequest, SortField, SortOrder};
use quickwit_search::SearchService;
use serde_json::Value as JsonValue;
use tracing::info;

#[allow(dead_code)]
pub struct CloudPremServiceImpl {
    search_service: Arc<dyn SearchService>,
}

impl fmt::Debug for CloudPremServiceImpl {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CloudPremServiceImpl")
    }
}

impl From<Arc<dyn SearchService>> for CloudPremServiceImpl {
    fn from(search_service: Arc<dyn SearchService>) -> Self {
        CloudPremServiceImpl { search_service }
    }
}

#[async_trait]
impl CloudPremService for CloudPremServiceImpl {
    async fn ping(&self, _request: PingRequest) -> CloudPremResult<PingResponse> {
        info!("Received Ping request");
        Ok(PingResponse {})
    }

    async fn list(&self, request: ListRequest) -> CloudPremResult<ListResponse> {
        info!("Received List request");

        let Some(query) = request.query else {
            return Err(CloudPremError::Internal("missing query".to_string()));
        };
        let query_evp_ast = quickwit_query::cloudprem::parse_query(query)
            .map_err(|err| CloudPremError::InvalidQuery(format!("failed to parse query: {err}")))?;
        let query_ast = quickwit_query::cloudprem::to_quickwit_query(query_evp_ast)?;

        let count_hits = if request.should_compute_count {
            CountHits::CountAll
        } else {
            CountHits::Underestimate
        };
        let search_request = SearchRequest {
            index_id_patterns: vec!["cloudprem".to_string()], /* TODO this should become
                                                               * configurable and sent by EVP */
            query_ast: serde_json::to_string(&query_ast)
                .map_err(|e| CloudPremError::Internal(e.to_string()))?,
            start_timestamp: None,
            end_timestamp: None,
            max_hits: request.num_events_to_fetch.into(),
            start_offset: 0,
            aggregation_request: None,
            snippet_fields: Vec::new(),
            sort_fields: request
                .sort
                .into_iter()
                .map(|sort_kv| SortField {
                    field_name: sort_kv.path, // or should it be .name ?
                    sort_order: if sort_kv.ascending {
                        SortOrder::Asc
                    } else {
                        SortOrder::Desc
                    }
                    .into(),
                    sort_datetime_format: None,
                })
                .collect(),
            scroll_ttl_secs: None,
            search_after: None,
            count_hits: count_hits.into(),
        };

        let response = self.search_service.root_search(search_request).await?;

        let hit_mapper = HitMapper {
            columns: request.columns,
        };
        let events = response
            .hits
            .into_iter()
            .map(|hit| hit_mapper.hit_to_event(hit))
            .collect::<Result<_, _>>()?;

        Ok(ListResponse {
            count: response.num_hits,
            streams: vec![quickwit_proto::cloudprem::Stream { events }],
            statistics: None,
        })
    }

    async fn fetch_one(&self, _request: FetchOneRequest) -> CloudPremResult<FetchOneResponse> {
        info!("Received FetchOne request");
        Err(CloudPremError::Unimplemented)
    }
}

struct HitMapper {
    // i assume we'll likely need a more tree-like structure in the future
    #[allow(dead_code)]
    columns: Vec<String>,
}

impl HitMapper {
    fn hit_to_event(&self, hit: Hit) -> CloudPremResult<Event> {
        // TODO we probably want to add the PartialHit as a dedicated "id" field or something like
        // that?
        let map: serde_json::Map<String, JsonValue> = serde_json::from_str(&hit.json)
            .map_err(|e| CloudPremError::Internal(format!("failed to parse hit: {e}")))?;

        // TODO filter by columns
        let field_values = JsonValueIterator::new(map)
            .map(|(key, value)| ContentKv {
                key,
                value: value.to_string(),
            })
            .collect();
        Ok(Event {
            content_size: hit.json.len() as u32, // TODO that's probably not what we want
            field_values,
        })
    }
}

enum MapOrArrayIter {
    Array(std::vec::IntoIter<JsonValue>),
    Map(serde_json::map::IntoIter),
}

impl MapOrArrayIter {
    fn is_map(&self) -> bool {
        matches!(self, MapOrArrayIter::Map(_))
    }
}

impl Iterator for MapOrArrayIter {
    type Item = (Option<String>, JsonValue);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            MapOrArrayIter::Array(iter) => iter.next().map(|value| (None, value)),
            MapOrArrayIter::Map(iter) => iter.next().map(|(key, value)| (Some(key), value)),
        }
    }
}

/// Iterate over all primitive values inside the provided JsonValue, ignoring Nulls, and opening
/// arrays and objects.
pub(crate) struct JsonValueIterator {
    stack: Vec<MapOrArrayIter>,
    current_key: String,
    dot_positions: Vec<usize>,
}

impl JsonValueIterator {
    pub fn new(source: serde_json::Map<String, JsonValue>) -> JsonValueIterator {
        let base_value = MapOrArrayIter::Map(source.into_iter());
        JsonValueIterator {
            stack: vec![base_value],
            current_key: String::new(),
            dot_positions: vec![0],
        }
    }
}

impl Iterator for JsonValueIterator {
    type Item = (String, JsonValue);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let currently_itered = self.stack.last_mut()?;

            if currently_itered.is_map() {
                // for map, we push pop the last part between each key,
                // for array, we mustn't
                if let Some(dot_pos) = self.dot_positions.last() {
                    self.current_key.truncate(*dot_pos);
                }
            }
            let Some((path, value)) = currently_itered.next() else {
                let poped = self.stack.pop().unwrap();
                if poped.is_map() {
                    self.dot_positions.pop();
                }
                continue;
            };
            if let Some(path) = path {
                self.current_key.push('.');
                self.current_key.push_str(&path);
            }
            match value {
                JsonValue::Array(array) => {
                    self.stack.push(MapOrArrayIter::Array(array.into_iter()));
                }
                JsonValue::Object(map) => {
                    self.dot_positions.push(self.current_key.len());
                    self.stack.push(MapOrArrayIter::Map(map.into_iter()));
                }
                JsonValue::Null => continue,
                value => {
                    // we always push '.' + key, to not prefix all paths with a '.',
                    // we need to ignore that initial '.' everywhere
                    let key = self.current_key[1..].to_string();
                    return Some((key, value));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::{json, Value as JsonValue};

    use super::JsonValueIterator;

    #[test]
    fn test_json_value_iterator() {
        let source_json = json!({
            "a": 1,
            "b": [2, 3],
            "c": {
                "d": 4,
                "e": 5,
                "f": ["f", 7]
            },
            "g": [
                {
                    "h": 8,
                    "i": 9
                },
                10,
                [11, 12]
            ]
        });
        let expected: HashMap<_, _> = [
            ("a", json!(1)),
            ("b", json!(2)),
            ("b", json!(3)),
            ("c.d", json!(4)),
            ("c.e", json!(5)),
            ("c.f", json!("f")),
            ("c.f", json!(7)),
            ("g.h", json!(8)),
            ("g.i", json!(9)),
            ("g", json!(10)),
            ("g", json!(11)),
            ("g", json!(12)),
        ]
        .into_iter()
        // invert key and value to not need a multimap (some keys are duplicated,
        // values were choosen to not be)
        .map(|(k, v)| (v, k.to_string()))
        .collect();
        let JsonValue::Object(map) = source_json else {
            panic!("should have been a map");
        };

        let extracted: HashMap<_, _> = JsonValueIterator::new(map).map(|(k, v)| (v, k)).collect();
        assert_eq!(extracted, expected);
    }
}
