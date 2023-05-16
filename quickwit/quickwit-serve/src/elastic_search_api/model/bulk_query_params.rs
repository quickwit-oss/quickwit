use quickwit_ingest::CommitType;
use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct ElasticIngestOptions {
    #[serde(default)]
    pub refresh: ElasticRefresh,
}

/// ?refresh parameter for elasticsearch bulk request
///
/// The syntax for this parameter is a bit confusing for backward compatibility reasons.
/// - Absence of ?refresh parameter or ?refresh=false means no refresh
/// - Presence of ?refresh parameter without any values or ?refresh=true means force refresh
/// - ?refresh=wait_for means wait for refresh
#[derive(Clone, Debug, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all(deserialize = "snake_case"))]
#[derive(Default)]
pub enum ElasticRefresh {
    // if the refresh parameter is not present it is false
    #[default]
    /// The request doesn't wait for commit
    False,
    // but if it is present without a value like this: ?refresh, it should be the same as
    // ?refresh=true
    #[serde(alias = "")]
    /// The request forces an immediate commit after the last document in the batch and waits for
    /// it to finish.
    True,
    /// The request will wait for the next scheduled commit to finish.
    WaitFor,
}

impl From<ElasticRefresh> for CommitType {
    fn from(val: ElasticRefresh) -> Self {
        match val {
            ElasticRefresh::False => CommitType::Auto,
            ElasticRefresh::True => CommitType::Force,
            ElasticRefresh::WaitFor => CommitType::WaitFor,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::elastic_search_api::model::{ElasticIngestOptions, ElasticRefresh};

    #[test]
    fn test_elastic_refresh_parsing() {
        assert_eq!(
            serde_qs::from_str::<ElasticIngestOptions>("")
                .unwrap()
                .refresh,
            ElasticRefresh::False
        );
        assert_eq!(
            serde_qs::from_str::<ElasticIngestOptions>("refresh=true")
                .unwrap()
                .refresh,
            ElasticRefresh::True
        );
        assert_eq!(
            serde_qs::from_str::<ElasticIngestOptions>("refresh=false")
                .unwrap()
                .refresh,
            ElasticRefresh::False
        );
        assert_eq!(
            serde_qs::from_str::<ElasticIngestOptions>("refresh=wait_for")
                .unwrap()
                .refresh,
            ElasticRefresh::WaitFor
        );
        assert_eq!(
            serde_qs::from_str::<ElasticIngestOptions>("refresh")
                .unwrap()
                .refresh,
            ElasticRefresh::True
        );
        assert_eq!(
            serde_qs::from_str::<ElasticIngestOptions>("refresh=wait")
                .unwrap_err()
                .to_string(),
            "unknown variant `wait`, expected one of `false`, `true`, `wait_for`"
        );
    }
}
