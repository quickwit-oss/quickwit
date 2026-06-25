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

use std::ops::Bound;

use quickwit_datetime::StrptimeParser;
use serde::Deserialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::JsonLiteral;
use crate::elastic_query_dsl::ConvertibleToQueryAst;
use crate::elastic_query_dsl::one_field_map::OneFieldMap;
use crate::not_nan_f32::NotNaNf32;
use crate::query_ast::QueryAst;

#[derive(Deserialize, Debug, Default, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub struct RangeQueryParams {
    #[serde(default)]
    gt: Option<JsonLiteral>,
    #[serde(default)]
    gte: Option<JsonLiteral>,
    #[serde(default)]
    lt: Option<JsonLiteral>,
    #[serde(default)]
    lte: Option<JsonLiteral>,
    #[serde(default)]
    boost: Option<NotNaNf32>,
    #[serde(default)]
    format: Option<JsonLiteral>,
}

pub type RangeQuery = OneFieldMap<RangeQueryParams>;

impl ConvertibleToQueryAst for RangeQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        let field = self.field;
        let RangeQueryParams {
            gt,
            gte,
            lt,
            lte,
            boost,
            format,
        } = self.value;
        let (gt, gte, lt, lte) = match format {
            // `epoch_millis` and `epoch_second` are not date patterns: the bounds are raw epoch
            // numbers. They must be handled explicitly, otherwise they would be (wrongly) parsed
            // as Java date format patterns (e.g. the `h` in `epoch_millis` is the 12-hour token).
            Some(JsonLiteral::String(format)) if format == "epoch_millis" => (
                gt.map(|v| convert_epoch(v, EpochUnit::Millis))
                    .transpose()?,
                gte.map(|v| convert_epoch(v, EpochUnit::Millis))
                    .transpose()?,
                lt.map(|v| convert_epoch(v, EpochUnit::Millis))
                    .transpose()?,
                lte.map(|v| convert_epoch(v, EpochUnit::Millis))
                    .transpose()?,
            ),
            Some(JsonLiteral::String(format)) if format == "epoch_second" => (
                gt.map(|v| convert_epoch(v, EpochUnit::Seconds))
                    .transpose()?,
                gte.map(|v| convert_epoch(v, EpochUnit::Seconds))
                    .transpose()?,
                lt.map(|v| convert_epoch(v, EpochUnit::Seconds))
                    .transpose()?,
                lte.map(|v| convert_epoch(v, EpochUnit::Seconds))
                    .transpose()?,
            ),
            Some(JsonLiteral::String(java_date_format)) => {
                let parser = StrptimeParser::from_java_datetime_format(&java_date_format).map_err(
                    |err| anyhow::anyhow!("failed to parse range query date format. {err}"),
                )?;
                (
                    gt.map(|v| parse_and_convert(v, &parser)).transpose()?,
                    gte.map(|v| parse_and_convert(v, &parser)).transpose()?,
                    lt.map(|v| parse_and_convert(v, &parser)).transpose()?,
                    lte.map(|v| parse_and_convert(v, &parser)).transpose()?,
                )
            }
            _ => (gt, gte, lt, lte),
        };

        let range_query_ast = crate::query_ast::RangeQuery {
            field,
            lower_bound: match (gt, gte) {
                (Some(_gt), Some(_gte)) => {
                    anyhow::bail!("both gt and gte are set")
                }
                (Some(gt), None) => Bound::Excluded(gt),
                (None, Some(gte)) => Bound::Included(gte),
                (None, None) => Bound::Unbounded,
            },
            upper_bound: match (lt, lte) {
                (Some(_lt), Some(_lte)) => {
                    anyhow::bail!("both lt and lte are set")
                }
                (Some(lt), None) => Bound::Excluded(lt),
                (None, Some(lte)) => Bound::Included(lte),
                (None, None) => Bound::Unbounded,
            },
        };
        let ast: QueryAst = range_query_ast.into();
        Ok(ast.boost(boost))
    }
}

fn parse_and_convert(literal: JsonLiteral, parser: &StrptimeParser) -> anyhow::Result<JsonLiteral> {
    if let JsonLiteral::String(date_time_str) = literal {
        let parsed_date_time = parser
            .parse_date_time(&date_time_str)
            .map_err(|reason| anyhow::anyhow!("Failed to parse date time: {}", reason))?;
        let parsed_date_time_rfc3339 = parsed_date_time.format(&Rfc3339)?;
        Ok(JsonLiteral::String(parsed_date_time_rfc3339))
    } else {
        Ok(literal)
    }
}

#[derive(Clone, Copy)]
enum EpochUnit {
    Seconds,
    Millis,
}

/// Converts an epoch bound (expressed as a number or a numeric string) into an RFC3339 string,
/// honoring the unit declared by the `epoch_second` / `epoch_millis` format.
fn convert_epoch(literal: JsonLiteral, unit: EpochUnit) -> anyhow::Result<JsonLiteral> {
    let timestamp: i64 = match &literal {
        JsonLiteral::Number(number) => number
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("expected an integer epoch value, got `{number}`"))?,
        JsonLiteral::String(text) => text
            .parse::<i64>()
            .map_err(|_| anyhow::anyhow!("expected an integer epoch value, got `{text}`"))?,
        JsonLiteral::Bool(value) => {
            anyhow::bail!("expected an integer epoch value, got boolean `{value}`")
        }
    };
    let nanos: i128 = match unit {
        EpochUnit::Seconds => timestamp as i128 * 1_000_000_000,
        EpochUnit::Millis => timestamp as i128 * 1_000_000,
    };
    let date_time = OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .map_err(|err| anyhow::anyhow!("invalid epoch value `{timestamp}`: {err}"))?;
    let rfc3339 = date_time.format(&Rfc3339)?;
    Ok(JsonLiteral::String(rfc3339))
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use super::{RangeQuery as ElasticRangeQuery, RangeQueryParams as ElasticRangeQueryParams};
    use crate::JsonLiteral;
    use crate::elastic_query_dsl::ConvertibleToQueryAst;
    use crate::query_ast::{QueryAst, RangeQuery};

    #[test]
    fn test_date_range_query_with_format() {
        let range_query_params = ElasticRangeQueryParams {
            gt: Some(JsonLiteral::String("2021-01-03T13:32:43".to_string())),
            gte: None,
            lt: None,
            lte: None,
            boost: None,
            format: JsonLiteral::String("yyyy-MM-dd['T'HH:mm:ss]".to_string()).into(),
        };
        let range_query: ElasticRangeQuery = ElasticRangeQuery {
            field: "date".to_string(),
            value: range_query_params,
        };
        let range_query_ast = range_query.convert_to_query_ast().unwrap();
        assert!(matches!(
            range_query_ast,
            QueryAst::Range(RangeQuery {
                field,
                lower_bound: Bound::Excluded(lower_bound),
                upper_bound: Bound::Unbounded,
            })
            if field == "date" && lower_bound == JsonLiteral::String("2021-01-03T13:32:43Z".to_string())
        ));
    }

    #[test]
    fn test_date_range_query_with_strict_date_optional_time_format() {
        let range_query_params = ElasticRangeQueryParams {
            gt: None,
            gte: None,
            lt: None,
            lte: Some(JsonLiteral::String("2024-09-28T10:22:55.797Z".to_string())),
            boost: None,
            format: JsonLiteral::String("strict_date_optional_time".to_string()).into(),
        };
        let range_query: ElasticRangeQuery = ElasticRangeQuery {
            field: "timestamp".to_string(),
            value: range_query_params,
        };
        let range_query_ast = range_query.convert_to_query_ast().unwrap();
        assert!(matches!(
            range_query_ast,
            QueryAst::Range(RangeQuery {
                field,
                lower_bound: Bound::Unbounded,
                upper_bound: Bound::Included(upper_bound),
            })
            if field == "timestamp" && upper_bound == JsonLiteral::String("2024-09-28T10:22:55.797Z".to_string())
        ));
    }

    #[test]
    fn test_date_range_query_with_epoch_millis_format() {
        let range_query_params = ElasticRangeQueryParams {
            gt: None,
            gte: Some(JsonLiteral::Number(1779137992972i64.into())),
            lt: Some(JsonLiteral::Number(1780495357572i64.into())),
            lte: None,
            boost: None,
            format: JsonLiteral::String("epoch_millis".to_string()).into(),
        };
        let range_query: ElasticRangeQuery = ElasticRangeQuery {
            field: "@timestamp".to_string(),
            value: range_query_params,
        };
        let range_query_ast = range_query.convert_to_query_ast().unwrap();
        assert!(matches!(
            range_query_ast,
            QueryAst::Range(RangeQuery {
                field,
                lower_bound: Bound::Included(lower_bound),
                upper_bound: Bound::Excluded(upper_bound),
            })
            if field == "@timestamp"
                && lower_bound == JsonLiteral::String("2026-05-18T20:59:52.972Z".to_string())
                && upper_bound == JsonLiteral::String("2026-06-03T14:02:37.572Z".to_string())
        ));
    }

    #[test]
    fn test_date_range_query_with_epoch_second_format() {
        let range_query_params = ElasticRangeQueryParams {
            gt: None,
            gte: Some(JsonLiteral::Number(1779137992i64.into())),
            lt: None,
            lte: None,
            boost: None,
            format: JsonLiteral::String("epoch_second".to_string()).into(),
        };
        let range_query: ElasticRangeQuery = ElasticRangeQuery {
            field: "@timestamp".to_string(),
            value: range_query_params,
        };
        let range_query_ast = range_query.convert_to_query_ast().unwrap();
        assert!(matches!(
            range_query_ast,
            QueryAst::Range(RangeQuery {
                field,
                lower_bound: Bound::Included(lower_bound),
                upper_bound: Bound::Unbounded,
            })
            if field == "@timestamp"
                && lower_bound == JsonLiteral::String("2026-05-18T20:59:52Z".to_string())
        ));
    }
}
