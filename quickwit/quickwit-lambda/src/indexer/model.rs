// Copyright (C) 2023 Quickwit, Inc.
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

use std::path::PathBuf;

use aws_lambda_events::event::s3::S3Event;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
/// Event types that can be used to invoke the indexer Lambda.
pub enum IndexerEvent {
    Custom { source_uri: String },
    S3(S3Event),
}

impl IndexerEvent {
    pub fn uri(&self) -> PathBuf {
        match &self {
            IndexerEvent::Custom { source_uri } => PathBuf::from(source_uri),
            IndexerEvent::S3(event) => [
                "s3://",
                event.records[0].s3.bucket.name.as_ref().unwrap(),
                event.records[0].s3.object.key.as_ref().unwrap(),
            ]
            .iter()
            .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_custom_event_uri() {
        let cust_event = json!({
            "source_uri": "s3://quickwit-test/test.json"
        });
        let parsed_cust_event: IndexerEvent = serde_json::from_value(cust_event).unwrap();
        assert_eq!(
            parsed_cust_event.uri(),
            PathBuf::from("s3://quickwit-test/test.json"),
        );
    }

    #[test]
    fn test_s3_event_uri() {
        let cust_event = json!({
          "Records": [
            {
              "eventVersion": "2.0",
              "eventSource": "aws:s3",
              "awsRegion": "us-east-1",
              "eventTime": "1970-01-01T00:00:00.000Z",
              "eventName": "ObjectCreated:Put",
              "userIdentity": {
                "principalId": "EXAMPLE"
              },
              "requestParameters": {
                "sourceIPAddress": "127.0.0.1"
              },
              "responseElements": {
                "x-amz-request-id": "EXAMPLE123456789",
                "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
              },
              "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "testConfigRule",
                "bucket": {
                  "name": "quickwit-test",
                  "ownerIdentity": {
                    "principalId": "EXAMPLE"
                  },
                  "arn": "arn:aws:s3:::quickwit-test"
                },
                "object": {
                  "key": "test.json",
                  "size": 1024,
                  "eTag": "0123456789abcdef0123456789abcdef",
                  "sequencer": "0A1B2C3D4E5F678901"
                }
              }
            }
          ]
        });
        let parsed_cust_event: IndexerEvent = serde_json::from_value(cust_event).unwrap();
        assert_eq!(
            parsed_cust_event.uri(),
            PathBuf::from("s3://quickwit-test/test.json"),
        );
    }
}
