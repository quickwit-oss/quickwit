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

use std::str::FromStr;

use aws_lambda_events::event::s3::S3Event;
use quickwit_common::uri::Uri;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
/// Event types that can be used to invoke the indexer Lambda.
pub enum IndexerEvent {
    Custom { source_uri: String },
    S3(S3Event),
}

impl IndexerEvent {
    pub fn uri(&self) -> anyhow::Result<Uri> {
        let path: String = match self {
            IndexerEvent::Custom { source_uri } => source_uri.clone(),
            IndexerEvent::S3(event) => [
                "s3://",
                event.records[0].s3.bucket.name.as_ref().unwrap(),
                "/",
                event.records[0].s3.object.key.as_ref().unwrap(),
            ]
            .join(""),
        };
        Uri::from_str(&path)
    }
}

/*
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
            parsed_cust_event.uri().unwrap(),
            Uri::from_str("s3://quickwit-test/test.json").unwrap(),
        );
    }

    #[test]
    fn test_s3_event_uri() {
        let s3_event = json!({
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
        let s3_event: IndexerEvent = serde_json::from_value(s3_event).unwrap();
        assert_eq!(
            s3_event.uri().unwrap(),
            Uri::from_str("s3://quickwit-test/test.json").unwrap(),
        );
    }
}
*/
