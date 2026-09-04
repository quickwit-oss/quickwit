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

use std::collections::BTreeMap;

use quickwit_config::TransformConfig;
use tracing::warn;
use vrl::compiler::runtime::Runtime;
pub use vrl::compiler::runtime::Terminate as VrlTerminate;
use vrl::compiler::state::RuntimeState;
use vrl::compiler::{Program, TargetValueRef, TimeZone};
pub use vrl::value::{Secrets as VrlSecrets, Value as VrlValue};

use super::doc_processor::DocProcessorError;

pub(super) struct VrlDoc {
    pub vrl_value: VrlValue,
    pub num_bytes: usize,
}

impl VrlDoc {
    pub fn new(vrl_value: VrlValue, num_bytes: usize) -> Self {
        Self {
            vrl_value,
            num_bytes,
        }
    }
}

pub(super) struct VrlProgram {
    program: Program,
    timezone: TimeZone,
    runtime: Runtime,
    metadata: VrlValue,
    secrets: VrlSecrets,
    // When `true`, documents terminated by the VRL `abort` expression are dropped silently
    // (no warn log) and routed to a distinct `DocProcessorError::TransformAbort` variant so
    // they can be tracked separately from unexpected runtime errors. When `false`, aborts
    // are treated like any other VRL runtime error: warned and counted as transform errors.
    drop_on_abort: bool,
}

impl VrlProgram {
    pub fn transform_doc(&mut self, vrl_doc: VrlDoc) -> Result<VrlDoc, DocProcessorError> {
        let VrlDoc {
            mut vrl_value,
            num_bytes,
        } = vrl_doc;

        let mut target = TargetValueRef {
            value: &mut vrl_value,
            metadata: &mut self.metadata,
            secrets: &mut self.secrets,
        };
        let drop_on_abort = self.drop_on_abort;
        let runtime_res = self
            .runtime
            .resolve(&mut target, &self.program, &self.timezone)
            .map_err(|transform_error| match transform_error {
                VrlTerminate::Abort(_) if drop_on_abort => {
                    DocProcessorError::TransformAbort(transform_error)
                }
                _ => {
                    warn!(transform_error=?transform_error);
                    DocProcessorError::Transform(transform_error)
                }
            });

        if let VrlValue::Object(metadata) = target.metadata {
            metadata.clear();
        }
        self.runtime.clear();

        runtime_res.map(|vrl_value| VrlDoc::new(vrl_value, num_bytes))
    }

    pub fn try_from_transform_config(transform_config: TransformConfig) -> anyhow::Result<Self> {
        let drop_on_abort = transform_config.drop_on_abort();
        let (program, timezone) = transform_config.compile_vrl_script()?;
        let state = RuntimeState::default();
        let runtime = Runtime::new(state);

        Ok(VrlProgram {
            program,
            runtime,
            timezone,
            metadata: VrlValue::Object(BTreeMap::new()),
            secrets: VrlSecrets::default(),
            drop_on_abort,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Classifies a `transform_doc` result so tests can assert on category without needing
    /// `VrlDoc: Debug`.
    #[derive(Debug, PartialEq, Eq)]
    enum Outcome {
        Ok,
        TransformAbort,
        TransformError,
        DocMapperParsing,
        JsonParsing,
        OltpLogsParsing,
        OltpTracesParsing,
    }

    fn classify(result: &Result<VrlDoc, DocProcessorError>) -> Outcome {
        match result {
            Ok(_) => Outcome::Ok,
            Err(DocProcessorError::TransformAbort(_)) => Outcome::TransformAbort,
            Err(DocProcessorError::Transform(VrlTerminate::Abort(_))) => Outcome::TransformError,
            Err(DocProcessorError::Transform(VrlTerminate::Error(_))) => Outcome::TransformError,
            Err(DocProcessorError::DocMapperParsing(_)) => Outcome::DocMapperParsing,
            Err(DocProcessorError::JsonParsing(_)) => Outcome::JsonParsing,
            Err(DocProcessorError::OltpLogsParsing(_)) => Outcome::OltpLogsParsing,
            Err(DocProcessorError::OltpTracesParsing(_)) => Outcome::OltpTracesParsing,
        }
    }

    fn is_explicit_abort(result: &Result<VrlDoc, DocProcessorError>) -> bool {
        matches!(
            result,
            Err(DocProcessorError::TransformAbort(VrlTerminate::Abort(_)))
                | Err(DocProcessorError::Transform(VrlTerminate::Abort(_)))
        )
    }

    fn vrl_doc(json: serde_json::Value) -> VrlDoc {
        let vrl_value = serde_json::from_value::<VrlValue>(json).expect("valid VRL value");
        let num_bytes = 0;
        VrlDoc::new(vrl_value, num_bytes)
    }

    #[test]
    fn test_successful_transform_passes_through() {
        let transform_config = TransformConfig::for_test(".body = upcase(string!(.body))");
        let mut vrl_program = VrlProgram::try_from_transform_config(transform_config).unwrap();
        let result = vrl_program
            .transform_doc(vrl_doc(serde_json::json!({"body": "hello"})))
            .expect("transform should succeed");
        let json = serde_json::to_value(result.vrl_value).unwrap();
        assert_eq!(json, serde_json::json!({"body": "HELLO"}));
    }

    #[test]
    fn test_abort_with_drop_on_abort_true_routes_to_transform_abort() {
        let transform_config = TransformConfig::for_test("abort").with_drop_on_abort(true);
        let mut vrl_program = VrlProgram::try_from_transform_config(transform_config).unwrap();
        let result = vrl_program.transform_doc(vrl_doc(serde_json::json!({"body": "x"})));
        assert_eq!(classify(&result), Outcome::TransformAbort);
        assert!(is_explicit_abort(&result));
    }

    #[test]
    fn test_abort_with_drop_on_abort_false_routes_to_transform_error() {
        let transform_config = TransformConfig::for_test("abort").with_drop_on_abort(false);
        let mut vrl_program = VrlProgram::try_from_transform_config(transform_config).unwrap();
        let result = vrl_program.transform_doc(vrl_doc(serde_json::json!({"body": "x"})));
        assert_eq!(classify(&result), Outcome::TransformError);
        assert!(is_explicit_abort(&result));
    }

    #[test]
    fn test_runtime_error_routes_to_transform_error_regardless_of_drop_on_abort() {
        // `parse_json!` on a non-string raises a runtime error (Terminate::Error),
        // which is distinct from an explicit `abort`.
        let script = "parse_json!(.body)";
        for drop_on_abort in [true, false] {
            let transform_config =
                TransformConfig::for_test(script).with_drop_on_abort(drop_on_abort);
            let mut vrl_program = VrlProgram::try_from_transform_config(transform_config).unwrap();
            let result = vrl_program.transform_doc(vrl_doc(serde_json::json!({"body": 42})));
            assert_eq!(
                classify(&result),
                Outcome::TransformError,
                "runtime error must classify as TransformError (drop_on_abort={drop_on_abort})"
            );
            assert!(
                !is_explicit_abort(&result),
                "runtime error must NOT be an Abort (drop_on_abort={drop_on_abort})"
            );
        }
    }

    #[test]
    fn test_drop_on_abort_does_not_affect_successful_transforms() {
        for drop_on_abort in [true, false] {
            let transform_config = TransformConfig::for_test(".body = upcase(string!(.body))")
                .with_drop_on_abort(drop_on_abort);
            let mut vrl_program = VrlProgram::try_from_transform_config(transform_config).unwrap();
            let result = vrl_program
                .transform_doc(vrl_doc(serde_json::json!({"body": "hello"})))
                .expect("transform should succeed regardless of drop_on_abort");
            let json = serde_json::to_value(result.vrl_value).unwrap();
            assert_eq!(json, serde_json::json!({"body": "HELLO"}));
        }
    }

    #[test]
    fn test_conditional_abort_only_drops_matching_docs() {
        // Validates the intended use case: filter-style scripts that abort on a predicate.
        let script = r#"
            if .drop == true {
                abort
            }
        "#;
        let transform_config = TransformConfig::for_test(script).with_drop_on_abort(true);
        let mut vrl_program = VrlProgram::try_from_transform_config(transform_config).unwrap();

        // Non-aborting doc passes through.
        let kept = vrl_program
            .transform_doc(vrl_doc(serde_json::json!({"body": "keep", "drop": false})))
            .expect("non-aborting doc should pass through");
        let kept_json = serde_json::to_value(kept.vrl_value).unwrap();
        assert_eq!(
            kept_json,
            serde_json::json!({"body": "keep", "drop": false})
        );

        // Aborting doc returns TransformAbort.
        let dropped = vrl_program.transform_doc(vrl_doc(serde_json::json!({"drop": true})));
        assert_eq!(classify(&dropped), Outcome::TransformAbort);
        assert!(is_explicit_abort(&dropped));
    }

    #[test]
    fn test_abort_with_message_routes_same_as_bare_abort() {
        // VRL allows `abort "reason"` — verify message-bearing aborts route identically
        // to bare aborts (same Terminate::Abort variant, same routing).
        for (script, drop_on_abort, expected) in [
            (
                r#"abort "filtered as debug""#,
                true,
                Outcome::TransformAbort,
            ),
            (
                r#"abort "filtered as debug""#,
                false,
                Outcome::TransformError,
            ),
        ] {
            let transform_config =
                TransformConfig::for_test(script).with_drop_on_abort(drop_on_abort);
            let mut vrl_program = VrlProgram::try_from_transform_config(transform_config).unwrap();
            let result = vrl_program.transform_doc(vrl_doc(serde_json::json!({"body": "x"})));
            assert_eq!(
                classify(&result),
                expected,
                "abort-with-message must route same as bare abort (drop_on_abort={drop_on_abort})"
            );
            assert!(is_explicit_abort(&result));
        }
    }

    #[test]
    fn test_transform_abort_error_display_includes_reason() {
        // Display impl is used in logs and `?` propagation; the abort reason must surface.
        let transform_config =
            TransformConfig::for_test(r#"abort "explicit reason""#).with_drop_on_abort(true);
        let mut vrl_program = VrlProgram::try_from_transform_config(transform_config).unwrap();
        let result = vrl_program.transform_doc(vrl_doc(serde_json::json!({"body": "x"})));
        let err = match result {
            Ok(_) => panic!("expected an abort error"),
            Err(err) => err,
        };
        let rendered = format!("{err}");
        assert!(
            rendered.contains("aborted")
                || rendered.contains("abort")
                || rendered.contains("explicit reason"),
            "Display should describe an abort; got: {rendered}"
        );
    }

    #[test]
    fn test_num_bytes_is_preserved_through_transform() {
        let transform_config = TransformConfig::for_test(".body = upcase(string!(.body))");
        let mut vrl_program = VrlProgram::try_from_transform_config(transform_config).unwrap();
        let vrl_value =
            serde_json::from_value::<VrlValue>(serde_json::json!({"body": "h"})).unwrap();
        let input = VrlDoc::new(vrl_value, 1234);
        let output = vrl_program.transform_doc(input).unwrap();
        assert_eq!(output.num_bytes, 1234);
    }
}
