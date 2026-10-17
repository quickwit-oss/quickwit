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
    runtime: Runtime,
    timezone: TimeZone,
    drop_on_abort: bool,
    metadata: VrlValue,
    secrets: VrlSecrets,
}

impl VrlProgram {
    pub fn transform_doc(&mut self, vrl_doc: VrlDoc) -> Result<Option<VrlDoc>, DocProcessorError> {
        let drop_on_abort = self.drop_on_abort;

        let VrlDoc {
            mut vrl_value,
            num_bytes,
        } = vrl_doc;

        let mut target = TargetValueRef {
            value: &mut vrl_value,
            metadata: &mut self.metadata,
            secrets: &mut self.secrets,
        };

        let runtime_result = self
            .runtime
            .resolve(&mut target, &self.program, &self.timezone);

        if let VrlValue::Object(metadata) = target.metadata {
            metadata.clear();
        }
        self.runtime.clear();

        match runtime_result {
            Err(VrlTerminate::Abort(_)) if drop_on_abort => Ok(None),
            runtime_result => runtime_result
                .map(|vrl_value| Some(VrlDoc::new(vrl_value, num_bytes)))
                .map_err(|transform_error| {
                    warn!(transform_error=?transform_error);
                    DocProcessorError::Transform(transform_error)
                }),
        }
    }

    pub fn try_from_transform_config(transform_config: TransformConfig) -> anyhow::Result<Self> {
        let (program, timezone) = transform_config.compile_vrl_script()?;
        let state = RuntimeState::default();
        let runtime = Runtime::new(state);

        Ok(VrlProgram {
            program,
            runtime,
            timezone,
            drop_on_abort: transform_config.drop_on_abort,
            metadata: VrlValue::Object(BTreeMap::new()),
            secrets: VrlSecrets::default(),
        })
    }
}
