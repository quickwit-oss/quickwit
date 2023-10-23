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
        let runtime_res = self
            .runtime
            .resolve(&mut target, &self.program, &self.timezone)
            .map_err(|transform_error| {
                warn!(transform_error=?transform_error);
                DocProcessorError::Transform(transform_error)
            });

        if let VrlValue::Object(metadata) = target.metadata {
            metadata.clear();
        }
        self.runtime.clear();

        runtime_res.map(|vrl_value| VrlDoc::new(vrl_value, num_bytes))
    }

    pub fn try_from_transform_config(transform_config: TransformConfig) -> anyhow::Result<Self> {
        let (program, timezone) = transform_config.compile_vrl_script()?;
        let state = RuntimeState::default();
        let runtime = Runtime::new(state);

        Ok(VrlProgram {
            program,
            runtime,
            timezone,
            metadata: VrlValue::Object(BTreeMap::new()),
            secrets: VrlSecrets::default(),
        })
    }
}
