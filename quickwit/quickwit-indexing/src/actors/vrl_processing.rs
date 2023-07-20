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

pub struct VrlProgram {
    runtime: Runtime,
    program: Program,
    timezone: TimeZone,
}

impl VrlProgram {
    pub fn transform_doc(&mut self, mut vrl_doc: VrlValue) -> Result<VrlValue, DocProcessorError> {
        let mut metadata = VrlValue::Object(BTreeMap::new());
        let mut secrets = VrlSecrets::new();
        let mut target = TargetValueRef {
            value: &mut vrl_doc,
            metadata: &mut metadata,
            secrets: &mut secrets,
        };
        let runtime_res = self
            .runtime
            .resolve(&mut target, &self.program, &self.timezone)
            .map_err(|transform_error| {
                warn!(transform_error=?transform_error);
                DocProcessorError::TransformError(transform_error)
            });

        self.runtime.clear();

        runtime_res
    }

    pub fn try_from_transform_config(transform_config: TransformConfig) -> anyhow::Result<Self> {
        let (program, timezone) = transform_config.compile_vrl_script()?;
        let state = RuntimeState::default();
        let runtime = Runtime::new(state);

        Ok(VrlProgram {
            program,
            runtime,
            timezone,
        })
    }
}
