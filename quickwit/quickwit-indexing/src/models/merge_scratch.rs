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

use quickwit_common::temp_dir::TempDirectory;
use tantivy::Directory;

use crate::merge_policy::{MergeOperation, MergeTask};

#[derive(Debug)]
pub struct MergeScratch {
    /// The merge operation data (splits, merge_split_id, operation type).
    pub merge_operation: MergeOperation,
    // TODO: remove once the old MergePipeline is deleted and the
    // DeleteTaskPipeline no longer routes through MergeSchedulerService.
    pub merge_task: Option<MergeTask>,
    pub merge_scratch_directory: TempDirectory,
    pub downloaded_splits_directory: TempDirectory,
    pub tantivy_dirs: Vec<Box<dyn Directory>>,
}
