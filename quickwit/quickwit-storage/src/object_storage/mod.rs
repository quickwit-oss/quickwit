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

mod error;

mod s3_compatible_storage;
pub use self::s3_compatible_storage::S3CompatibleObjectStorage;
pub use self::s3_compatible_storage_resolver::S3CompatibleObjectStorageFactory;

mod policy;
pub use crate::object_storage::policy::MultiPartPolicy;

mod s3_compatible_storage_resolver;

#[cfg(feature = "azure")]
mod azure_blob_storage;
#[cfg(feature = "azure")]
pub use self::azure_blob_storage::{AzureBlobStorage, AzureBlobStorageFactory};
