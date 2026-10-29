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

use quickwit_authorize::{Authorization, RequestFamily};

use crate::{FetchRequest, IngestRequest, TailRequest};

impl Authorization for IngestRequest {
    fn request_family(&self) -> RequestFamily {
        RequestFamily::IndexWrite
    }
}

impl Authorization for FetchRequest {
    fn request_family(&self) -> RequestFamily {
        RequestFamily::IndexRead
    }
}

impl Authorization for TailRequest {
    fn request_family(&self) -> RequestFamily {
        RequestFamily::IndexRead
    }
}
