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

use std::alloc::{Layout, alloc_zeroed, dealloc, realloc};
use std::hint::black_box;

use quickwit_common::{CountingAllocator, allocated_bytes};

#[cfg(not(feature = "jemalloc-profiled"))]
#[global_allocator]
static GLOBAL: CountingAllocator<std::alloc::System> = CountingAllocator(std::alloc::System);

#[cfg(feature = "jemalloc-profiled")]
#[global_allocator]
static GLOBAL: CountingAllocator<quickwit_common::jemalloc_profiled::JemallocProfiled> =
    CountingAllocator(quickwit_common::jemalloc_profiled::JemallocProfiled(
        tikv_jemallocator::Jemalloc,
    ));

fn main() {
    let initial = allocated_bytes();
    unsafe {
        let layout = Layout::from_size_align(1024 * 1024, 64).unwrap();
        let ptr = alloc_zeroed(layout);
        assert!(!ptr.is_null());
        assert_eq!(allocated_bytes(), initial + layout.size());
        assert!(
            std::slice::from_raw_parts(ptr, layout.size())
                .iter()
                .all(|byte| *byte == 0)
        );

        let grown = realloc(ptr, layout, 2 * layout.size());
        assert!(!grown.is_null());
        assert_eq!(allocated_bytes(), initial + 2 * layout.size());
        let shrunk = realloc(
            grown,
            Layout::from_size_align(2 * layout.size(), 64).unwrap(),
            32,
        );
        assert!(!shrunk.is_null());
        assert_eq!(allocated_bytes(), initial + 32);
        dealloc(shrunk, Layout::from_size_align(32, 64).unwrap());
    }
    assert_eq!(allocated_bytes(), initial);

    let data = black_box(vec![42u8; 1024 * 1024]);
    assert_eq!(allocated_bytes(), initial + data.capacity());
    drop(data);
    assert_eq!(allocated_bytes(), initial);

    // Exercise TLS initialization/destruction with the wrapper actually installed globally.
    let data = std::thread::spawn(|| black_box(vec![42u8; 1024 * 1024]))
        .join()
        .unwrap();
    let before_free = allocated_bytes();
    let capacity = data.capacity();
    drop(data);
    assert_eq!(allocated_bytes(), before_free - capacity);
}
