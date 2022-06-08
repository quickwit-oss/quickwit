// Copyright (C) 2021 Quickwit, Inc.
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
//


static RUNTIMES: OnceCell<HashMap<RuntimeType, tokio::runtime::Runtime>> = OnceCell::new();

fn get_tokio_runtime_handle(runtime_type: RuntimeType) -> tokio::runtime::Handle {
    let runtime: &Runtime = RUNTIMES
        .get_or_init(|| {
            let mut runtimes = HashMap::default();
            // TODO make configurable
            let blocking_runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            runtimes.insert(RuntimeType::Blocking, blocking_runtime);
            let non_blocking_runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            runtimes.insert(RuntimeType::NonBlocking, non_blocking_runtime);
            runtimes
        })
        .get(&runtime_type)
        .unwrap();
    runtime.handle().clone()
}
