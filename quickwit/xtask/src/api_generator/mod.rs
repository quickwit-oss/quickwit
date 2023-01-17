// Copyright (C) 2022 Quickwit, Inc.
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

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};

use crate::{ES_ARTIFACTS_DIR_NAME, ES_STACK_VERSION, GENERATED_FILE_NAME, ROOT_DIR};

pub fn generate_api() -> anyhow::Result<()> {
    let download_dir = ROOT_DIR
        .join(ES_ARTIFACTS_DIR_NAME)
        .join(&*ES_STACK_VERSION)
        .join("rest-api-spec");

    let target_file = ROOT_DIR
        .join("quickwit-serve")
        .join("src")
        .join("elastic_search_api")
        .join(GENERATED_FILE_NAME);

    if target_file.exists() {
        let _ = fs::remove_file(&target_file).expect(&format!(
            "Error removing existing target file: {:?}.",
            target_file
        ));
    }

    // TODO
    println!("Generating at: {:?}", target_file);
    Ok(())
}
