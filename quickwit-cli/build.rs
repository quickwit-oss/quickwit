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

use std::process::Command;

fn main() {
    let git_commit_hash = Command::new("git")
        .args(&["rev-parse", "HEAD"])
        .output()
        .map_or("unknown".to_string(), |output| {
            let hash = String::from_utf8(output.stdout).unwrap();
            String::from(&hash[..7])
        });
    println!("cargo:rustc-env=GIT_COMMIT_HASH={}", git_commit_hash);
}
