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

use once_cell::sync::Lazy;
use quickwit_telemetry::payload::TelemetryEvent;
use regex::Regex;
use rust_embed::RustEmbed;
use warp::hyper::header::HeaderValue;
use warp::path::Tail;
use warp::reply::Response;
use warp::{Filter, Rejection};

use crate::rest::recover_fn;

/// Regular expression to identify which path should serve an asset file.
/// If not matched, the server serves the `index.html` file.
const PATH_PATTERN: &str = r"(^static|\.(png|json|txt|ico|js|map|css|woff2|ttf)$)";

const UI_INDEX_FILE_NAME: &str = "index.html";

#[derive(RustEmbed)]
#[folder = "../quickwit-ui/build/"]
struct Asset;

pub fn ui_handler() -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path("ui")
        .and(warp::path::tail())
        .and_then(serve_file)
        .recover(recover_fn)
        .boxed()
}

async fn serve_file(path: Tail) -> Result<impl warp::Reply, Rejection> {
    serve_impl(path.as_str()).await
}

async fn serve_impl(path: &str) -> Result<impl warp::Reply + use<>, Rejection> {
    static PATH_PTN: Lazy<Regex> = Lazy::new(|| Regex::new(PATH_PATTERN).unwrap());
    let path_to_file = if PATH_PTN.is_match(path) {
        path
    } else {
        // Quickwit UI is a single page application.
        // Any path request that is not an asset should serve the `index.html` file.
        // The client (browser) usually request `index.html` once unless the user refreshes the
        // page.
        quickwit_telemetry::send_telemetry_event(TelemetryEvent::UiIndexPageLoad).await;
        UI_INDEX_FILE_NAME
    };
    let asset = Asset::get(path_to_file).ok_or_else(warp::reject::not_found)?;
    let mime = mime_guess::from_path(path_to_file).first_or_octet_stream();

    let mut res = Response::new(asset.data.into_owned().into());
    res.headers_mut().insert(
        "content-type",
        HeaderValue::from_str(mime.as_ref()).unwrap(),
    );
    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_regex() {
        let path_ptn = Regex::new(PATH_PATTERN).unwrap();

        assert!(path_ptn.is_match("manifest.json"));
        assert!(path_ptn.is_match("favicon.ico"));
        assert!(path_ptn.is_match("static/js/main.df380554.js.map"));
        assert!(path_ptn.is_match("android-chrome-192x192.png"));
        assert!(!path_ptn.is_match("search"));
        assert!(!path_ptn.is_match(""));
    }
}
