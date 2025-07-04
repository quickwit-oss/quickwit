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

use axum::Router;
use axum::extract::Path;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use once_cell::sync::Lazy;
use quickwit_telemetry::payload::TelemetryEvent;
use regex::Regex;
use rust_embed::RustEmbed;

/// Regular expression to identify which path should serve an asset file.
/// If not matched, the server serves the `index.html` file.
const PATH_PATTERN: &str = r"(^static|\.(png|json|txt|ico|js|map)$)";

const UI_INDEX_FILE_NAME: &str = "index.html";

#[derive(RustEmbed)]
#[folder = "../quickwit-ui/build/"]
struct Asset;

/// Axum routes for the UI handler
pub fn ui_routes() -> Router {
    Router::new()
        .route("/ui/*path", get(serve_file_axum))
        .route("/ui", get(serve_ui_root))
        // Root redirect to UI
        .route("/", get(redirect_root_to_ui))
}

/// Axum handler for serving UI files
async fn serve_file_axum(Path(path): Path<String>) -> impl IntoResponse {
    serve_impl_axum(&path).await
}

/// Axum handler for serving UI root (when no path is provided)
async fn serve_ui_root() -> impl IntoResponse {
    serve_impl_axum("").await
}

/// Axum handler for redirecting root to UI
async fn redirect_root_to_ui() -> impl IntoResponse {
    axum::response::Redirect::permanent("/ui/search")
}

/// Axum implementation of serve_impl
async fn serve_impl_axum(path: &str) -> Response {
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

    match Asset::get(path_to_file) {
        Some(asset) => {
            let mime = mime_guess::from_path(path_to_file).first_or_octet_stream();
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, mime.as_ref())],
                asset.data.into_owned(),
            )
                .into_response()
        }
        None => (StatusCode::NOT_FOUND, "File not found").into_response(),
    }
}

#[cfg(test)]
mod tests {
    use axum_test::TestServer;

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

    #[tokio::test]
    async fn test_ui_routes_axum() {
        let app = ui_routes();
        let server = TestServer::new(app).unwrap();

        // Test root redirect
        let response = server.get("/").await;
        assert_eq!(response.status_code(), 308); // Permanent redirect

        // Test UI root returns 404 when no assets are embedded (in test environment)
        let response = server.get("/ui").await;
        assert_eq!(response.status_code(), 404);

        // Test UI path returns 404 when no assets are embedded (in test environment)
        let response = server.get("/ui/search").await;
        assert_eq!(response.status_code(), 404);

        // Test 404 for non-existent assets
        let response = server.get("/ui/nonexistent.js").await;
        assert_eq!(response.status_code(), 404);
    }
}
