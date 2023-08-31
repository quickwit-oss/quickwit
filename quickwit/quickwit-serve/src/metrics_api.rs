#[derive(utoipa::OpenApi)]
#[openapi(paths(metrics_handler))]
/// Endpoints which are weirdly tied to another crate with no
/// other bits of information attached.
///
/// If a crate plans to encompass different schemas, handlers, etc...
/// Then it should have it's own specific API group.
pub struct MetricsApi;

#[utoipa::path(
    get,
    tag = "Get Metrics",
    path = "/",
    responses(
        (status = 200, description = "Successfully fetched metrics.", body = String),
    ),
)]
/// Get Node Metrics
///
/// These are in the form of prometheus metrics.
pub fn metrics_handler() -> impl warp::Reply {
    quickwit_common::metrics::metrics_text_payload()
}
