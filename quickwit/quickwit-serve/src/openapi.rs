use utoipa::OpenApi;


#[derive(OpenApi)]
#[openapi(
    components(
        schemas(
            quickwit_cluster::ClusterSnapshot,
        )
    )
)]
pub struct ApiDoc;