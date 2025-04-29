use quickwit_remote_api::{make_client_tls_config, run_server};

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let target = "cloudprem.quickwit.dev:443";
    //let target = "127.0.0.1:7281";

    let tls_config = make_client_tls_config(
        "/Users/trinity.pointard/pomsky-pki/bridge-side/dev.crt",
        "/Users/trinity.pointard/pomsky-pki/bridge-side/dev.key",
        target,
    )?;

    run_server(target, None, Some(tls_config)).await
}
