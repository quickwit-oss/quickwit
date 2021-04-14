use clap::App;

fn main() {
    let app = App::new("Quickwit CLI")
        .about("Index and search datasets from your terminal.")
        .author("Quickwit, Inc. <hello@quickwit.io>")
        .version(env!("CARGO_PKG_VERSION"));

    let _ = app.get_matches();
}
