# Replay

Replay is a small util that sequentially replays a bunch of gRPC calls made to the
quickwit metastore, as fast as possible.

Right now, both the grpc address and the file are hardcoded.

To run it:

- run `cargo run --release --bin replay` from the `quickwit-metastore` directory.

It assumes a quickwit metastore service is running on `localhost:7280`

To get that, simply run:
`./quickwit run --service metastore`

A minimal `quickwit.yaml` to run against the postgres could be

```yaml
version: "0.4"
metastore_uri: postgres://quickwit-dev:quickwit-dev@localhost/quickwit-metastore-dev
```

To run postgres

`docker-compose up postgres` from the quickwit root directory.

# Warning

The replay file first request is creating the index.
That request actually includes an index_config json data, and this part is about to be heavily changed.

For the moment, I recommend experimenting on top of quickwit rev 2b0e3963f67303f4e6a362d53fa8bebd3cbad33e.

# Warning 2

The replay data does not delete the index and the splits.

It is required to run
`TRUNCATE TABLE indexes CASCADE;`
via
`psql -h localhost -U quickwit-dev quickwit-metastore-dev`

to rerun the replay data.
