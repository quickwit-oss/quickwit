# quickwit-metastore

## Setup the environment.

### Install libpg-dev and diesel_cli

If you do not have libpg-dev and diesel_cli installed, please install them with the following command.

```
% sudo apt install libpq-dev
% cargo install diesel_cli --no-default-features --features postgres 
```

### Setup Diesel for quickwit-metastore

Let's set up the environment using diesel_cli.
First, run the following command to start PostgreSQL using Docker.

```
$ docker-compose up -d
```

After PostgreSQL has been successfully started, you can start the Diesel setup.
Execute the following command to create `migrations/postgresql` and `diesel_postgresql.toml`.

```
$ diesel setup --migration-dir=migrations/postgresql --config-file=diesel_postgresql.toml
```

In order to change the output destination of `schema.rs`, modify `diesel_postgresql.tom`l as follows.

```
[print_schema]
file = "src/postgresql/schema.rs"
```

Next, create a `*.sql` file under the `migrations/postgresql` directory.

```
$ diesel migration generate --migration-dir=migrations/postgresql create_indexes
$ diesel migration generate --migration-dir=migrations/postgresql create_splits
```

Since the generated file is empty, update the SQL statement.
After updating the SQL statement, generate `quickwit-metastore/src/postgresql/schema.rs` with the following command

```
$ diesel migration run --migration-dir=migrations/postgresql --config-file=diesel_postgresql.toml
```

Once you have successfully generated the `quickwit-metastore/src/postgresql/schema.rs`, you can start developing with Diesel.


## Testing quickwit-metastore

To test FileBackedMetastore only, use the following command.

```
$ cargo test
```

To test including PostgresqlMetastore, you need to start PostgreSQL beforehand.
Start PostgreSQL for testing with the following command in `quickwit` project root.

```
$ make docker-compose-up DOCKER_SERVICES=postgres
```

Once PostgreSQL is up and running, you can run tests including PostgresqlMetastore with the following command.

```
$ cargo test --features=postgres
```

You can stop PostgreSQL with the following command.

```
$ docker-compose down
```
