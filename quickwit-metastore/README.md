# quickwit-metastore

## Starting postgres

The following command starts a postgresql server
locally to test the postgres metastore implementation.

`docker-compose up postgres`

It's data is saved in the tmp directory, and
is not necessarily cleaned up between two runs.

You can execute `make rm-postgres` to remove the
data of this postgresql database.

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

## Sqlx-cli and migrations

This sqlx-cli can be useful (but is not necessary) to work with migrations.

```
cargo install sqlx-cli
```

You can then use the following commands to apply/revert your postgresql migrations.
```
sqlx migrate run  --database-url postgres://quickwit-dev:quickwit-dev@localhost:5432/quickwit-metastore-dev --source migrations/postgresql
sqlx migrate revert  --database-url postgres://quickwit-dev:quickwit-dev@localhost:5432/quickwit-metastore-dev --source migrations/postgresql
```
