# NYC Open Data to DuckDB

## Useful commands

This project can be managed with a few [Babashka tasks](https://book.babashka.org/#tasks) and [Devenv scripts](https://devenv.sh/scripts/).

Fetch data from NYC Open Data and ingest it into DuckDB:

```sh
ingestion
```

Run dbt run in a dlt pipeline:

```sh
transformation
```

Build dbt docs:

```sh
docs-build
```

Serve dbt docs:

```sh
docs-serve
```
