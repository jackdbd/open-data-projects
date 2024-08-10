# NYC 311

This project can be managed with a few [Babashka tasks](https://book.babashka.org/#tasks).

## Ingestion

Data ingestion performed by [dlt](https://github.com/dlt-hub/dlt).

Define `created_date_start` and `created_date_stop` in the `[sources.socrata]` section of `config.toml`, then run the dlt pipeline using this Babashka task:

```sh
bb run ingest
```

## Transformation

```sh
cd transformation
```

```sh
dbt debug
```

```sh
dbt run
```

```sh
dbt test
```

```sh
dbt docs generate && dbt docs serve
```
