import dlt
import fire
import os
from loguru import logger
from socrata import nyc_open_data_source
from dlt.common.configuration.inject import with_config
from dlt.common.pipeline import LoadInfo
from notifications import safe_send_telegram_text

repo_root = os.path.abspath(os.path.join(__file__, "..", ".."))
data_root = os.path.join(repo_root, "assets", "data")
schemas_root = os.path.join(repo_root, "assets", "schemas")

logger.debug({"repo_root": repo_root, "data_root": data_root})

# pipeline_name = os.path.basename(__file__).replace('.py', '')
# print(f'pipeline_name {pipeline_name}')

# https://duckdb.org/docs/sql/meta/information_schema.html

# In DuckDB, a catalog is at the top level of the organizational structure. It's
# a container for schemas and other database objects. I think the `system` and
# `temp` catalogs are reserved by DuckDB. The default catalog is `main`.
# duckdb_catalog = 'main'
# I think it make sense to use the same name for:
# 1. DuckDB file
# 2. DuckDB catalog
# 3. dlt pipeline

# A dlt dataset refers to a DuckDB schema. Here we are ingesting data in DuckDB
# from a 3rd party API, so I think the following names would be appropriate for
# a DuckDB schema: `landing_zone` (see the "bronze layer" in the Medallion
# Architecture), `raw_data`, `external_data`, `api_data`, `api_source`.
# Note that dlt has also a concept of schema:
# - dlt dataset and dlt schema are different things.
# - A dlt schema can have a different name from a DuckDB schema.
duckdb_schema = "landing_zone"

# A single dlt pipeline will create, in the specified dlt dataset (i.e. DuckDB
# schema) a table for each REST API resource.
# https://dlthub.com/devel/general-usage/destination-tables


@with_config(sections=("telegram"))
def get_telegram_config(config=dlt.config.value):
    return {"parse_mode": config["parse_mode"]}


@with_config(sections=("telegram"))
def get_telegram_credentials(credentials=dlt.secrets.value):
    return {"bot_token": credentials["bot_token"], "chat_id": credentials["chat_id"]}


def run_pipeline_nyc_open_data() -> None:
    pipeline_name = "nyc_open_data"
    # Data will be stored at:
    # <dlt-pipeline_name>.<dlt-dataset_name>.<dlt-resource>
    # Which corresponds to:
    # <DuckDB-catalog>.<DuckDB-schema>.<DuckDB-table>

    telegram_config = get_telegram_config()
    telegram_credentials = get_telegram_credentials()
    parse_mode = telegram_config["parse_mode"]
    bot_token = telegram_credentials["bot_token"]
    chat_id = telegram_credentials["chat_id"]

    pipeline = dlt.pipeline(
        dataset_name=duckdb_schema,
        destination=dlt.destinations.duckdb(
            os.path.join(data_root, f"{pipeline_name}.duckdb")
        ),
        # dev_mode=True,
        export_schema_path=os.path.join(schemas_root, "export"),
        pipeline_name=pipeline_name,
        progress="log",
    )

    load_info: LoadInfo = pipeline.run(nyc_open_data_source())
    # print(load_info)
    # print(load_info.asdict())

    prc = pipeline.runtime_config
    print(f'pipeline {pipeline_name} log_level: {prc.get("log_level")}')

    # print(f"pipeline {pipeline_name} runtime_config.items")
    # for x in pipeline.runtime_config.items():
    #     print(x)

    # print(f"pipeline {pipeline_name} config.keys")
    # for x in pipeline.config.keys():
    #     print(x)

    # send alert to Telegram chat. See how it's done for Slack.
    # https://dlthub.com/docs/running-in-production/alerting#slack
    # https://dlthub.com/docs/examples/chess_production/
    # https://dlthub.com/docs/walkthroughs/add_credentials#adding-credentials-to-your-deployment
    for package in load_info.load_packages:
        for table_name, table in package.schema_update.items():
            for column_name, column in table["columns"].items():
                header = "<b>Schema update!</b>"
                footer = f"<i>Sent by dlt pipeline <code>{pipeline_name}</code></i>"
                text = "".join(
                    [
                        header,
                        "\n\n",
                        f"Table: <code>{table_name}</code>",
                        "\n" f"Column: <code>{column_name}</code>",
                        "\n\n",
                        footer,
                    ]
                )

                safe_send_telegram_text(
                    bot_token=bot_token,
                    chat_id=chat_id,
                    parse_mode=parse_mode,
                    text=text,
                )

    # TODO: do I need this? Explain what this means and the pros & cons of using it.
    load_info.raise_on_failed_jobs()


def run():
    run_pipeline_nyc_open_data()


if __name__ == "__main__":
    fire.Fire(run)
