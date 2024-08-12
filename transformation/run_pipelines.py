import dlt
import os
import sys

# Add the parent directory to the system path so that I can import Python code from sibling directories.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common import (
    get_telegram_config,
    get_telegram_credentials,
    APP_NAME,
    DB_FILE_PATH,
    SCHEMAS_ROOT,
    DBT_PACKAGE_PATH,
)
from telegram import (
    dbt_model_materialized_text,
    generic_exception_text,
    runtime_configuration_text,
    safe_send_telegram_text,
)

duckdb_catalog = "nyc_open_data"
duckdb_schema = "silver_layer"
pipeline_name = "nyc_open_data_transformation"


def run() -> None:
    telegram_config = get_telegram_config()
    telegram_credentials = get_telegram_credentials()
    parse_mode = telegram_config["parse_mode"]
    bot_token = telegram_credentials["bot_token"]
    chat_id = telegram_credentials["chat_id"]

    # TODO: I want the DuckDB schema to be `silver_layer`, not `silver_layer_silver_layer`
    # This pipeline creates the following structure in DuckDB:
    # nyc_open_data.silver_layer_silver_layer.<table-name>
    pipeline = dlt.pipeline(
        dataset_name=duckdb_schema,
        destination=dlt.destinations.duckdb(DB_FILE_PATH),
        export_schema_path=os.path.join(SCHEMAS_ROOT, "export"),
        pipeline_name=pipeline_name,
        progress="log",
        # https://dlthub.com/docs/dlt-ecosystem/staging
        # staging="?"
    )

    safe_send_telegram_text(
        bot_token=bot_token,
        chat_id=chat_id,
        parse_mode=parse_mode,
        text=runtime_configuration_text(pipeline=pipeline, app_name=APP_NAME),
    )

    venv = dlt.dbt.get_venv(pipeline)

    dbt = dlt.dbt.package(pipeline, DBT_PACKAGE_PATH, venv=venv)

    models = []
    try:
        # It seems the DuckDB schema created is <dbt-destination_dataset_name>_<dlt-pipeline-dataset_name>
        models = dbt.run_all(destination_dataset_name=duckdb_schema)
    except Exception as ex:
        safe_send_telegram_text(
            bot_token=bot_token,
            chat_id=chat_id,
            parse_mode=parse_mode,
            text=generic_exception_text(app_name=APP_NAME, exception=ex),
        )

    for model in models:
        safe_send_telegram_text(
            bot_token=bot_token,
            chat_id=chat_id,
            parse_mode=parse_mode,
            text=dbt_model_materialized_text(app_name=APP_NAME, dbt=dbt, model=model),
        )


if __name__ == "__main__":
    run()
