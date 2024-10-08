import os
import sys

import dlt

# Add the parent directory to the system path so that I can import Python code from sibling directories.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common import (
    APP_NAME,
    DB_FILE_PATH,
    DBT_PACKAGE_PATH,
    SCHEMAS_ROOT,
    get_telegram_config,
    get_telegram_credentials,
)
from telegram import (
    dbt_models_recap_text,
    dbt_tests_recap_text,
    generic_exception_text,
    runtime_configuration_text,
    safe_send_telegram_text,
)


def run() -> None:
    telegram_config = get_telegram_config()
    telegram_credentials = get_telegram_credentials()
    parse_mode = telegram_config["parse_mode"]
    bot_token = telegram_credentials["bot_token"]
    chat_id = telegram_credentials["chat_id"]

    # I want the DuckDB schema containing the data transformed by dbt to be
    # called `silver_layer`. It looks the only way to achieve this is to set
    # ONLY the destination_dataset_name parameter in the dbt.run_all() method,
    # and NOT set the dataset_name parameter in the dlt.pipeline() method.
    # Otherwise the DuckDB schema created will be:
    #  <destination_dataset_name>_<dataset_name>
    pipeline = dlt.pipeline(
        destination=dlt.destinations.duckdb(DB_FILE_PATH),
        export_schema_path=os.path.join(SCHEMAS_ROOT, "export"),
        pipeline_name="nyc_open_data_transformation",
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

    destination_dataset_name = "silver_layer"

    try:
        results = dbt.run_all(
            run_params=(
                "--fail-fast",
                "--log-format",
                "text",
                "--log-level",
                "warn",
                "--log-format-file",
                "json",
                "--log-level-file",
                "info",
            ),
            # additional_vars=None,
            # source_tests_selector=None,
            destination_dataset_name=destination_dataset_name,
        )

        safe_send_telegram_text(
            bot_token=bot_token,
            chat_id=chat_id,
            parse_mode=parse_mode,
            text=dbt_models_recap_text(
                dbt_node_results=results, dbt=dbt, app_name=APP_NAME
            ),
        )
    except Exception as ex:
        tip = f"Try looking for errors in the terminal or in the <code>dbt.log</code> file. Or try running <code>dbt run</code> manually."
        safe_send_telegram_text(
            bot_token=bot_token,
            chat_id=chat_id,
            parse_mode=parse_mode,
            text=generic_exception_text(app_name=APP_NAME, exception=ex, tip=tip),
        )

    try:
        results = dbt.test(
            cmd_params=[],
            additional_vars=None,
            destination_dataset_name=destination_dataset_name,
        )

        safe_send_telegram_text(
            bot_token=bot_token,
            chat_id=chat_id,
            parse_mode=parse_mode,
            text=dbt_tests_recap_text(
                dbt_node_results=results, dbt=dbt, app_name=APP_NAME
            ),
        )
    except Exception as ex:
        tip = f"Try looking for errors in the terminal or in the <code>dbt.log</code> file. Or try running <code>dbt test</code> manually."
        safe_send_telegram_text(
            bot_token=bot_token,
            chat_id=chat_id,
            parse_mode=parse_mode,
            text=generic_exception_text(
                app_name=APP_NAME,
                exception=ex,
                tip=tip,
            ),
        )


if __name__ == "__main__":
    run()
