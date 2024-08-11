import dlt
import os
import sys
from socrata import nyc_open_data_source
from dlt.common.configuration.inject import with_config
from dlt.common.pipeline import LoadInfo
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.pipeline.exceptions import PipelineStepFailed

# Add the parent directory to the system path so that I can import Python code from sibling directories.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common import (
    get_telegram_config,
    get_telegram_credentials,
    APP_NAME,
    DB_FILE_PATH,
    SCHEMAS_ROOT,
)
from telegram import (
    load_info_text,
    runtime_configuration_text,
    safe_send_telegram_text,
    pipeline_step_failed_text,
    config_field_missing_exception_text,
    table_schema_update_text,
)

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
duckdb_catalog = "nyc_open_data"
duckdb_schema = "landing_zone"

# A single dlt pipeline will create, in the specified dlt dataset (i.e. DuckDB
# schema) a table for each REST API resource.
# https://dlthub.com/devel/general-usage/destination-tables
pipeline_name = "nyc_open_data_ingestion"


def run() -> None:
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
        destination=dlt.destinations.duckdb(DB_FILE_PATH),
        # dev_mode=True,
        export_schema_path=os.path.join(SCHEMAS_ROOT, "export"),
        pipeline_name=pipeline_name,
        progress="log",
    )

    # I don't think dlt allows us to retrieve the dlt runtime configuration of a
    # pipeline that ran in the past. It might be a good idea to extract it now
    # and store it somewhere (e.g. table in dlt destination, GitHub comment,
    # Slack channel, etc).
    safe_send_telegram_text(
        bot_token=bot_token,
        chat_id=chat_id,
        parse_mode=parse_mode,
        text=runtime_configuration_text(pipeline=pipeline, app_name=APP_NAME),
    )

    # https://dlthub.com/docs/walkthroughs/run-a-pipeline#failed-api-or-database-connections-and-other-exceptions
    try:
        load_info: LoadInfo = pipeline.run(nyc_open_data_source())
    except PipelineStepFailed as ex:
        safe_send_telegram_text(
            bot_token=bot_token,
            chat_id=chat_id,
            parse_mode=parse_mode,
            text=pipeline_step_failed_text(exception=ex, app_name=APP_NAME),
        )
        # we handled the exception by sending a notification to Telegram, so we
        # now let the exception bubble up
        raise
    except ConfigFieldMissingException as ex:
        safe_send_telegram_text(
            bot_token=bot_token,
            chat_id=chat_id,
            parse_mode=parse_mode,
            text=config_field_missing_exception_text(exception=ex, app_name=APP_NAME),
        )
        raise

    safe_send_telegram_text(
        bot_token=bot_token,
        chat_id=chat_id,
        parse_mode=parse_mode,
        text=load_info_text(load_info=load_info, app_name=APP_NAME),
    )

    # Send alerts about schema updates to Telegram.
    # https://dlthub.com/docs/running-in-production/alerting#slack
    # https://dlthub.com/docs/examples/chess_production/
    # https://dlthub.com/docs/walkthroughs/add_credentials#adding-credentials-to-your-deployment
    for package in load_info.load_packages:
        for table_name, table in package.schema_update.items():
            safe_send_telegram_text(
                bot_token=bot_token,
                chat_id=chat_id,
                parse_mode=parse_mode,
                text=table_schema_update_text(app_name=APP_NAME, table_name=table_name),
            )

    # dlt does NOT raise exceptions on failed jobs, unless we call the method
    # load_info.raise_on_failed_jobs(), which raises a DestinationHasFailedJobs
    # exception.
    # https://dlthub.com/docs/walkthroughs/run-a-pipeline#failed-jobs-in-load-package
    # TODO: Explain the pros & cons of calling this method.
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
