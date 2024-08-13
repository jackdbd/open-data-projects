import os

import dlt
from dlt.common.configuration.inject import with_config

REPO_ROOT = os.path.abspath(os.path.join(__file__, "..", ".."))
DATA_ROOT = os.path.join(REPO_ROOT, "assets", "data")
SCHEMAS_ROOT = os.path.join(REPO_ROOT, "assets", "schemas")

APP_NAME = "Open Data Projects"
DB_NAME = "nyc_open_data"
DB_FILE_PATH = os.path.join(DATA_ROOT, f"{DB_NAME}.duckdb")
DBT_PACKAGE_PATH = os.path.join(REPO_ROOT, "transformation", "nyc_open_data")
TEST_DLT_PIPELINE_NAME = "test_pipeline"


@with_config(sections=("telegram"))
def get_telegram_config(config=dlt.config.value):
    return {"parse_mode": config["parse_mode"]}


@with_config(sections=("telegram"))
def get_telegram_credentials(credentials=dlt.secrets.value):
    return {"bot_token": credentials["bot_token"], "chat_id": credentials["chat_id"]}


if __name__ == "__main__":
    telegram_config = get_telegram_config()
    telegram_credentials = get_telegram_credentials()
    print("Telegram config")
    print(telegram_config)

    print("\nTelegram credentials")
    print(telegram_credentials)
