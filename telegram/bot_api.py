import dlt
import requests
from typing import Dict
from dlt.common import logger
from dlt.common.json import json
from dlt.pipeline import Pipeline
from dlt.pipeline.exceptions import PipelineStepFailed


def send_text(
    bot_token: str,
    chat_id: str,
    text: str,
    disable_notification: bool = True,
    parse_mode: str = "HTML",
    link_preview_options: Dict = {"is_disabled": False, "show_above_text": False},
) -> None:
    """Tries sending a `text` to a Telegram `chat_id` (does **not** handle exceptions).

    :param bot_token: Token of your Telegram bot.
    :param chat_id: ID of the Telegram chat where you want to send the `text` to.
    :param text: Text message.

    Reference:
    - <https://core.telegram.org/bots/api#sendmessage>
    - <https://core.telegram.org/bots/api#html-style>
    - <https://core.telegram.org/bots/api#linkpreviewoptions>
    """
    logger.debug(f"send text to Telegram chat {chat_id}")

    r = requests.post(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        data=json.dumps(
            {
                "chat_id": chat_id,
                "disable_notification": disable_notification,
                "link_preview_options": link_preview_options,
                "parse_mode": parse_mode,
                "text": text,
            }
        ).encode("utf-8"),
        headers={"Content-Type": "application/json;charset=utf-8"},
    )

    r.raise_for_status()
    # logger.warning(r.json())


def safe_send_text(
    bot_token: str,
    chat_id: str,
    text: str,
    disable_notification: bool = True,
    parse_mode: str = "HTML",
    link_preview_options: Dict = {"is_disabled": False, "show_above_text": False},
) -> None:
    """Tries sending a `text` to a Telegram `chat_id`, and handles any exception
    that might be raised.

    :param bot_token: Token of your Telegram bot.
    :param chat_id: ID of the Telegram chat where you want to send the `text` to.
    :param text: Text message.
    """
    try:
        send_text(
            bot_token=bot_token,
            chat_id=chat_id,
            text=text,
            link_preview_options=link_preview_options,
        )
    except requests.exceptions.HTTPError as e:
        logger.warning(e)
    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    import os
    import sys

    # Add the parent directory to the system path so that I can import Python code from sibling directories.
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from common import (
        get_telegram_config,
        get_telegram_credentials,
        APP_NAME,
        TEST_DLT_PIPELINE_NAME,
    )

    # replace dlt logger with loguru, so I don't have to remember to set an
    # environment variable to see log messages
    from loguru import logger

    from html_texts import (
        pipeline_step_failed,
        runtime_configuration,
        table_schema_update,
    )

    telegram_config = get_telegram_config()
    telegram_credentials = get_telegram_credentials()
    parse_mode = telegram_config["parse_mode"]
    bot_token = telegram_credentials["bot_token"]
    chat_id = telegram_credentials["chat_id"]

    text = """🧪 <b>Test message</b>

You should watch <a href="https://youtu.be/dQw4w9WgXcQ?si=9p8oAhp09Q4Ge6ws">this cool video</a>.

<pre><code class="language-python">
import pandas

df = pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 4]})</code></pre>"""

    link_preview_options = {"is_disabled": True}
    # link_preview_options = {"show_above_text": True}

    logger.debug(f"send generic text")
    safe_send_text(
        bot_token=bot_token,
        chat_id=chat_id,
        text=text,
        link_preview_options=link_preview_options,
    )

    app_name = APP_NAME
    pipeline_name = TEST_DLT_PIPELINE_NAME
    pipeline = dlt.pipeline(pipeline_name=pipeline_name)

    logger.debug(f"send runtime_configuration text")
    safe_send_text(
        bot_token=bot_token,
        chat_id=chat_id,
        text=runtime_configuration(pipeline=pipeline, app_name=app_name),
    )

    load_id = "12345.67890"
    step = "extract"  # one of "sync", "extract", "normalize", "load"
    step_info = None

    logger.debug(f"send pipeline_step_failed text")
    safe_send_text(
        bot_token=bot_token,
        chat_id=chat_id,
        text=pipeline_step_failed(
            exception=PipelineStepFailed(
                pipeline,
                exception=Exception("I am the original cause of the failure"),
                step=step,
                step_info=step_info,
                load_id=load_id,
            ),
            app_name=app_name,
        ),
    )

    table_name = "my_table"
    logger.debug(f"send table_schema_update text")
    safe_send_text(
        bot_token=bot_token,
        chat_id=chat_id,
        text=table_schema_update(
            app_name=app_name,
            table_name=table_name,
            table={"columns": {"foo": "VARCHAR", "bar": "INTEGER"}},
        ),
    )
