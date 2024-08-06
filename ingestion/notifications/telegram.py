import requests
from typing import Dict

# from dlt.common import logger
from loguru import logger
from dlt.common.json import json


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

    # raise Exception("Some non-HTTPError exception (for testing)")


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
    text = """
    <b>Test message</b>

    You should watch <a href="https://youtu.be/dQw4w9WgXcQ?si=9p8oAhp09Q4Ge6ws">this cool video</a>.

    <pre><code class="language-python">
import pandas

df = pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 4]})</code></pre>
    """
    bot_token = "your-telegram-bot-token"
    chat_id = "your-telegram-chat-id"

    pipeline_name = "my_pipeline"
    table_name = "my_table"
    column_name = "my_column"

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

    link_preview_options = {"is_disabled": True}
    # link_preview_options = {"show_above_text": True}

    safe_send_text(
        bot_token=bot_token,
        chat_id=chat_id,
        text=text,
        link_preview_options=link_preview_options,
    )

    send_text(
        bot_token=bot_token,
        chat_id=chat_id,
        text=text,
        link_preview_options=link_preview_options,
    )
