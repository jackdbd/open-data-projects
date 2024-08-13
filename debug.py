import debugpy
from loguru import logger

from telegram.bot_api import safe_send_text
from telegram.html_texts import generic_exception

host = "localhost"
port = 5678
logger.debug(f"Start debug server on {host}:{port}")
debugpy.listen((host, 5678))
logger.debug("Waiting for debugger attach...")
debugpy.wait_for_client()  # This will block execution until the debugger is attached
logger.debug("Debugger attached.")

# functions that you actually want to debug
safe_send_text(bot_token="123", chat_id="123", text="test message")
generic_exception(exception=Exception("test exception"))
