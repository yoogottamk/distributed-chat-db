import logging
import os

from rich.logging import RichHandler

logging.basicConfig(
    level=logging.NOTSET, format="%(message)s", datefmt="[%X]", handlers=[RichHandler()]
)

level = logging.CRITICAL
if os.getenv("DDBMS_CHAT_DEBUG") or os.getenv("DDBMS_CHAT_DEBUG_ONLY"):
    level = logging.DEBUG

log = logging.getLogger("ddbms_chat")
# set own loggers level to debug
log.setLevel(level)
