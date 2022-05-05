import os
from os import uname
from pathlib import Path

DB_NAME = "L117"
PROJECT_ROOT = Path(__file__).parent.parent

HOSTNAME = uname().nodename.lower()

RUN_OFFLINE = bool(os.getenv("RUN_OFFLINE"))
