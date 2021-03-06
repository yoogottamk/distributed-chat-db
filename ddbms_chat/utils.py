from __future__ import annotations

import inspect
import logging
import os
import re
from copy import deepcopy
from types import GeneratorType
from typing import Dict, Generic, List, TypeVar

import pymysql
from pymysql.cursors import DictCursor

from ddbms_chat.config import DB_NAME
from ddbms_chat.models.syscat import Site

log = logging.getLogger("ddbms_chat")

T = TypeVar("T")


class PyQL(Generic[T]):
    def __init__(self, item_list: List[T], filter: Dict = {}):
        self.items: List[T] = item_list
        self.filter = filter

    def __len__(self) -> int:
        return len(self.items)

    def __getitem__(self, idx) -> T:
        try:
            return self.items[idx]
        except IndexError as e:
            raise IndexError(
                f"Couldn't find item at index {idx} with conditions {self.filter}"
            ) from e

    def __add__(self, o) -> PyQL[T]:
        if len(self.items) == 0:
            return o

        if len(o.items) == 0:
            return self

        if type(self[0]) != type(o[0]):
            raise ValueError("Incompatible types")

        return PyQL(self.items + o.items, self.filter | o.filter)

    def where(self, **kwargs):
        current_list = deepcopy(self.items)
        filtered_list = []
        for k, v in kwargs.items():
            for item in current_list:
                if getattr(item, k) == v:
                    filtered_list.append(item)

            current_list = deepcopy(filtered_list)
            filtered_list = []

        return PyQL(current_list, kwargs)


class DBConnection:
    def __init__(self, site: Site, connect_db: bool = True):
        self.kwargs = {
            "host": site.ip,
            "user": site.user,
            "password": site.password,
            "autocommit": True,
            "cursorclass": DictCursor,
        }

        if connect_db:
            self.kwargs["database"] = DB_NAME

    def __enter__(self):
        self.conn = pymysql.connect(**self.kwargs)
        self.cursor = self.conn.cursor()

        return self.cursor

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.conn.close()


def inspect_object(o):
    members = inspect.getmembers(o)
    values = {}

    for name, value in members:
        if name.startswith("__"):
            continue

        if callable(value):
            signature = inspect.signature(value)
            if len(signature.parameters) == 0:
                ret_val = value()
                if isinstance(ret_val, GeneratorType):
                    ret_val = list(ret_val)
                values[name] = ret_val
            else:
                values[name] = signature
        else:
            values[name] = value

    return values


def debug_log(msg: str, *args):
    caller_name = inspect.stack()[1].function

    if debug_caller_re := os.getenv("DDBMS_CHAT_DEBUG_ONLY"):
        if re.match(debug_caller_re, caller_name) is None:
            return

    log.debug(f"[{caller_name}] {msg}", stacklevel=2, *args)
