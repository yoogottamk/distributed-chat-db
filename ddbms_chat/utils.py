from copy import deepcopy
from typing import List

import pymysql

from ddbms_chat.config import DB_NAME
from ddbms_chat.models.syscat import Site


class PyQL:
    def __init__(self, item_list: List):
        self.items = item_list

    def __getitem__(self, idx):
        return self.items[idx]

    def where(self, **kwargs):
        current_list = deepcopy(self.items)
        filtered_list = []
        for k, v in kwargs.items():
            for item in current_list:
                if getattr(item, k) == v:
                    filtered_list.append(item)

            current_list = deepcopy(filtered_list)
            filtered_list = []

        return PyQL(current_list)


class DBConnection:
    def __init__(self, site: Site, connect_db: bool = True):
        self.kwargs = {
            "host": site.ip,
            "user": site.user,
            "password": site.password,
            "autocommit": True,
        }

        if connect_db:
            self.kwargs["database"] = DB_NAME

    def __enter__(self):
        self.conn = pymysql.connect(**self.kwargs)
        self.cursor = self.conn.cursor()

        return self.cursor

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.conn.close()
