import pymysql

from ddbms_chat.config import DB_NAME
from ddbms_chat.models.syscat import Site


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
