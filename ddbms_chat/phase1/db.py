from pymysql.cursors import Cursor

from ddbms_chat.config import DB_NAME


def recreate_db(cursor: Cursor):
    cursor.execute(f"drop database if exists {DB_NAME}")
    cursor.execute(f"create database {DB_NAME}")
