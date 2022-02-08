from pymysql.cursors import Cursor

from ddbms_chat.config import PROJECT_ROOT
from ddbms_chat.syscat.allocation import ALLOCATION
from ddbms_chat.syscat.columns import COLUMNS
from ddbms_chat.syscat.fragments import FRAGMENTS
from ddbms_chat.syscat.sites import SITES
from ddbms_chat.syscat.tables import TABLES


def setup_tables(cursor: Cursor):
    create_statements = []

    sql_dir = PROJECT_ROOT / "ddbms_chat/phase1/sql"
    for sql_file in sorted(sql_dir.glob("*.sql")):
        create_statements.append(sql_file.read_text().strip())

    for statement in create_statements:
        cursor.execute(statement)


def fill_tables(cursor: Cursor):
    for items in [SITES, TABLES, FRAGMENTS, ALLOCATION, COLUMNS]:
        table_name = items[0].__class__.__name__
        print(f"    Creating {table_name}")
        for item in items:
            keys = []
            values = []
            for key, type in item.__annotations__.items():
                keys.append(f"`{key}`")
                value = getattr(item, key)
                if type in ["int", "str", "datetime"]:
                    values.append(value)
                else:
                    values.append(value.id)

            cursor.execute(
                f"insert into `{table_name.lower()}`({','.join(keys)}) values ({','.join(['%s'] * len(keys))})",
                tuple(values),
            )
