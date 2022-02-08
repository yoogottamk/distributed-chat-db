from typing import Union
from ddbms_chat.config import DB_NAME
from ddbms_chat.models.syscat import Column, Fragment, Site, Table
from ddbms_chat.syscat.allocation import ALLOCATION
from ddbms_chat.syscat.columns import COLUMNS
from ddbms_chat.syscat.fragments import FRAGMENTS
from ddbms_chat.syscat.sites import SITES
from ddbms_chat.syscat.tables import TABLES
from ddbms_chat.utils import DBConnection, PyQL


def foreign_key_sql(column_name: str, src_table: str, dst_table: str):
    src_table = src_table.lower()
    dst_table = dst_table.lower()

    return f"""constraint `fk_{src_table}_{dst_table}`
  foreign key (`{column_name}`)
  references `{DB_NAME}`.`{dst_table}` (`id`)
    on delete no action
    on update no action"""


def create_table_sql(name: str, columns: PyQL, foreign_key_name: Union[str, None]):
    type_map = {
        "int": "INT",
        "str": "VARCHAR(512)",
        "datetime": "DATETIME",
    }

    column_descriptions = []
    foreign_key_descriptions = []

    for column in columns:
        # handle type
        column_type = type_map.get(column.type)
        if column.type[0].isupper():
            # only create foreign keys for derived horizontal
            if foreign_key_name and foreign_key_name.startswith(column.type.lower()):
                foreign_key_descriptions.append(
                    foreign_key_sql(column.name, name, foreign_key_name)
                )
            column_type = "INT"
        description = f"  `{column.name}` {column_type}"

        # handle not null
        if column.notnull:
            description += " not null"

        # handle primary key / unqiue
        if column.unique and not column.pk:
            description += " unique"

        column_descriptions.append(f"{description},")

    pk_description = ""
    pk_tuple = ""
    for pk_col in columns.where(pk=True):
        pk_tuple += f"`{pk_col.name}`,"
    pk_description = f"  primary key ({pk_tuple[:-1]})"
    newline = "\n"

    return f"""create table if not exists `{DB_NAME}`.`{name}` (
{newline.join(column_descriptions)}
{pk_description}{"," if len(foreign_key_descriptions) else ""}
{",".join(foreign_key_descriptions)})
engine = InnoDB
"""


def setup_tables():
    fragment: Fragment
    for fragment in FRAGMENTS:
        print(f"Creating fragment {fragment.name}")
        table: Table = TABLES.where(id=fragment.table.id)[0]
        columns = COLUMNS.where(table=table)
        site: Site = ALLOCATION.where(fragment=fragment)[0].site

        foreign_key_name = None

        if table.fragment_type == "V":
            vsplit_cols = columns.where(name="id")
            for col_name in fragment.logic.split(","):
                vsplit_cols += columns.where(name=col_name)

            columns = vsplit_cols

        if table.fragment_type == "DH":
            foreign_key_name = FRAGMENTS.where(id=fragment.parent)[0].name

        sql = create_table_sql(fragment.name, columns, foreign_key_name)
        try:
            with DBConnection(site) as cursor:
                cursor.execute(sql)
        except Exception as e:
            print(sql)
            raise e
