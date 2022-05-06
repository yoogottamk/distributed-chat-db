from ddbms_chat.config import DB_NAME
from ddbms_chat.models.syscat import Allocation, Column, Fragment, Site, Table
from ddbms_chat.syscat.allocation import ALLOCATION
from ddbms_chat.syscat.columns import COLUMNS
from ddbms_chat.syscat.fragments import FRAGMENTS
from ddbms_chat.syscat.sites import SITES
from ddbms_chat.syscat.tables import TABLES
from ddbms_chat.utils import DBConnection, PyQL


def create_table_sql(name: str, columns: PyQL[Column]):
    type_map = {
        "int": "INT",
        "str": "VARCHAR(512)",
        "datetime": "DATETIME",
    }

    column_descriptions = []

    for column in columns:
        # handle type
        column_type = type_map.get(column.type)
        if column.type[0].isupper():
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
{pk_description}
) engine = InnoDB
"""


def setup_tables(
    fragment_list: PyQL[Fragment] = FRAGMENTS,
    table_list: PyQL[Table] = TABLES,
    column_list: PyQL[Column] = COLUMNS,
    allocation_list: PyQL[Allocation] = ALLOCATION,
    site_list: PyQL[Site] = SITES,
):
    for fragment in fragment_list:
        print(f"Creating fragment {fragment.name}")
        table = table_list.where(
            id=fragment.table if type(fragment.table) is int else fragment.table.id
        )[0]

        if type(column_list[0].table) is int:
            columns = column_list.where(table=table.id)
        else:
            columns = column_list.where(table=table)

        if type(allocation_list[0].fragment) is int:
            site = allocation_list.where(fragment=fragment.id)[0].site
        else:
            site = allocation_list.where(fragment=fragment)[0].site

        if type(site) is int:
            site = site_list.where(id=site)[0]

        if table.fragment_type == "V":
            vsplit_cols: PyQL[Column] = PyQL([])
            for col_name in fragment.logic.split(","):
                vsplit_cols += columns.where(name=col_name)

            columns = vsplit_cols

        sql = create_table_sql(fragment.name, columns)

        try:
            with DBConnection(site) as cursor:
                cursor.execute(sql)
        except Exception as e:
            print(sql)
            raise e
