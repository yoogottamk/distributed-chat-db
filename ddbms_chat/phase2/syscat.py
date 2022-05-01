import csv
from dataclasses import fields
from datetime import datetime
from typing import Optional, Tuple

from ddbms_chat.config import PROJECT_ROOT
from ddbms_chat.models.syscat import Allocation, Column, Fragment, Site, Table
from ddbms_chat.phase1 import db, syscat_tables
from ddbms_chat.syscat.sites import SITES
from ddbms_chat.utils import DBConnection, PyQL, debug_log

CSV_ROOT = PROJECT_ROOT / "ddbms_chat/phase2/syscat"


def read_syscat(
    site: Optional[Site] = None,
) -> Tuple[PyQL[Allocation], PyQL[Column], PyQL[Fragment], PyQL[Site], PyQL[Table]]:
    if site is None:
        site = SITES[0]

    assert site

    with DBConnection(site) as cursor:
        ret = []
        for col_name, col_cls in [
            ("allocation", Allocation),
            ("column", Column),
            ("fragment", Fragment),
            ("site", Site),
            ("table", Table),
        ]:
            cursor.execute(f"select * from `{col_name}`")
            rows = cursor.fetchall()
            ret.append(PyQL([col_cls(**row) for row in rows]))

    return tuple(ret)


def read_syscat_rows_from_csv():
    syscat_name_cls = {
        "allocation": Allocation,
        "column": Column,
        "fragment": Fragment,
        "site": Site,
        "table": Table,
    }

    rows = {table_name: [] for table_name in syscat_name_cls.keys()}

    for table_name, table_cls in syscat_name_cls.items():
        debug_log("%s", "----" * 15)
        debug_log("Reading table %s", table_name)
        field_types = {field.name: field.type for field in fields(table_cls)}

        with open(CSV_ROOT / f"{table_name}.csv", "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cleaned_values = {}
                for k in row:
                    if field_types[k] == "str":
                        cleaned_values[k] = row[k]
                    elif field_types[k] == "int":
                        cleaned_values[k] = int(row[k])
                    elif field_types[k] == "datetime":
                        cleaned_values[k] = datetime.fromtimestamp(float(row[k]))
                    elif field_types[k][0].isupper():
                        cleaned_values[k] = int(row[k])
                    else:
                        raise ValueError(
                            f"Unknown type {field_types[k]} encountered in {table_name}"
                        )

                rows[table_name].append(table_cls(**cleaned_values))

    return rows


if __name__ == "__main__":
    table_map = read_syscat_rows_from_csv()
    tables = [
        table_map["site"],
        table_map["table"],
        table_map["fragment"],
        table_map["allocation"],
        table_map["column"],
    ]

    for site in table_map["site"]:
        with DBConnection(site, connect_db=False) as cursor:
            db.recreate_db(cursor)
        with DBConnection(site) as cursor:
            syscat_tables.setup_tables(cursor)
            syscat_tables.fill_tables(cursor, tables)
