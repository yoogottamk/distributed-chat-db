import csv
from copy import deepcopy
from dataclasses import asdict, fields, make_dataclass
from datetime import datetime
from typing import Dict, List

from ddbms_chat.config import PROJECT_ROOT
from ddbms_chat.models.syscat import Column, Fragment, Site, Table
from ddbms_chat.phase1.app_tables import setup_tables
from ddbms_chat.phase2.syscat import read_syscat
from ddbms_chat.utils import DBConnection, PyQL, debug_log

CSV_ROOT = PROJECT_ROOT / "ddbms_chat/phase2/app_tables"

(
    syscat_allocation,
    syscat_columns,
    syscat_fragments,
    syscat_sites,
    syscat_tables,
) = read_syscat()


def insert_table_sql(fragment_name: str, column_values: Dict):
    processed_values = []
    for k in column_values:
        if type(k) is not int:
            processed_values.append(f"'{column_values[k]}'")
        else:
            processed_values.append(column_values[k])
    sql = f"""insert into `{fragment_name}`
({",".join(map(lambda x: f"`{x}`", column_values.keys()))})
values
({",".join(processed_values)})
"""

    return sql


def make_model(table_name: str, columns: List[Column]):
    model_name = "".join(map(lambda x: x.capitalize(), table_name.split("_")))

    fields = []
    uniq_cols = []

    type_map = {"int": int, "str": str, "datetime": datetime}

    for column in columns:
        fields.append((column.name, type_map.get(column.type, int)))
        if column.pk:
            uniq_cols.append(column.name)

    eq_func = lambda self, o: type(self) == type(o) and all(
        [
            self.__getattr__(uniq_col) == o.__getattr__(uniq_col)
            for uniq_col in uniq_cols
        ]
    )

    return make_dataclass(model_name, fields, namespace={"__eq__": eq_func})


def read_app_rows_from_csv():
    table_names = [(table.name, table.id) for table in syscat_tables]

    rows = {table_name: [] for table_name, _ in table_names}

    for table_name, table_id in table_names:
        model = make_model(table_name, syscat_columns.where(table=table_id).items)
        field_types = {field.name: field.type for field in fields(model)}

        with open(CSV_ROOT / f"{table_name}.csv", "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cleaned_values = {}
                for k in row:
                    if field_types[k] is str:
                        cleaned_values[k] = row[k]
                    elif field_types[k] is int:
                        cleaned_values[k] = int(row[k])
                    elif field_types[k] is datetime:
                        cleaned_values[k] = datetime.fromtimestamp(float(row[k]))
                    else:
                        raise ValueError("Unknown type encountered")

                rows[table_name].append(model(**cleaned_values))

    return rows


def _get_site_from_fragment(fragment: Fragment) -> Site:
    allocation = syscat_allocation.where(fragment=fragment.id)[0]
    return syscat_sites.where(id=allocation.site)[0]


def fill_app_tables(table_rows):
    for table_name in table_rows:
        tables = syscat_tables.where(name=table_name)
        assert len(tables) == 1, "Table not present in system catalog?"
        table: Table = tables[0]

        fragment_type = table.fragment_type

        debug_log("%s", "----" * 15)
        debug_log("Processing table %s fragment type %s", table_name, fragment_type)

        fragments = syscat_fragments.where(table=table.id)
        rows = table_rows[table_name]

        if fragment_type == "-":
            fragment: Fragment = fragments[0]
            site = _get_site_from_fragment(fragment)

            with DBConnection(site) as cursor:
                for row in rows:
                    debug_log(
                        f"[-] inserting into %s @ site %s", fragment.name, site.id
                    )
                    cursor.execute(insert_table_sql(fragment.name, asdict(row)))
        elif fragment_type == "V":
            for fragment in fragments:
                fragment: Fragment
                cols = [*fragment.logic.split(",")]

                site = _get_site_from_fragment(fragment)

                with DBConnection(site) as cursor:
                    for row in rows:
                        row_values = asdict(row)
                        fragment_columns = {}

                        for col in cols:
                            fragment_columns[col] = row_values[col]

                        debug_log(
                            f"[V] inserting into %s @ site %s", fragment.name, site.id
                        )
                        cursor.execute(
                            insert_table_sql(fragment.name, fragment_columns)
                        )
        elif fragment_type == "H":
            for row in rows:
                column_values = asdict(row)
                found_site = False

                for fragment in fragments:
                    fragment: Fragment
                    if not eval(fragment.logic, deepcopy(column_values)):
                        continue

                    found_site = True
                    site = _get_site_from_fragment(fragment)

                    with DBConnection(site) as cursor:
                        debug_log(
                            f"[H] inserting into %s @ site %s", fragment.name, site.id
                        )
                        cursor.execute(insert_table_sql(fragment.name, column_values))
                    break

                assert found_site, "Row didn't satisfy any fragment predicate"
        elif fragment_type == "DH":
            for row in rows:
                column_values = asdict(row)
                found_site = False

                for fragment in fragments:
                    fragment: Fragment
                    parent_fragment: Fragment
                    parent_table: Table

                    parent_fragment = syscat_fragments.where(id=fragment.parent)[0]
                    parent_table = syscat_tables.where(id=parent_fragment.table)[0]
                    parent_rows = PyQL(table_rows[parent_table.name])

                    predicate = parent_fragment.logic

                    orig_key, mapped_key = fragment.logic.split("|", 1)[:2]

                    try:
                        parent_row = parent_rows.where(
                            **{mapped_key: column_values[orig_key]}
                        )[0]
                    except IndexError as e:
                        raise ValueError(
                            "Foreign key constraint broken, couldn't find row with "
                            f"{mapped_key}={column_values[orig_key]} in {parent_table.name}"
                        ) from e

                    predicate_globals = asdict(parent_row)

                    if not eval(predicate, deepcopy(predicate_globals)):
                        continue

                    found_site = True
                    site = _get_site_from_fragment(fragment)

                    with DBConnection(site) as cursor:
                        debug_log(
                            f"[DH] inserting into %s @ site %s", fragment.name, site.id
                        )
                        cursor.execute(insert_table_sql(fragment.name, column_values))
                    break

                assert found_site, "Row didn't satisfy any fragment predicate"


if __name__ == "__main__":
    table_rows = read_app_rows_from_csv()
    setup_tables(
        syscat_fragments, syscat_tables, syscat_columns, syscat_allocation, syscat_sites
    )
    fill_app_tables(table_rows)
