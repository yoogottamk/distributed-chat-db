from typing import Dict, List, Optional, Union

import requests

from ddbms_chat.models.query import Condition, ConditionAnd, ConditionOr
from ddbms_chat.models.syscat import Column, Site, Table
from ddbms_chat.phase1.syscat_tables import fill_tables
from ddbms_chat.phase2.syscat import read_syscat
from ddbms_chat.utils import DBConnection

_, _, _, syscat_sites, _ = read_syscat()


def send_request_to_site(
    site_id: int,
    method: str,
    endpoint: str,
    params: Optional[Dict] = None,
    headers: Optional[Dict] = None,
    json: Optional[Dict] = None,
):
    """
    Send request to site after verifyng it is running

    Also manages authentication related stuff
    """
    sites = syscat_sites.where(id=site_id)
    if len(sites) == 0:
        raise ValueError(f"Site {site_id} not present in system catalog")

    site = sites[0]

    r = requests.get(f"http://{site.ip}:12117/ping")
    if not r.ok:
        raise ValueError(f"Site {site.name} is down")

    method_fn = {
        "get": requests.get,
        "post": requests.post,
    }

    req_headers = (headers or {}) | {"Authorization": site.password}

    r = method_fn[method](
        f"http://{site.ip}:12117{endpoint}",
        params=params,
        headers=req_headers,
        json=json,
    )
    return r


def create_syscat_rows(
    site: Site,
    src_relation: str,
    dst_relation: str,
    column_list: Optional[List[Union[str, Column]]] = None,
):
    with DBConnection(site) as cursor:
        cursor.execute("select 1 + max(id) from `table`")
        res = cursor.fetchone()
        assert res
        table_row_id = list(res.values())[0]

        cursor.execute("select 1 + max(id) from `column`")
        res = cursor.fetchone()
        assert res
        row_id = list(res.values())[0]

        cursor.execute(
            f"select * from `column` where `table` = (select id from `table` where name = {src_relation})"
        )
        if column_list is None:
            columns = [Column(**column) for column in cursor.fetchall()]
        else:
            if len(column_list) > 0 and type(column_list[0]) is str:
                columns = [
                    Column(**column)
                    for column in cursor.fetchall()
                    if column["name"] in column_list
                ]
            else:
                columns = column_list

        for i in range(len(columns)):
            columns[i].id = row_id + i
            columns[i].table = table_row_id

        # create syscat table and column rows
        fill_tables(cursor, [[Table(table_row_id, dst_relation, "-")], columns])


def condition_dict_to_object(
    condition_dict: Dict,
) -> Union[ConditionAnd, Condition, ConditionOr]:
    # Condition
    if "type" not in condition_dict:
        return Condition(**condition_dict)

    if condition_dict["type"] == "or":
        return ConditionOr(
            [condition_dict_to_object(cond) for cond in condition_dict["conditions"]]
        )

    if condition_dict["type"] == "and":
        return ConditionAnd(
            [condition_dict_to_object(cond) for cond in condition_dict["conditions"]]
        )

    raise ValueError(f"Unknown condition type {condition_dict['type']}")


def _process_column_name(col_name: str):
    if "(" not in col_name:
        return col_name.split(".")[-1]

    subsections = col_name.split("(")
    return subsections[0] + "(" + col_name.split(".")[-1]


def construct_select_condition_string(
    condition: Union[Condition, ConditionOr, ConditionAnd]
):

    if type(condition) is Condition:
        return f"{_process_column_name(condition.lhs)} {condition.op} {_process_column_name(condition.rhs)}"

    if type(condition) is ConditionAnd:
        return (
            "("
            + " and ".join(
                [
                    construct_select_condition_string(cond)
                    for cond in condition.conditions
                ]
            )
            + ")"
        )

    if type(condition) is ConditionOr:
        return (
            "("
            + " or ".join(
                [
                    construct_select_condition_string(cond)
                    for cond in condition.conditions
                ]
            )
            + ")"
        )

    raise ValueError(f"Unknown condition of type {type(condition)}")