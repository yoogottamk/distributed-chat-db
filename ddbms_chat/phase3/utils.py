import re
from typing import Dict, List, Optional, Union

import requests

from ddbms_chat.models.query import Condition, ConditionAnd, ConditionOr
from ddbms_chat.models.syscat import Column, Site, Table
from ddbms_chat.phase1.syscat_tables import fill_tables
from ddbms_chat.phase2.syscat import read_syscat
from ddbms_chat.utils import DBConnection

_, _, _, syscat_sites, _ = read_syscat()


def get_component_relations(rel_name: str) -> List[str]:
    if "-" not in rel_name:
        relation_name = re.sub(r"_\d+$", "", rel_name)
        return [relation_name]

    return sorted(rel_name.split("-", 1)[1].split("-"))


def send_request_to_site(
    site_id: Optional[int],
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
    if site_id:
        sites = syscat_sites.where(id=site_id)
        if len(sites) == 0:
            raise ValueError(f"Site {site_id} not present in system catalog")

        site = sites[0]
        ip = site.ip
        name = site.name
        password = site.password
    else:
        ip = "127.0.0.1"
        name = "local_node"
        password = ""

    r = requests.get(f"http://{ip}:12117/ping")
    if not r.ok:
        raise ValueError(f"Site {name} is down")

    method_fn = {
        "get": requests.get,
        "post": requests.post,
    }

    req_headers = (headers or {}) | {"Authorization": password}

    r = method_fn[method](
        f"http://{ip}:12117{endpoint}",
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


def condition_object_to_dict(cond: Union[ConditionAnd, Condition, ConditionOr]):
    if type(cond) is Condition:
        return {"lhs": cond.lhs, "op": cond.op, "rhs": cond.rhs}

    if type(cond) is ConditionOr:
        return {
            "conditions": [condition_object_to_dict(x) for x in cond.conditions],
            "type": "or",
        }

    if type(cond) is ConditionAnd:
        return {
            "conditions": [condition_object_to_dict(x) for x in cond.conditions],
            "type": "and",
        }


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
    # probably just a value since my code always converts it to a.b
    if "." not in col_name:
        return col_name

    if "(" not in col_name:
        return f"`{col_name.split('.')[-1]}`"

    subsections = col_name.split("(")
    return subsections[0] + "(`" + col_name.split(".")[-1][:-1] + "`)"


def construct_select_condition_string(
    condition: Union[Condition, ConditionOr, ConditionAnd],
    rel1_name: str = "",
    rel2_name: str = "",
    rel1_cols: List[str] = [],
    rel2_cols: List[str] = [],
):

    if type(condition) is Condition:
        lhs_col = _process_column_name(condition.lhs)
        rhs_col = _process_column_name(condition.rhs)
        if lhs_col == rhs_col:
            return f"`{rel1_name}`.{lhs_col} {condition.op} `{rel2_name}`.{rhs_col}"

        if rel1_name and rel2_name and rel1_cols and rel2_cols:
            if lhs_col in rel1_cols:
                return f"`{rel1_name}`.{lhs_col} {condition.op} `{rel2_name}`.{rhs_col}"
            elif lhs_col in rel2_cols:
                return f"`{rel2_name}`.{lhs_col} {condition.op} `{rel1_name}`.{rhs_col}"

        return f"{lhs_col} {condition.op} {rhs_col}"

    if type(condition) is ConditionAnd:
        return (
            "("
            + " and ".join(
                [
                    construct_select_condition_string(cond, rel1_name, rel2_name)
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
                    construct_select_condition_string(cond, rel1_name, rel2_name)
                    for cond in condition.conditions
                ]
            )
            + ")"
        )

    raise ValueError(f"Unknown condition of type {type(condition)}")
