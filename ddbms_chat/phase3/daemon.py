from functools import wraps
from http import HTTPStatus
from typing import List

from flask import Flask, abort, request

from ddbms_chat.config import DB_NAME, HOSTNAME
from ddbms_chat.models.query import ConditionAnd
from ddbms_chat.models.syscat import Column
from ddbms_chat.phase2.app_tables import insert_table_sql
from ddbms_chat.phase2.syscat import read_syscat
from ddbms_chat.phase3.utils import (
    _process_column_name,
    condition_dict_to_object,
    construct_select_condition_string,
    create_syscat_rows,
    send_request_to_site,
)
from ddbms_chat.utils import DBConnection

app = Flask(__name__)
_, _, _, syscat_sites, _ = read_syscat()
sites = syscat_sites.where(name=HOSTNAME)

# debugging
if len(sites) == 0:
    DEBUG = True
# running on actual node
else:
    DEBUG = False
    CURRENT_SITE = sites[0]


def authenticate_request(f):
    @wraps(f)
    def _authenticate_request(*args, **kwargs):
        if DEBUG or request.remote_addr == "127.0.0.1":
            return f(*args, **kwargs)

        auth_header = request.headers.get("Authorization")
        if auth_header != CURRENT_SITE.password:
            abort(HTTPStatus.UNAUTHORIZED, description="Wrong credentials provided")

        return f(*args, **kwargs)

    return _authenticate_request


RUNNING_READ_QUERY = False
RUNNING_WRITE_QUERY = False


@app.get("/ping")
def healthcheck():
    return "pong"


@authenticate_request
@app.post("/exec/<action>")
def exec_query(action: str):
    payload = request.json

    if payload is None:
        abort(HTTPStatus.BAD_REQUEST)

    match action:
        case "fetch":
            relation_name, site_id, target_relation_name = (
                payload["relation_name"],
                payload["site_id"],
                payload["target_relation_name"],
            )
            r = send_request_to_site(site_id, "get", f"/fetch/{relation_name}")
            if not r.ok:
                abort(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    description=f"Couldn't fetch rows for {relation_name} from site {site_id}",
                )
            payload = r.json()

            create_syscat_rows(
                CURRENT_SITE,
                "",
                target_relation_name,
                [Column(**column) for column in payload["columns"]],
            )
            with DBConnection(CURRENT_SITE) as cursor:
                for row in payload["rows"]:
                    sql = insert_table_sql(target_relation_name, row)
                    cursor.execute(sql)
        case "union":
            relation1_name, relation2_name, target_relation_name = (
                payload["relation1_name"],
                payload["relation2_name"],
                payload["target_relation_name"],
            )
            create_syscat_rows(CURRENT_SITE, relation1_name, target_relation_name)
            with DBConnection(CURRENT_SITE) as cursor:
                cursor.execute(
                    f"create table {target_relation_name} as "
                    f"select * from {relation1_name} union select * from {relation2_name}"
                )
        case "join":
            relation1_name, relation2_name, join_condition, target_relation_name = (
                payload["relation1_name"],
                payload["relation2_name"],
                payload["join_condition"],
                payload["target_relation_name"],
            )
            with DBConnection(CURRENT_SITE) as cursor:
                # TODO: put columns in syscat
                create_syscat_rows(CURRENT_SITE, "", target_relation_name, [])
                join_condition = condition_dict_to_object(join_condition)
                cursor.execute(
                    f"create table {target_relation_name} as "
                    f"select * from {relation1_name}, {relation2_name} "
                    f"where {construct_select_condition_string(join_condition)}"
                )
        case "select":
            relation_name, select_condition, target_relation_name = (
                payload["relation_name"],
                payload["select_condition"],
                payload["target_relation_name"],
            )
            select_condition = condition_dict_to_object(select_condition)
            if type(select_condition) is not ConditionAnd:
                select_condition = ConditionAnd([select_condition])
            create_syscat_rows(CURRENT_SITE, relation_name, target_relation_name)
            with DBConnection(CURRENT_SITE) as cursor:
                cursor.execute(
                    f"create table {target_relation_name} as "
                    f"select * from {relation_name} "
                    f"where {construct_select_condition_string(select_condition)}"
                )
        case "project":
            relation_name, project_columns, target_relation_name = (
                payload["relation_name"],
                payload["project_columns"],
                payload["target_relation_name"],
            )
            reduced_columns = [_process_column_name(col) for col in project_columns]
            create_syscat_rows(
                CURRENT_SITE, relation_name, target_relation_name, reduced_columns
            )
            with DBConnection(CURRENT_SITE) as cursor:
                cursor.execute(
                    f"create table {target_relation_name} as "
                    f"select {','.join(reduced_columns)} from {relation_name}"
                )
        case "rename":
            # TODO: implement rename
            ...
        case unk_action:
            abort(HTTPStatus.BAD_REQUEST, description=f"Unknown action {unk_action}")


@authenticate_request
@app.get("/fetch/<relation_name>")
def fetch_relation(relation_name: str):
    with DBConnection(CURRENT_SITE) as cursor:
        cursor.execute(
            "select table_name from information_schema.tables where table_schema = %s",
            (DB_NAME,),
        )
        existing_relations = {row["TABLE_NAME"] for row in cursor.fetchall()}

    if relation_name not in existing_relations:
        abort(HTTPStatus.BAD_REQUEST, description=f"Unknown relation {relation_name}")

    with DBConnection(CURRENT_SITE) as cursor:
        cursor.execute(f"select * from {relation_name}")
        rows = cursor.fetchall()
        cursor.execute(
            f"select * from `column` where `table` = (select id from `table` where name = {relation_name})"
        )
        columns = cursor.fetchall()

    return {"rows": rows, "columns": columns}


@authenticate_request
@app.post("/cleanup/<query_id>")
def cleanup(query_id: str):
    with DBConnection(CURRENT_SITE) as cursor:
        cursor.execute(
            "select table_name from information_schema.tables where table_schema = %s",
            (DB_NAME,),
        )
        existing_relations: List[str] = [row["TABLE_NAME"] for row in cursor.fetchall()]

        for relation in existing_relations:
            if relation.startswith(query_id):
                cursor.execute(f"drop table {relation}")

        cursor.execute(f"select * from `table` where name like '{query_id}%'")
        tables_to_delete = cursor.fetchall()
        cursor.execute(f"delete from `table` where name like '{query_id}%'")

        for relation in tables_to_delete:
            cursor.execute(f"delete from `column` where `table` = {relation['table']}")


if __name__ == "__main__":
    app.run("0.0.0.0", 12117, debug=DEBUG)
