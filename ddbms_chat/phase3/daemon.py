import subprocess
from functools import wraps
from http import HTTPStatus
from typing import List

from flask import Flask, abort, request

from ddbms_chat.config import DB_NAME, HOSTNAME
from ddbms_chat.phase2.syscat import read_syscat
from ddbms_chat.phase3.utils import (
    _process_column_name,
    condition_dict_to_object,
    construct_select_condition_string,
    send_request_to_site,
)
from ddbms_chat.utils import DBConnection, debug_log

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

tx_log_file = open("tx-participant.log", "w+")


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
            sql = r.json()["table_sql"]

            processed_sql = []
            for line in sql.split("\n"):
                if line.startswith("DROP TABLE IF EXISTS `"):
                    processed_sql.append(
                        f"DROP TABLE IF EXISTS `{target_relation_name}`;"
                    )
                elif line.startswith("CREATE TABLE `"):
                    processed_sql.append(f"CREATE TABLE `{target_relation_name}` (")
                elif line.startswith("LOCK TABLES `"):
                    processed_sql.append(f"LOCK TABLES `{target_relation_name}` WRITE;")
                elif line.startswith("INSERT INTO `"):
                    processed_sql.append(
                        f"INSERT INTO `{target_relation_name}` "
                        + line.split("`", 2)[-1]
                    )
                elif line.startswith(") ENGINE=InnoDB"):
                    processed_sql.append(") ENGINE=InnoDB;")
                elif line.strip():
                    processed_sql.append(line.strip())

            sql = "\n".join(processed_sql)
            with DBConnection(CURRENT_SITE) as cursor:
                for query in sql.split(";"):
                    if query.strip():
                        debug_log(query.strip())
                        cursor.execute(query.strip())
        case "union":
            relation1_name, relation2_name, target_relation_name = (
                payload["relation1_name"],
                payload["relation2_name"],
                payload["target_relation_name"],
            )
            with DBConnection(CURRENT_SITE) as cursor:
                query = (
                    f"create table `{target_relation_name}` as "
                    f"select * from `{relation1_name}` union select * from `{relation2_name}`"
                )
                debug_log(query)
                cursor.execute(query)
        case "join":
            relation1_name, relation2_name, join_condition, target_relation_name = (
                payload["relation1_name"],
                payload["relation2_name"],
                payload["join_condition"],
                payload["target_relation_name"],
            )
            with DBConnection(CURRENT_SITE) as cursor:
                cursor.execute(
                    f"select column_name from information_schema.columns where table_name = '{relation1_name}'"
                )
                rel1_cols = {list(x.values())[0] for x in cursor.fetchall()}
                cursor.execute(
                    f"select column_name from information_schema.columns where table_name = '{relation2_name}'"
                )
                rel2_cols = {list(x.values())[0] for x in cursor.fetchall()}

                intersection = rel1_cols & rel2_cols

                if len(intersection) > 1:
                    abort(
                        HTTPStatus.INTERNAL_SERVER_ERROR,
                        description=f"One or more of these column names are ambiguous: {intersection}",
                    )
                if len(intersection) == 0:
                    rel_cols = rel1_cols | rel2_cols
                else:
                    rel_cols = ((rel1_cols | rel2_cols) - intersection) | {
                        f"`{relation1_name}`.`{list(intersection)[0]}`"
                    }

                quoted_cols = []
                for x in rel_cols:
                    if not x.startswith("`") and "(" not in x:
                        quoted_cols.append(f"`{x}`")
                    else:
                        quoted_cols.append(x)

                join_condition = condition_dict_to_object(join_condition)
                query = (
                    f"create table `{target_relation_name}` as "
                    f"select {','.join(quoted_cols)} from `{relation1_name}` join `{relation2_name}` "
                    f"on {construct_select_condition_string(join_condition, relation1_name, relation2_name, list(rel1_cols), list(rel2_cols))}"
                )
                debug_log(query)
                cursor.execute(query)
        case "select":
            relation_name, select_condition, target_relation_name = (
                payload["relation_name"],
                payload["select_condition"],
                payload["target_relation_name"],
            )
            select_condition = condition_dict_to_object(select_condition)
            with DBConnection(CURRENT_SITE) as cursor:
                query = (
                    f"create table `{target_relation_name}` as "
                    f"select * from `{relation_name}` "
                    f"where {construct_select_condition_string(select_condition)}"
                )
                debug_log(query)
                cursor.execute(query)
        case "project":
            relation_name, project_columns, target_relation_name = (
                payload["relation_name"],
                payload["project_columns"],
                payload["target_relation_name"],
            )

            group_by_str = ""
            if "group_by" in payload:
                gb_cols = [_process_column_name(col) for col in payload["group_by"]]
                group_by_str = f" group by {','.join(gb_cols)}"
                if "having" in payload:
                    group_by_str += f" having {construct_select_condition_string(condition_dict_to_object(payload['having']))}"

            reduced_columns = [_process_column_name(col) for col in project_columns]
            quoted_cols = []
            for x in reduced_columns:
                if not x.startswith("`") and "(" not in x:
                    quoted_cols.append(f"`{x}`")
                else:
                    quoted_cols.append(x)
            with DBConnection(CURRENT_SITE) as cursor:
                query = (
                    f"create table `{target_relation_name}` as "
                    f"select {','.join(quoted_cols)} from `{relation_name}` {group_by_str}"
                )
                debug_log(query)
                cursor.execute(query)
        case "rename":
            old_name, new_name = payload["old_name"], payload["new_name"]
            with DBConnection(CURRENT_SITE) as cursor:
                cursor.execute(f"rename table `{old_name}` to `{new_name}`")
        case unk_action:
            abort(HTTPStatus.BAD_REQUEST, description=f"Unknown action {unk_action}")

    return {"success": True}


@authenticate_request
@app.get("/fetch/<relation_name>")
def fetch_relation(relation_name: str):
    dump = subprocess.Popen(
        [
            "mysqldump",
            f"-u{CURRENT_SITE.user}",
            f"-p{CURRENT_SITE.password}",
            DB_NAME,
            relation_name,
        ],
        stdout=subprocess.PIPE,
    )

    if (exit_code := dump.wait()) != 0:
        abort(
            HTTPStatus.BAD_REQUEST,
            description=f"mysqldump failed with error code {exit_code}",
        )

    result_lines = []
    for l in dump.stdout.readlines():
        l = l.decode("utf-8").strip()
        if len(l) == 0 or l.startswith("/*") or l.startswith("--"):
            continue
        result_lines.append(l)

    return {"table_sql": "\n".join(result_lines)}


@authenticate_request
@app.post("/cleanup/<query_id>")
def cleanup(query_id: str):
    with DBConnection(CURRENT_SITE) as cursor:
        cursor.execute(
            "select table_name from information_schema.tables where table_schema = %s",
            (DB_NAME,),
        )
        existing_relations: List[str] = [
            list(row.values())[0] for row in cursor.fetchall()
        ]

        for relation in existing_relations:
            if relation.startswith(query_id):
                cursor.execute(f"drop table `{relation}`")

    return {"success": True}


@authenticate_request
@app.post("/2pc/prepare")
def tx_2pc_prepare():
    global RUNNING_READ_QUERY, RUNNING_WRITE_QUERY

    payload = request.json

    sql = payload["sql"]
    txid = payload["txid"]

    # in the middle of another query
    if RUNNING_READ_QUERY or RUNNING_WRITE_QUERY:
        tx_log_file.write(
            f"{txid}: abort: cant write; in the middle of another query\n"
        )
        return "vote-abort"

    RUNNING_WRITE_QUERY = True

    split_sql = sql.strip().split()
    relation_name = split_sql[1].strip("`")
    sql = " ".join([split_sql[0], f"`{txid}_{relation_name}`", *split_sql[2:]])

    try:
        with DBConnection(CURRENT_SITE) as cursor:
            create_table_sql = f"create table `{txid}_{relation_name}` as select * from `{relation_name}`"
            debug_log(create_table_sql)
            cursor.execute(create_table_sql)
            debug_log(sql)
            cursor.execute(sql)
    except Exception as e:
        print(e)
        tx_log_file.write(f"{txid}: abort: error\n")
        return "vote-abort"

    tx_log_file.write(f"{txid}: vote-commit\n")
    return "vote-commit"


@authenticate_request
@app.post("/2pc/global-commit")
def tx_2pc_global_commit():
    global RUNNING_READ_QUERY, RUNNING_WRITE_QUERY
    RUNNING_WRITE_QUERY = False

    payload = request.json

    txid = payload["txid"]

    with DBConnection(CURRENT_SITE) as cursor:
        cursor.execute(
            "select table_name from information_schema.tables where table_schema = %s",
            (DB_NAME,),
        )
        existing_relations: List[str] = [
            list(row.values())[0] for row in cursor.fetchall()
        ]

        for relation in existing_relations:
            if relation.startswith(txid):
                debug_log("Found relation %s", relation)
                src_relation_name = relation
                target_relation_name = relation.split("_", 1)[1]
                debug_log("Dropping %s", target_relation_name)
                cursor.execute(f"drop table `{target_relation_name}`")
                debug_log("Renaming %s to %s", src_relation_name, target_relation_name)
                cursor.execute(
                    f"rename table `{src_relation_name}` to `{target_relation_name}`"
                )
                break

    tx_log_file.write(f"{txid}: commit\n")
    return {"success": True}


@authenticate_request
@app.post("/2pc/global-abort")
def tx_2pc_global_abort():
    global RUNNING_READ_QUERY, RUNNING_WRITE_QUERY
    RUNNING_WRITE_QUERY = False

    payload = request.json

    txid = payload["txid"]

    with DBConnection(CURRENT_SITE) as cursor:
        cursor.execute(
            "select table_name from information_schema.tables where table_schema = %s",
            (DB_NAME,),
        )
        existing_relations: List[str] = [
            list(row.values())[0] for row in cursor.fetchall()
        ]

        for relation in existing_relations:
            if relation.startswith(txid):
                cursor.execute(f"drop table `{relation}`")
                break

    tx_log_file.write(f"{txid}: abort\n")
    return {"success": True}


if __name__ == "__main__":
    app.run("0.0.0.0", 12117, debug=DEBUG)
