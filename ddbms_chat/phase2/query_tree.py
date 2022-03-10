from typing import List, Union

import networkx as nx
from rich.pretty import pprint

from ddbms_chat.models.query import Condition, ConditionAnd, ConditionOr
from ddbms_chat.models.tree import (
    JoinNode,
    ProjectionNode,
    RelationNode,
    SelectionNode,
    UnionNode,
)
from ddbms_chat.phase2.parser import parse_select, parse_sql
from ddbms_chat.utils import debug_log, inspect_object


def _find_columns_used_by_condition(
    condition: Union[Condition, ConditionAnd, ConditionOr], tables: List[str]
) -> List[str]:
    if type(condition) is Condition:
        columns = []

        for table in tables:
            if condition.lhs.startswith(f"{table}."):
                columns.append(condition.lhs)
            if condition.rhs.startswith(f"{table}."):
                columns.append(condition.rhs)

        return list(set(columns))

    # make type checker happy
    assert (type(condition) is ConditionAnd) or (type(condition) is ConditionOr)

    columns = []
    for child_condition in condition.conditions:
        columns += _find_columns_used_by_condition(child_condition, tables)

    return list(set(columns))


def build_naive_query_tree(sql_query: str):
    parsed_query = parse_sql(sql_query)
    print(parsed_query)
    query = parse_select(parsed_query)

    qt = nx.DiGraph()
    node_map = {"relations": {}, "conditions": []}
    relation_heads = {}

    pprint(query)

    for table in query.tables:
        relation = RelationNode(table)
        node_map["relations"][table] = relation
        qt.add_node(relation)
        relation_heads[relation] = relation

    if query.where:
        for condition in query.where.conditions:
            columns_used = _find_columns_used_by_condition(condition, query.tables)
            node_map["conditions"].append((SelectionNode(condition), columns_used))

            qt.add_node(SelectionNode(condition), shape="rectangle")

    # iterate through conditions using only 1 columns to make selections more selective
    node_map["conditions"] = sorted(node_map["conditions"], key=lambda x: len(x[1]))

    for condition, columns_used in node_map["conditions"]:
        if len(columns_used) == 1:
            relation = node_map["relations"].get(columns_used[0].split(".")[0])
            if relation in relation_heads:
                debug_log("Adding edge from %s to %s", condition, relation)
                qt.add_edge(condition, relation_heads[relation])
                relation_heads[relation] = condition
        else:
            relation_list = list(
                map(
                    lambda col: node_map["relations"].get(col.split(".")[0]),
                    columns_used,
                )
            )
            skip_update = False

            # check if all conditions use columns from tables
            for relation in relation_list:
                if relation not in relation_heads:
                    skip_update = True

            if skip_update:
                continue

            # check if selection needs to be converted to a join
            dst_nodes = set()
            for relation in relation_list:
                # skip self edge
                if hash(condition) == hash(relation_heads[relation]):
                    continue
                dst_nodes.add(relation_heads[relation])

            for relation in relation_list:
                # skip self edge
                if hash(condition) == hash(relation_heads[relation]):
                    continue
                debug_log("Adding edge from %s to %s", condition, relation)
                qt.add_edge(condition, relation_heads[relation])
                relation_heads[relation] = condition

    nx.nx_pydot.to_pydot(qt).write_png("qt.png")


if __name__ == "__main__":
    # test_query = (
    #     "SELECT P.Pnumber, P.Dnum, E.Lname, E.Address, E.Bdate "
    #     "FROM PROJECT P, DEPARTMENT D, EMPLOYEE E "
    #     "WHERE P.Dnum=D.Dnumber AND D.Mgr_ssn=E.Ssn AND P.Plocation = 'Stafford'"
    # )
    # test_query = (
    #     "SELECT P.Pnumber, P.Dnum, E.Lname, E.Address, E.Bdate "
    #     "FROM PROJECT P, EMPLOYEE E "
    #     "INNER JOIN DEPARTMENT D ON P.Dnum = D.Dnumber "
    #     "WHERE D.Mgr_ssn = E.Ssn AND D.mgr_ssn % 3 = 1 AND P.Plocation = 'Stafford' "
    #     "GROUP BY P.Pnumber, P.Dnum "
    #     "HAVING P.Pnumber > 5 OR P.Dnum < 3 "
    #     "LIMIT 10"
    # )
    test_query = (
        "select G.`name`, M.`content` "
        "from `group` G, `message` M, `group_member` GM, `user` U "
        "where GM.`user` = 1 and U.`id` = 1 and GM.`group` = G.`id` and M.`sent_at` > U.`last_seen` and M.group = G.id"
    )
    build_naive_query_tree(test_query)
