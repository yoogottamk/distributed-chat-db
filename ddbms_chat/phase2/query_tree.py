from typing import List, Union

import networkx as nx
from rich.pretty import pprint

from ddbms_chat.models.query import Condition, ConditionAnd, ConditionOr
from ddbms_chat.models.syscat import Table
from ddbms_chat.models.tree import (
    JoinNode,
    ProjectionNode,
    RelationNode,
    SelectionNode,
    UnionNode,
)
from ddbms_chat.phase2.parser import parse_select, parse_sql
from ddbms_chat.phase2.syscat import read_syscat
from ddbms_chat.phase2.utils import to_pydot
from ddbms_chat.utils import debug_log

(
    syscat_allocation,
    syscat_columns,
    syscat_fragments,
    syscat_sites,
    syscat_tables,
) = read_syscat()


def _find_columns_used_by_condition(
    condition: Union[Condition, ConditionAnd, ConditionOr], tables: List[str]
) -> List[str]:
    """
    find all the columns a condition references
    """
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


def get_relation_head(qt: nx.DiGraph, node):
    """
    get the root node for a given relation

    useful for adding nodes based on conditions
    """
    in_edges = list(qt.in_edges(node))

    if len(in_edges) == 0:
        return node

    return get_relation_head(qt, in_edges[0][0])


def localize_query_tree(qt: nx.DiGraph, nodes: List[RelationNode]):
    """
    localize all relations
    """
    for relation_node in nodes:
        tables = syscat_tables.where(name=relation_node.name)

        assert len(tables) == 1, f"Table {relation_node.name} not found"
        table: Table = tables[0]

        if table.fragment_type == "-":
            fragment = syscat_fragments.where(table=table.id)[0]
            new_relation_root = RelationNode(relation_node.name)
            new_relation_root.is_localized = True
            new_relation_root.site_id = syscat_allocation.where(fragment=fragment.id)[
                0
            ].site
            qt.add_node(new_relation_root, shape="rectangle", style="filled")
        else:
            get_node = (
                lambda: JoinNode(Condition("id", "=", "id"))
                if table.fragment_type == "V"
                else UnionNode()
            )

            fragments = syscat_fragments.where(table=table.id)

            new_relation_root = get_node()

            for fragment in fragments[:2]:
                rel_node = RelationNode(fragment.name)
                rel_node.is_localized = True
                rel_node.site_id = syscat_allocation.where(fragment=fragment.id)[0].site
                qt.add_node(rel_node, shape="rectangle", style="filled")

                qt.add_edge(new_relation_root, rel_node)

            for fragment in fragments[2:]:
                rel_node = RelationNode(fragment.name)
                rel_node.is_localized = True
                rel_node.site_id = syscat_allocation.where(fragment=fragment.id)[0].site
                qt.add_node(rel_node, shape="rectangle", style="filled")

                new_join_node = get_node()
                qt.add_edge(new_join_node, new_relation_root)
                qt.add_edge(new_join_node, rel_node)

                new_relation_root = new_join_node

        for in_edge, _ in qt.in_edges(relation_node):
            qt.add_edge(in_edge, new_relation_root)
        qt.remove_node(relation_node)

    return qt


def build_naive_query_tree(sql_query: str):
    """
    builds the query tree given sql query
    """
    parsed_query = parse_sql(sql_query)
    print(parsed_query)
    query = parse_select(parsed_query)

    qt = nx.DiGraph()
    node_map = {"relations": {}, "conditions": []}
    relation_nodes = []

    pprint(query)

    for table in query.tables:
        relation = RelationNode(table)
        node_map["relations"][table] = relation
        qt.add_node(relation, shape="rectangle", style="filled")
        relation_nodes.append(relation)

    if query.where:
        for condition in query.where.conditions:
            columns_used = _find_columns_used_by_condition(condition, query.tables)
            node_map["conditions"].append((SelectionNode(condition), columns_used))

    # iterate through conditions using only 1 columns to make selections more selective
    node_map["conditions"] = sorted(node_map["conditions"], key=lambda x: len(x[1]))

    for condition, columns_used in node_map["conditions"]:
        debug_log("%s", "----" * 15)
        debug_log("Processing condition %s", condition)

        # condition on a single column
        if len(columns_used) == 1:
            relation = node_map["relations"].get(columns_used[0].split(".")[0])
            if relation in relation_nodes:
                debug_log("Adding edge from %s to %s", condition, relation)
                qt.add_edge(condition, get_relation_head(qt, relation))
            else:
                raise ValueError(f"Unknown relation accessed: {relation}")

            continue

        relation_list = list(
            set(
                map(
                    lambda col: node_map["relations"].get(col.split(".")[0]),
                    columns_used,
                )
            )
        )
        skip_update = False

        # check if all conditions use columns from tables
        for relation in relation_list:
            if relation not in relation_nodes:
                skip_update = True

        if skip_update:
            continue

        # check if selection needs to be converted to a join
        dst_nodes = set()
        for relation in relation_list:
            # skip self edge
            if hash(condition) == hash(get_relation_head(qt, relation)):
                continue
            dst_nodes.add(get_relation_head(qt, relation))

        if (
            type(condition.condition) is Condition
            and condition.condition.op == "="
            and len(dst_nodes) == 2
        ):
            condition = JoinNode(condition.condition)

            for relation in relation_list:
                edge_v = get_relation_head(qt, relation)
                debug_log("[c1] Adding edge from %s to %s", condition, edge_v)
                qt.add_edge(condition, edge_v)
        elif len(dst_nodes) == 1:
            for relation in relation_list:
                edge_v = get_relation_head(qt, relation)
                debug_log("[c2] Adding edge from %s to %s", condition, edge_v)
                qt.add_edge(condition, edge_v)
        else:
            join = JoinNode()

            for relation in relation_list:
                # skip self edge
                if hash(condition) == hash(get_relation_head(qt, relation)):
                    continue

                edge_v = get_relation_head(qt, relation)
                debug_log("[c31] Adding edge from %s to %s", join, edge_v)
                qt.add_edge(join, edge_v)

                debug_log("[c32] Adding edge from %s to %s", condition, join)
                qt.add_edge(condition, join)

    # all conditions have been processed
    # all relation_heads should be equal
    assert (
        len(set([hash(get_relation_head(qt, rel)) for rel in relation_nodes])) == 1
    ), "Disjoint graph?"

    # add project
    project_node = ProjectionNode(query.columns)
    qt.add_node(project_node, shape="note")
    qt.add_edge(project_node, get_relation_head(qt, relation_nodes[0]))

    return qt, node_map


if __name__ == "__main__":
    test_query = (
        "select G.`name`, M.`content` "
        "from `group` G, `message` M, `group_member` GM, `user` U "
        "where GM.`user` = 1 and U.`id` = 1 and GM.`group` = G.`id` and M.`sent_at` > U.`last_seen` and M.group = G.id"
    )

    qt, node_map = build_naive_query_tree(test_query)
    to_pydot(qt).write_png("qt.png")

    qt = localize_query_tree(qt, list(node_map["relations"].values()))
    to_pydot(qt).write_png("qt-loc.png")
