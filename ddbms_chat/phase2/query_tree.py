import re
from typing import Dict, List, Union

import networkx as nx

from ddbms_chat.models.query import Condition, ConditionAnd, ConditionOr, SelectQuery
from ddbms_chat.models.syscat import Column, Fragment, Table
from ddbms_chat.models.tree import (
    JoinNode,
    ProjectionNode,
    RelationNode,
    SelectionNode,
    UnionNode,
)
from ddbms_chat.phase2.parser import (
    extract_names_from_func_col,
    parse_select,
    parse_sql,
)
from ddbms_chat.phase2.syscat import read_syscat
from ddbms_chat.phase2.utils import to_pydot
from ddbms_chat.utils import PyQL, debug_log

# from z3 import And, Or, Solver, sat, simplify


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


def _find_columns_used_in_query(qt: nx.DiGraph, nodes: List[RelationNode]):
    """
    optimization: from the lowest level, only select columns that are used in the query
    """
    columns_used = {}
    table_names = [node.name for node in nodes]
    for node in nodes:
        columns_used[node.name] = set()
        cur_node = node

        while True:
            incoming_edges = list(qt.in_edges(cur_node))
            if len(incoming_edges) == 0:
                break
            cur_node = incoming_edges[0][0]

            if type(cur_node) in [JoinNode, SelectionNode]:
                if not cur_node.condition:
                    continue
                cols = _find_columns_used_by_condition(cur_node.condition, table_names)
                for col in cols:
                    if col.startswith(node.name + "."):
                        columns_used[node.name].add(col.split(".")[1].lower())
            elif type(cur_node) is ProjectionNode:
                for col in cur_node.columns:
                    if "(" in col:
                        _, col = extract_names_from_func_col(col)
                    if col.startswith(node.name + "."):
                        columns_used[node.name].add(col.split(".")[1].lower())

    return columns_used


def get_relation_head(qt: nx.DiGraph, node):
    """
    get the root node for a given relation

    useful for adding nodes based on conditions
    """
    in_edges = list(qt.in_edges(node))

    if len(in_edges) == 0:
        return node

    return get_relation_head(qt, in_edges[0][0])


def build_naive_query_tree(sql_query: SelectQuery):
    """
    builds the query tree given sql query
    """
    qt = nx.DiGraph()
    node_map = {"relations": {}, "conditions": []}
    relation_nodes = []

    for table in sql_query.tables:
        relation = RelationNode(table)
        node_map["relations"][table] = relation
        qt.add_node(relation, shape="rectangle", style="filled")
        relation_nodes.append(relation)

    if sql_query.where:
        for condition in sql_query.where.conditions:
            columns_used = _find_columns_used_by_condition(condition, sql_query.tables)
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
    project_node = ProjectionNode(sql_query.columns)
    qt.add_node(project_node, shape="note")
    qt.add_edge(project_node, get_relation_head(qt, relation_nodes[0]))

    return qt, node_map


def construct_z3_from_condition(
    condition: Union[Condition, ConditionAnd, ConditionOr], column_name: str
):
    if type(condition) is Condition:
        if condition.lhs != column_name and condition.rhs != column_name:
            return None


def get_relevant_fragments_for_relation(
    fragments: PyQL, fragment_type: str, columns_used_in_query: Dict
) -> List:
    relation_name = re.sub(r"_\d+$", "", fragments[0].name)
    table: Table = syscat_tables.where(name=relation_name)[0]
    pkey: Column = syscat_columns.where(table=table.id, pk=1)[0]

    relevant_fragments = []

    if fragment_type == "V":
        for fragment in fragments:
            fragment_cols = set(map(lambda x: x.lower(), fragment.logic.split(",")))
            # vertical fragmentation will use primary key for joining
            query_cols = columns_used_in_query[relation_name] | {pkey.name}

            debug_log("Fragment cols: %s", fragment_cols)
            debug_log("Query cols: %s", query_cols)

            column_list = list(fragment_cols & query_cols)

            debug_log("Final list: %s", column_list)

            if len(column_list) > 1:
                relevant_fragments.append(fragment)

    if fragment_type == "H":
        # TODO: make optimization
        return fragments.items

    if fragment_type == "DH":
        # TODO: make optimization
        return fragments.items

    return relevant_fragments


def optimize_and_localize_query_tree(qt: nx.DiGraph, node_map: Dict):
    relations = list(node_map["relations"].values())
    columns_used_in_query = _find_columns_used_in_query(qt, relations)

    # get SelectionNodes directly attached to RelationNode
    relation_attached_selects = {}
    for relation_node in relations:
        relation_attached_selects[relation_node.name] = []

        parent_node = list(qt.in_edges(relation_node))[0][0]
        if type(parent_node) is SelectionNode:
            relation_attached_selects[relation_node.name].append(parent_node)

    # localize query tree
    qt = localize_query_tree(qt, relations, columns_used_in_query)
    to_pydot(qt).write_png("qt-loc.png")

    relation_nodes = []

    for node, out_degree in qt.out_degree():
        if out_degree != 0:
            continue
        relation_nodes.append(node)

    # optimization #1
    # add ProjectionNode to select only columns present in query
    for relation_node in relation_nodes:
        relation_node: RelationNode

        fragment_name = relation_node.name
        relation_name = re.sub(r"_\d+$", "", fragment_name)

        debug_log("%s", "----" * 15)
        debug_log("Trying to find relation %s", relation_name)
        table: Table = syscat_tables.where(name=relation_name)[0]
        debug_log("Trying to find fragment %s", fragment_name)
        fragment: Fragment = syscat_fragments.where(table=table.id, name=fragment_name)[
            0
        ]
        pkey: Column = syscat_columns.where(table=table.id, pk=1)[0]

        if table.fragment_type == "V":
            fragment_cols = set(map(lambda x: x.lower(), fragment.logic.split(",")))
            # vertical fragmentation will use primary key for joining
            query_cols = columns_used_in_query[relation_name] | {pkey.name}

            debug_log("Fragment cols: %s", fragment_cols)
            debug_log("Query cols: %s", query_cols)

            column_list = list(fragment_cols & query_cols)

            debug_log("Final list: %s", column_list)
        else:
            fragment_cols = set(
                [col.name for col in syscat_columns.where(table=table.id)]
            )
            column_list = list(columns_used_in_query[relation_name])

        # if its 1, it is just the primary key
        if len(column_list) == 1:
            qt.remove_node(relation_node)
            continue

        # if its equal to the column list, all are being used
        # no need to add ProjectionNode
        if len(column_list) == len(fragment_cols):
            continue

        simplified_project_node = ProjectionNode(column_list)
        qt.add_node(simplified_project_node, shape="note")

        in_node = list(qt.in_edges(relation_node))[0][0]
        qt.remove_edge(in_node, relation_node)
        qt.add_edge(in_node, simplified_project_node)
        qt.add_edge(simplified_project_node, relation_node)

    # recalculate relation_nodes; some of the fragments were removed
    relation_nodes = []
    nodes_to_be_removed = []

    for node, out_degree in qt.out_degree():
        if out_degree != 0:
            continue
        if type(node) is RelationNode:
            relation_nodes.append(node)
        else:
            nodes_to_be_removed.append(node)

    for node in nodes_to_be_removed:
        qt.remove_node(node)

    # optimization #2
    # push selects directly to fragments
    for relation_node in relation_nodes:
        relation_node: RelationNode

        fragment_name = relation_node.name
        relation_name = re.sub(r"_\d+$", "", fragment_name)

        table: Table = syscat_tables.where(name=relation_name)[0]
        fragment: Fragment = syscat_fragments.where(table=table.id, name=fragment_name)[
            0
        ]

        # relation_attached_selects

    # recalculate relation_nodes; some of the fragments were removed
    relation_nodes = []
    nodes_to_be_removed = []

    for node, out_degree in qt.out_degree():
        if out_degree != 0:
            continue
        if type(node) is RelationNode:
            relation_nodes.append(node)
        else:
            nodes_to_be_removed.append(node)

    for node in nodes_to_be_removed:
        qt.remove_node(node)

    # iteratively remove 0/single childed JoinNodes
    while True:
        nodes_to_be_removed = []
        for node in qt:
            if type(node) is JoinNode:
                out_edges = list(qt.out_edges(node))
                n_out_edges = len(out_edges)
                if n_out_edges == 2:
                    continue
                elif n_out_edges == 0:
                    # these should be cleared in the previous relation_nodes calculation
                    qt.remove_node(node)
                elif n_out_edges == 1:
                    in_node = list(qt.in_edges(node))[0][0]
                    out_node = out_edges[0][1]
                    qt.add_edge(in_node, out_node)
                    nodes_to_be_removed.append(node)

        if len(nodes_to_be_removed) == 0:
            break

        for node in nodes_to_be_removed:
            qt.remove_node(node)

    return qt


def localize_query_tree(
    qt: nx.DiGraph, nodes: List[RelationNode], columns_used_in_query: Dict
):
    """
    localize all relations
    """
    for relation_node in nodes:
        tables = syscat_tables.where(name=relation_node.name)

        assert len(tables) == 1, f"Table {relation_node.name} not found"
        table: Table = tables[0]

        pkey: Column = syscat_columns.where(table=table.id, pk=1)[0]

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
                lambda f1, f2: JoinNode(Condition(f1, "=", f2))
                if table.fragment_type == "V"
                else UnionNode()
            )

            fragments = syscat_fragments.where(table=table.id)

            new_relation_root = get_node(
                f"{fragments[0].name}.{pkey.name}", f"{fragments[1].name}.{pkey.name}"
            )

            relevant_fragments = get_relevant_fragments_for_relation(
                fragments, table.fragment_type, columns_used_in_query
            )

            for fragment in relevant_fragments[:2]:
                rel_node = RelationNode(fragment.name)
                rel_node.is_localized = True
                rel_node.site_id = syscat_allocation.where(fragment=fragment.id)[0].site
                qt.add_node(rel_node, shape="rectangle", style="filled")

                qt.add_edge(new_relation_root, rel_node)

            for fragment in relevant_fragments[2:]:
                rel_node = RelationNode(fragment.name)
                rel_node.is_localized = True
                rel_node.site_id = syscat_allocation.where(fragment=fragment.id)[0].site
                qt.add_node(rel_node, shape="rectangle", style="filled")

                new_join_node = get_node(
                    f"?.{pkey.name}", f"{fragment.name}.{pkey.name}"
                )
                qt.add_edge(new_join_node, new_relation_root)
                qt.add_edge(new_join_node, rel_node)

                new_relation_root = new_join_node

        for in_edge, _ in qt.in_edges(relation_node):
            qt.add_edge(in_edge, new_relation_root)
        qt.remove_node(relation_node)

    return qt


def build_query_tree(select_query: SelectQuery) -> nx.DiGraph:
    qt, node_map = build_naive_query_tree(select_query)
    to_pydot(qt).write_png("qt.png")

    qt = optimize_and_localize_query_tree(qt, node_map)
    to_pydot(qt).write_png("qt-opt.png")

    return qt


if __name__ == "__main__":
    # test_query = (
    #     "select G.`name` "
    #     "from `group` G, `group_member` GM "
    #     "where GM.`user` = 1 and G.`id` = GM.`group`"
    # )
    # test_query = "select * from `group` where `created_by` = 1"
    # test_query = (
    #     "select U.`name`, M.`sent_at`, M.`content` "
    #     "from `message` M, `user` U "
    #     "where M.`group` = 1 and M.`author` = U.id;"
    # )
    test_query = (
        "select G.`name`, M.`content` "
        "from `group` G, `message` M, `group_member` GM, `user` U "
        "where GM.`user` = 1 and U.`id` = 1 and GM.`group` = G.`id` and M.`sent_at` > U.`last_seen` and M.group = G.id"
    )

    parsed_query = parse_sql(test_query)
    select_query = parse_select(parsed_query)

    build_query_tree(select_query)
