from collections import defaultdict
from dataclasses import asdict
from typing import List

import networkx as nx

from ddbms_chat.models.syscat import Site
from ddbms_chat.models.tree import (
    JoinNode,
    ProjectionNode,
    RelationNode,
    SelectionNode,
    UnionNode,
)
from ddbms_chat.phase3.utils import (
    condition_object_to_dict,
    get_component_relations,
    send_request_to_site,
)
from ddbms_chat.utils import DBConnection, debug_log


def build_relation_name(
    query_id: str, plan_idx: int, component_relations: List[str]
) -> str:
    return f"{query_id}_{plan_idx}-{'-'.join(set(sorted(component_relations)))}"


def get_executable_nodes(qt: nx.DiGraph):
    relation_nodes = defaultdict(lambda: [])

    for node, out_degree in qt.out_degree():
        if out_degree == 0:
            parent = list(qt.predecessors(node))[0]
            relation_nodes[parent].append(node)

    for parent in relation_nodes:
        n_children = len(relation_nodes[parent])
        if n_children == 2 and type(parent) in [UnionNode, JoinNode]:
            return relation_nodes[parent], parent

    for parent in relation_nodes:
        n_children = len(relation_nodes[parent])
        if n_children == 1 and type(parent) in [SelectionNode, ProjectionNode]:
            return relation_nodes[parent], parent

    return None, None


def plan_execution(qt: nx.DiGraph, query_id: str):
    plan = []

    while True:
        try:
            actionable_nodes, parent = get_executable_nodes(qt)
        except:
            actionable_nodes, parent = None, None

        if actionable_nodes is None:
            raise ValueError("No actionable node found in query tree")
        if parent is None:
            raise ValueError("Parent node doesn't exist")

        if len(actionable_nodes) == 2:
            component_rels = get_component_relations(actionable_nodes[1].name)
            old_node_name = build_relation_name(query_id, len(plan), component_rels)
            # TODO: use semijoin optimization for joins
            # if type(parent) is JoinNode
            if actionable_nodes[0].site_id != actionable_nodes[1].site_id:
                plan.append(
                    (
                        actionable_nodes[0].site_id,
                        "fetch",
                        (actionable_nodes[1].name, actionable_nodes[1].site_id),
                        old_node_name,
                    )
                )
            else:
                old_node_name = actionable_nodes[1].name

            component_rels = get_component_relations(actionable_nodes[1].name)
            node_name = build_relation_name(query_id, len(plan), component_rels)
            if type(parent) is UnionNode:
                plan.append(
                    (
                        actionable_nodes[0].site_id,
                        "union",
                        (actionable_nodes[0].name, old_node_name),
                        node_name,
                    )
                )
            elif type(parent) is JoinNode:
                component_rels = get_component_relations(
                    actionable_nodes[0].name
                ) + get_component_relations(actionable_nodes[1].name)
                node_name = build_relation_name(query_id, len(plan), component_rels)
                plan.append(
                    (
                        actionable_nodes[0].site_id,
                        "join",
                        (
                            actionable_nodes[0].name,
                            old_node_name,
                            parent.condition,
                        ),
                        node_name,
                    )
                )
            else:
                raise ValueError(f"Didn't expect node of type {type(parent)}")

            qt.remove_node(actionable_nodes[0])
            qt.remove_node(actionable_nodes[1])
            grandparent = list(qt.predecessors(parent))[0]
            qt.remove_node(parent)
            rel_node = RelationNode(node_name)
            rel_node.is_localized = True
            rel_node.site_id = actionable_nodes[0].site_id
            qt.add_node(rel_node, shape="rectangle", style="filled")
            qt.add_edge(grandparent, rel_node)
        elif len(actionable_nodes) == 1:
            node_name = f"{query_id}_{len(plan)}"
            component_rels = get_component_relations(actionable_nodes[0].name)
            node_name = build_relation_name(query_id, len(plan), component_rels)

            if type(parent) is SelectionNode:
                plan.append(
                    (
                        actionable_nodes[0].site_id,
                        "select",
                        (actionable_nodes[0].name, parent.condition),
                        node_name,
                    )
                )
            elif type(parent) is ProjectionNode:
                plan.append(
                    (
                        actionable_nodes[0].site_id,
                        "project",
                        (actionable_nodes[0].name, parent.columns),
                        node_name,
                    )
                )
            else:
                raise ValueError(f"Didn't expect node of type {type(parent)}")

            qt.remove_node(actionable_nodes[0])
            grandparent_or_none = list(qt.predecessors(parent))
            if len(grandparent_or_none) == 0:
                break
            else:
                grandparent = grandparent_or_none[0]
            qt.remove_node(parent)
            rel_node = RelationNode(node_name)
            rel_node.is_localized = True
            rel_node.site_id = actionable_nodes[0].site_id
            qt.add_node(rel_node, shape="rectangle", style="filled")
            qt.add_edge(grandparent, rel_node)

    debug_log("%s", plan)
    return plan


def execute_plan(plan: List, query_id: str, current_site: Site):
    sites_involved = set()

    for i, (site_id, action, metadata, new_relation_name) in enumerate(plan):
        sites_involved.add(site_id)
        payload = {"target_relation_name": new_relation_name}
        match action:
            case "fetch":
                payload |= {"relation_name": metadata[0], "site_id": metadata[1]}
            case "union":
                payload |= {
                    "relation1_name": metadata[0],
                    "relation2_name": metadata[1],
                }
            case "join":
                payload |= {
                    "relation1_name": metadata[0],
                    "relation2_name": metadata[1],
                    "join_condition": condition_object_to_dict(metadata[2]),
                }
            case "select":
                payload |= {
                    "relation_name": metadata[0],
                    "select_condition": condition_object_to_dict(metadata[1]),
                }
            case "project":
                payload |= {
                    "relation_name": metadata[0],
                    "project_columns": metadata[1],
                }
        r = send_request_to_site(site_id, "post", f"/exec/{action}", json=payload)
        if not r.ok:
            raise ValueError(f"Failed to execute step {i + 1} of plan")

    r = send_request_to_site(
        current_site.id,
        "post",
        "/exec/fetch",
        json={
            "relation_name": plan[-1][-1],
            "site_id": plan[-1][0],
            "target_relation_name": f"{query_id}-result",
        },
    )
    if not r.ok:
        raise ValueError("Failed to retrieve results")

    with DBConnection(current_site) as cursor:
        cursor.execute(f"select * from `{query_id}-result`")
        rows = cursor.fetchall()
        print(rows)

    for site_id in sites_involved:
        r = send_request_to_site(site_id, "post", f"/cleanup/{query_id}")
        if not r.ok:
            raise ValueError(f"Cleanup failed at site {site_id}")
