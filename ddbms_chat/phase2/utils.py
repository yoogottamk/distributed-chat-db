import networkx as nx


# modified version of nx.nx_pydot.to_pydot
def to_pydot(N):
    """Returns a pydot graph from a NetworkX graph N.

    Parameters
    ----------
    N : NetworkX graph
      A graph created with NetworkX

    Examples
    --------
    >>> K5 = nx.complete_graph(5)
    >>> P = nx.nx_pydot.to_pydot(K5)

    Notes
    -----

    """
    import pydot

    # set Graphviz graph type
    if N.is_directed():
        graph_type = "digraph"
    else:
        graph_type = "graph"
    strict = nx.number_of_selfloops(N) == 0 and not N.is_multigraph()

    name = N.name
    graph_defaults = N.graph.get("graph", {})
    if name == "":
        P = pydot.Dot("", graph_type=graph_type, strict=strict, **graph_defaults)
    else:
        P = pydot.Dot(
            f'"{name}"', graph_type=graph_type, strict=strict, **graph_defaults
        )
    try:
        P.set_node_defaults(**N.graph["node"])
    except KeyError:
        pass
    try:
        P.set_edge_defaults(**N.graph["edge"])
    except KeyError:
        pass

    for n, nodedata in N.nodes(data=True):
        str_nodedata = {k: str(v) for k, v in nodedata.items()}
        p = pydot.Node(str(n) + str(hash(n)), label=str(n), **str_nodedata)
        P.add_node(p)

    if N.is_multigraph():
        for u, v, key, edgedata in N.edges(data=True, keys=True):
            str_edgedata = {k: str(v) for k, v in edgedata.items() if k != "key"}
            edge = pydot.Edge(str(u), str(v), key=str(key), **str_edgedata)
            P.add_edge(edge)

    else:
        for u, v, edgedata in N.edges(data=True):
            str_edgedata = {k: str(v) for k, v in edgedata.items()}
            edge = pydot.Edge(
                str(u) + str(hash(u)), str(v) + str(hash(v)), **str_edgedata
            )
            P.add_edge(edge)
    return P
