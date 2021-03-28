import matplotlib.pyplot as plt
import networkx as nx
from sqlalchemy import inspect


class Join:

    def __init__(self, source, source_key, destination, destination_key):
        self.source = source
        self.source_key = source_key
        self.destination = destination
        self.destination_key = destination_key


class Table:

    def __init__(self, name, alias, tablename_to_join):
        self.name = name
        self.alias = alias
        self.tablename_to_join = tablename_to_join


def create_schema(engine):
    schema = {}
    inspector = inspect(engine)
    for tablename in inspector.get_table_names():
        table = Table(tablename, tablename, {})
        schema[tablename] = table

    for tablename in inspector.get_table_names():
        table = schema[tablename]
        foreign_keys = inspector.get_foreign_keys(tablename)
        for fk in foreign_keys:
            join = Join(table, fk['constrained_columns'][0], schema[fk['referred_table']], fk['referred_columns'][0])
            # symmetry
            join_back = Join(schema[fk['referred_table']], fk['referred_columns'][0], table,
                             fk['constrained_columns'][0])
            table.tablename_to_join[join.destination.name] = join
            schema[fk['referred_table']].tablename_to_join[join_back.destination.name] = join_back
    return schema


def plot_schema(schema):
    graph = nx.Graph()
    edges = []
    for tablename in schema.keys():
        new_edges = [(tablename, dest_name) for dest_name in schema[tablename].tablename_to_join.keys()]
        edges = edges + new_edges
    graph.add_edges_from(edges)
    graph = nx.relabel_nodes(graph, {i: name for i, name in enumerate(schema)})
    nx.draw(graph, with_labels=True, node_size=1500, node_color='lightgrey')
    plt.show()
