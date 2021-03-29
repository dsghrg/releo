import random


class RandomQueryGenerator:

    def __init__(self, schema):
        self.schema = schema

    def generate(self):
        n_of_relations = round(random.random() * (len(self.schema) - 2) + 2)
        query = []
        gen(self.schema, query, [random.choice(list(self.schema.keys()))], [], n_of_relations)
        return query


def gen(schema, current_query, children, visited, n):
    next_relation = schema[random.choice(children)]
    children.remove(next_relation.name)
    visited.append(next_relation.name)
    children_to_add = [child for child in next_relation.tablename_to_join if child not in visited]
    current_query.append(next_relation.name)
    children = children + children_to_add
    if len(current_query) < n and len(children) > 0:
        gen(schema, current_query, children, visited, n)
