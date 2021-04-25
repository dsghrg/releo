import random


class RandomQueryGenerator:

    def __init__(self, schema, cfg):
        self.seed = cfg['seed'] if 'seed' in cfg else 1
        self.min = cfg['min-tables'] if 'min-tables' in cfg else 2
        self.max = cfg['max-tables'] if 'max-tables' in cfg else len(schema)
        self.myrandom = random.Random(self.seed)
        self.schema = schema

    def notify(self, info):
        """
        adds the possibility to interact with the generator
        think of providing additional infos to generate more interesting queries
        info param processing obviously needs to match with what the configured environment provides
        """
        pass

    def generate(self):
        n_of_relations = round(self.myrandom.random() * (self.max - self.min) + self.min)
        query = []
        self._gen(self.schema, query, [self.myrandom.choice(list(self.schema.keys()))], [], n_of_relations)
        return query

    def _gen(self, schema, current_query, children, visited, n):
        next_relation = schema[self.myrandom.choice(children)]
        children.remove(next_relation.name)
        visited.append(next_relation.name)
        children_to_add = [child for child in next_relation.tablename_to_join if child not in visited]
        current_query.append(next_relation.name)
        children = children + children_to_add
        if len(current_query) < n and len(children) > 0:
            self._gen(schema, current_query, children, visited, n)
