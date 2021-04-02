import time


class PostgresJoinBreakdownJson():

    def __init__(self, cfg, engine, schema):
        self.cfg = cfg
        self.engine = engine
        self.schema = schema

    def execute(self, sql_query):
        start_time = time.time()
        rs = self.engine.execute(sql_query)
        elapes_time = 1000 * (time.time() - start_time)
        rec = rs.first()[0]
        costs = {'children': [], 'isRoot': True, 'cost': elapes_time}
        traverse_plan(rec[0]['Plan'], self.schema, costs)
        return costs


def traverse_plan(current, schema, parent):
    if 'Plans' in current:
        for plan in current['Plans']:
            parent = traverse_plan(plan, schema, parent)

    if 'Node Type' in current and 'join' in current['Node Type'].lower():
        condition = current['Hash Cond'].replace('(', '').replace(')', '')
        left, right = condition.split(" = ")
        left_table = find_table_name_by_alias(left.split(".")[0].replace('"', ''), schema)
        right_table = find_table_name_by_alias(right.split(".")[0].replace('"', ''), schema)
        cost = current['Total Cost']
        join = {'left': left_table.name, 'right': right_table.name, 'cost': cost, 'children': []}
        parent['children'].append(join)
        return join
    return parent


def find_table_name_by_alias(alias, schema):
    table = None
    for tablename in schema:
        table = schema[tablename] if table is None and schema[tablename].alias == alias else table
    return table
