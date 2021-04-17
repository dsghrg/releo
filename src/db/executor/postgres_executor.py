import time
import os


class PostgresJoinBreakdownJson():

    def __init__(self, cfg, engine, schema):
        self.cfg = cfg
        self.engine = engine
        self.schema = schema
        self.global_log_path = cfg['global']['log-path']
        self.local_log_path = self.global_log_path + '/executor'
        self.system_context = cfg['global']['context']
        os.makedirs(self.local_log_path)

    def execute(self, sql_query):
        if 'current_episode' in self._get_rl_context():
            print('executing stmt for episode: ' + str(self._get_rl_context()['current_episode']))
        start_time = time.time()
        rs = self.engine.execute(sql_query)
        elapes_time = 1000 * (time.time() - start_time)
        rec = rs.first()[0]
        costs = {'children': [], 'isRoot': True, 'cost': elapes_time}
        _traverse(rec[0]['Plan'], self.schema, costs)
        return costs

    def _get_rl_context(self):
        return self.system_context['rl-agent'] if 'rl-agent' in self.system_context else {}


def get_inclusive(node):
    if node is not None and 'Actual Total Time' in node:
        return node['Actual Total Time']
    else:
        return 0


def _traverse(current, schema, parent_join):
    if 'Plans' in current:
        for plan in current['Plans']:
            parent_join = _traverse(plan, schema, parent_join)

    if 'Node Type' in current and 'join' in current['Node Type'].lower():
        # elapsed time so far for total query
        inclusive_children_sum = sum([get_inclusive(child) for child in current['Plans'] if 'Plans' in current] + [0])
        current_elapsed_time = current['Actual Total Time']
        # https://github.com/postgres/pgadmin4/blob/c1ba645dceed5c9551a5f408e37a14d1041ee598/web/pgadmin/misc/static/explain/js/explain.js#L617
        # elapsed_time_join = current_elapsed_time - inclusive_children_sum
        elapsed_time_join = current_elapsed_time
        condition = current['Hash Cond'].replace('(', '').replace(')', '')
        left, right = condition.split(" = ")
        left_table = find_table_name_by_alias(left.split(".")[0].replace('"', ''), schema)
        right_table = find_table_name_by_alias(right.split(".")[0].replace('"', ''), schema)
        join = {'left': left_table.name, 'right': right_table.name, 'cost': elapsed_time_join, 'children': []}
        parent_join['children'].append(join)
        return join
    return parent_join


def find_table_name_by_alias(alias, schema):
    table = None
    for tablename in schema:
        table = schema[tablename] if table is None and schema[tablename].alias == alias else table
    return table
