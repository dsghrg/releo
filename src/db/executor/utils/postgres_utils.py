def _get_inclusive(node):
    if node is not None and 'Actual Total Time' in node:
        return node['Actual Total Time']
    else:
        return 0


def _find_table_name_by_alias(alias, schema):
    table = None
    for tablename in schema:
        table = schema[tablename] if table is None and schema[tablename].alias == alias else table
    return table


def traverse(current, schema, parent_join):
    if 'Plans' in current:
        for plan in current['Plans']:
            parent_join = traverse(plan, schema, parent_join)

    if 'Node Type' in current and 'join' in current['Node Type'].lower():
        # elapsed time so far for total query
        inclusive_children_sum = sum([_get_inclusive(child) for child in current['Plans'] if 'Plans' in current] + [0])
        current_elapsed_time = current['Actual Total Time']
        # https://github.com/postgres/pgadmin4/blob/c1ba645dceed5c9551a5f408e37a14d1041ee598/web/pgadmin/misc/static/explain/js/explain.js#L617
        elapsed_time_join = current_elapsed_time - inclusive_children_sum
        # elapsed_time_join = current_elapsed_time
        condition = current['Hash Cond'] if 'Hash Cond' in current else current['Merge Cond']
        if condition is None:
            condition = current['Index Cond']
        condition = condition.replace('(', '').replace(')', '')
        left, right = condition.split(" = ")
        left_table = _find_table_name_by_alias(left.split(".")[0].replace('"', ''), schema)
        right_table = _find_table_name_by_alias(right.split(".")[0].replace('"', ''), schema)
        join = {'left': left_table.name, 'right': right_table.name, 'cost': elapsed_time_join, 'children': []}
        parent_join['children'].append(join)
        return join
    return parent_join
