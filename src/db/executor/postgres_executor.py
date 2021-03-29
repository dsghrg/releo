def execute_sql_query(db_engine, schema, sql_query):
    rs = db_engine.execute(sql_query)
    rec = rs.first()[0]
    costs = {}
    traverse_plan(rec[0]['Plan'], costs, schema)
    return costs


def traverse_plan(current, costs, schema):
    if 'join' in current['Node Type'].lower():
        condition = current['Hash Cond'].replace('(', '').replace(')', '')
        left, right = condition.split(" = ")
        left_table = find_table_name_by_alias(left.split(".")[0].replace('"', ''), schema)
        right_table = find_table_name_by_alias(right.split(".")[0].replace('"', ''), schema)

        cost = current['Total Cost']
        costs[left_table.name + '->' + right_table.name] = cost
    if 'Plans' in current:
        for plan in current['Plans']:
            traverse_plan(plan, costs, schema)


def find_table_name_by_alias(alias, schema):
    table = None
    for tablename in schema:
        table = schema[tablename] if table is None and schema[tablename].alias == alias else table
    return table
