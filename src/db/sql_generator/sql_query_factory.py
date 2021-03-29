import re

from db.sql_generator.generate_sql import generate_sql_query


def get_sql_generator(name, cfg):
    if name == 'mssql-force-order':
        return lambda schema, order_vector: generate_sql_query(schema, order_vector, mssql_patcher)
    if name == 'postgres-force-order':
        return lambda schema, order_vector: generate_sql_query(schema, order_vector, postgres_patcher)


def mssql_patcher(query):
    query = re.sub(r'\border\b', '[order]', query)
    query = query.replace(";", "\nOPTION(FORCE ORDER);")
    return "SET STATISTICS XML ON;\n" + query


# preserve/force join order in postgres
def postgres_patcher(query):
    query = "EXPLAIN (ANALYZE, FORMAT JSON)\n" + query
    return re.sub(r'\border\b', '"order"', query)
