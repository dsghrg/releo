import re

from src.db.generate_sql import generate_sql_query


def get_sql_generator(name):
    if name == 'mssql':
        return lambda schema, order_vector: generate_sql_query(schema, order_vector, mssql_patcher)
    else:
        return lambda schema, order_vector: generate_sql_query(schema, order_vector, postgres_patcher)


def mssql_patcher(query):
    query = re.sub(r'\border\b', '[order]', query)
    return query.replace(";", "\nOPTION(FORCE ORDER);")


# preserve/force join order in postgres
def postgres_patcher(query):
    return re.sub(r'\border\b', '"order"', query)
