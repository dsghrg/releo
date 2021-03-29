from db.executor.postgres_executor import execute_sql_query
import db.executor.mssql_executor as mssql


def get_executor(name, cfg):
    if name == 'postgres-join-breakdown-json':
        return lambda engine, schema, sql: execute_sql_query(engine, schema, sql)
    if name == 'mssql-join-breakdown-xml':
        return lambda engine, schema, sql: mssql.execute_sql_query(engine, schema, sql)
