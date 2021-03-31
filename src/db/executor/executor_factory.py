import db.executor.postgres_executor as postgres
import db.executor.mssql_executor as mssql


def get_executor(name, cfg, engine, schema):
    if name == 'postgres-join-breakdown-json':
        return postgres.PostgresJoinBreakdownJson(cfg, engine, schema)
    if name == 'mssql-join-breakdown-xml':
        return mssql.MssqlJoinBreakdownXml(cfg, engine, schema)
