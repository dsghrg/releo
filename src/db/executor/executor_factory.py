import db.executor.postgres_lookup_executor as postgres_lookup
import db.executor.postgres_executor as postgres
import db.executor.mssql_executor as mssql
import db.executor.mssql_lookup_executor as mssql_lookup


def get_executor(name, cfg, engine, schema):
    if name == 'postgres-join-breakdown-json':
        return postgres.PostgresJoinBreakdownJson(cfg, engine, schema)
    if name == 'postgres-lookup-join-breakdown-json':
        return postgres_lookup.PostgresLookupJoinBreakdownJson(cfg, engine, schema)
    if name == 'mssql-join-breakdown-xml':
        return mssql.MssqlJoinBreakdownXml(cfg, engine, schema)
    if name == 'mssql-lookup-join-breakdown-xml':
        return mssql_lookup.MssqlLookupJoinBreakdownXml(cfg, engine, schema)
