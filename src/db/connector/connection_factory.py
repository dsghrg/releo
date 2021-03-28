from db.connector.mssql import create_mssql_engine
from db.connector.postgres import create_postgres_engine


def create_engine(dbms):
    if dbms == 'mssql':
        return create_mssql_engine()
    if dbms == 'postgres':
        return create_postgres_engine()
    return
