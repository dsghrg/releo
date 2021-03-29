import urllib

from sqlalchemy import create_engine


def create_mssql_engine(cfg):
    params = urllib.parse.quote_plus(
        r'DRIVER={ODBC Driver 13 for SQL Server};SERVER=MSSQLBENCH;DATABASE={};Trusted_Connection=yes'.format(
            cfg['db']))
    conn_sql_server = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    return create_engine(conn_sql_server, fast_executemany=True)
