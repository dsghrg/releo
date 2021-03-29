import urllib

from sqlalchemy import create_engine


def create_mssql_engine():
    params = urllib.parse.quote_plus(
        r'DRIVER={ODBC Driver 13 for SQL Server};SERVER=MSSQLBENCH;DATABASE=order_db_unif;Trusted_Connection=yes')
    conn_sql_server = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    return create_engine(conn_sql_server, fast_executemany=True)
