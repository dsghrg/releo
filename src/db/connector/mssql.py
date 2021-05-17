import urllib

from sqlalchemy import create_engine


def create_mssql_engine(cfg):
    params = urllib.parse.quote_plus(
        r'DRIVER={'+cfg['driver']+'};SERVER=' + cfg['host'] + ',' + str(cfg['port']) + ';DATABASE=' + cfg[
            'db'] + ';PWD=' + cfg['password'] + ';UID=' + cfg['username'])
    conn_sql_server = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    return create_engine(conn_sql_server, fast_executemany=True)
