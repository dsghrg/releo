import time
import urllib

import pandas as pd
# import psycopg2
from dbconnection import postgres_connection
from sqlalchemy import create_engine


def create_mssql_engine():
    params = urllib.parse.quote_plus(
        r'DRIVER={ODBC Driver 13 for SQL Server};SERVER=MSSQLBENCH;DATABASE=order_db_unif;Trusted_Connection=yes')
    conn_sql_server = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    return create_engine(conn_sql_server, fast_executemany=True)


def create_postgres_engine():
    creds = postgres_connection()
    return create_engine(
        'postgresql://{}:{}@{}:5432/{}'.format(creds['user'], creds['password'],
                                               creds['host'], creds['database']))


def mssql_queries():
    forced = 'SELECT count(*) FROM product as prod JOIN subcategory as sub ON sub.id = prod.subcategory_id JOIN order_details as ord_det ON prod.id = ord_det.product_id JOIN [order] as ord ON ord_det.order_id = ord.id OPTION(FORCE ORDER)'
    unforced = 'SELECT count(*) FROM product as prod JOIN subcategory as sub ON sub.id = prod.subcategory_id JOIN order_details as ord_det ON prod.id = ord_det.product_id JOIN [order] as ord ON ord_det.order_id = ord.id'
    return forced, unforced


def postgres_queries():
    forced = 'BEGIN;\nSET LOCAL join_collapse_limit = 1;\nSELECT count(*) FROM product as prod JOIN subcategory as sub ON sub.id = prod.subcategory_id JOIN order_details as ord_det ON prod.id = ord_det.product_id JOIN "order" as ord ON ord_det.order_id = ord.id;\nCOMMIT;'
    unforced = 'BEGIN;\nSET LOCAL join_collapse_limit = 8;\nSELECT count(*) FROM product as prod JOIN subcategory as sub ON sub.id = prod.subcategory_id JOIN order_details as ord_det ON prod.id = ord_det.product_id JOIN "order" as ord ON ord_det.order_id = ord.id;\nCOMMIT;'
    return forced, unforced


if __name__ == '__main__':
    engine = create_postgres_engine()

    logs = []

    query1, query2 = postgres_queries()

    for i in range(1, 1000):
        print(i)
        try:
            start_time = time.time()
            engine.execute(query1)
            end_time = time.time()
            elapsed_time_forced = end_time - start_time

            # Execute without order forcing
            start_time = time.time()
            engine.execute(query2)
            end_time = time.time()
            elapsed_time_non_forced = end_time - start_time

            logs.append([elapsed_time_forced, elapsed_time_non_forced])

        except Exception as ex:
            print(ex)
            engine = create_mssql_engine()

    logs_df = pd.DataFrame(logs, columns=['time_forced', 'time_non_forced'])
    logs_df.to_csv(r'logfiles_mssql_four_times_join.csv', index=False, sep=';')
