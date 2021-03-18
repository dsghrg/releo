import csv
import functools
import hashlib
import itertools
import random
import time
import re

import numpy as np
import pandas as pd

# import psycopg2
# from dbconnection import postgres_connection

from sqlalchemy import create_engine
import urllib


def mssql_patcher(query):
    return (query.replace(";", "")) + "\nOPTION(FORCE ORDER);"


# preserve/force join order in postgres
def postgres_patcher(query):
    return "BEGIN;\nSET LOCAL join_collapse_limit = 1;\n" + query + "\nCOMMIT;"


def create_mssql_engine():
    params = urllib.parse.quote_plus(r'DRIVER={ODBC Driver 13 for SQL Server};SERVER=MSSQLBENCH;DATABASE=order_db_unif;Trusted_Connection=yes')
    conn_sql_server = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    return create_engine(conn_sql_server, fast_executemany=True)

def connect():
    conn = psycopg2.connect(**postgres_connection())
    cursor = conn.cursor()
    return conn, cursor


def reconnect(conn, cursor):
    conn.close()
    cursor.close()
    return connect()


if __name__ == '__main__':
    engine = create_mssql_engine()

    logs = []

    query1 = 'SELECT count(*) FROM product as prod JOIN subcategory as sub ON sub.id = prod.subcategory_id JOIN order_details as ord_det ON prod.id = ord_det.product_id JOIN [order] as ord ON ord_det.order_id = ord.id OPTION(FORCE ORDER)'
    query2 = 'SELECT count(*) FROM product as prod JOIN subcategory as sub ON sub.id = prod.subcategory_id JOIN order_details as ord_det ON prod.id = ord_det.product_id JOIN [order] as ord ON ord_det.order_id = ord.id'
    
    for i in range(1,1000):
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
    
    logs_df = pd.DataFrame(logs, columns = ['time_forced', 'time_non_forced'])
    logs_df.to_csv(r'logfiles_mssql_four_times_join.csv', index = False, sep = ';')


