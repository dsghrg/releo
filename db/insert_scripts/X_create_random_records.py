import math
import random
import string
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.schema import DropTable
from sqlalchemy.ext.compiler import compiles
import urllib
from dbconnection import postgres_connection

# from dbconnection import postgres_connection

size_customer = int(500000 * 1)
size_deliverer = 4
size_discount = 50 * 1
size_category = 1000 * 1
size_subcategory = 5000 * 1
size_product = 100000 * 1
size_order = 2000000 * 1
size_order_details = 6000000 * 1


@compiles(DropTable, "postgresql")
def _compile_drop_table(element, compiler, **kwargs):
    return compiler.visit_drop_table(element) + " CASCADE"


def random_string_generator(str_size, allowed_chars):
    return ''.join(random.choice(allowed_chars) for x in range(str_size))


print('creating pandas tables...')
# Customer table
customer_lst = [[k, random_string_generator(30, string.ascii_letters)] for k in range(1, size_customer + 1)]
customer_df = pd.DataFrame(customer_lst, columns=['id', 'name'])

# deliverer table
deliverer_lst = [[1, 'DHL'], [2, 'IPS'], [3, 'FEDEX'], [4, 'UPS']]
deliverer_df = pd.DataFrame(deliverer_lst, columns=['id', 'name'])

# discount table
discount_lst = [[k, random_string_generator(30, string.ascii_letters), random.uniform(0, 1)] for k in
                range(1, size_discount + 1)]
discount_df = pd.DataFrame(discount_lst, columns=['id', 'code', 'discount'])

# category table
category_lst = [[k, random_string_generator(30, string.ascii_letters)] for k in range(1, size_category + 1)]
category_df = pd.DataFrame(category_lst, columns=['id', 'category_name'])

# subcategory table
subcategory_lst = [[k, random_string_generator(30, string.ascii_letters), random.randint(1, size_category)] for k in
                   range(1, size_subcategory + 1)]
subcategory_df = pd.DataFrame(subcategory_lst, columns=['id', 'subcategory_name', 'category_id'])

# order table
order_lst = [[k,
              random.randint(1, size_customer),
              random.uniform(0, 1) * 1480 + 20,
              random.randint(1, size_deliverer),
              random.randint(1, size_discount)] for k in range(1, size_order + 1)]
order_df = pd.DataFrame(order_lst, columns=['id', 'customer_id', 'amount', 'deliverer_id', 'discount_id'])

# product table
product_lst = [[k,
                random_string_generator(30, string.ascii_letters),
                math.floor(random.uniform(0, 1) * 480 + 20),
                random.randint(1, size_subcategory)] for k in range(1, size_product + 1)]
product_df = pd.DataFrame(product_lst, columns=['id', 'name', 'price', 'subcategory_id'])

# order_details table
order_details_lst = [[k,
                      random.randint(1, size_order),
                      random.randint(1, size_product),
                      math.floor(random.uniform(0, 1) * 480 + 20),
                      random.randint(1, 20)] for k in range(1, size_order_details + 1)]
order_details_df = pd.DataFrame(order_details_lst, columns=['id', 'order_id', 'product_id', 'price', 'quantity'])

print('connecting to db...')
# creds = postgres_connection()
# engine = create_engine(
#     'postgresql://{}:{}@{}:5432/{}'.format(creds['user'], creds['password'],
#                                            creds['host'], creds['database']))

# SERVERNAME = 'mssqlbench.cloudlab.zhaw.ch'
# SERVER = 'MSSQLBENCH'
# DATABASE = 'random_database_small'

# print('connected!')
#
# params_log = urllib.parse.quote_plus(r'DRIVER={ODBC Driver 13 for SQL Server};SERVER=MSSQLBENCH;DATABASE=random_database_small;Trusted_Connection=yes')
# conn_sql_server = 'mssql+pyodbc:///?odbc_connect={}'.format(params_log)
#
# engine = create_engine(postgres_engine, fast_executemany=True)


# SERVERNAME = 'localhost'
# SERVER = 'MSSQLSERVER'
# DATABASE = 'shop_db'
#
# params_log = urllib.parse.quote_plus(r'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=shop_db;Trusted_Connection=yes')
# conn_sql_server = 'mssql+pyodbc:///?odbc_connect={}'.format(params_log)
# engine = create_engine(conn_sql_server, fast_executemany = True)

params = urllib.parse.quote_plus(
    r'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,5432;DATABASE=shop_db;PWD=zhaw2021_mssql;UID=SA')
conn_sql_server = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
engine = create_engine(conn_sql_server, fast_executemany=True)
print('connected')

customer_df.to_sql("customer", con=engine, schema='public', if_exists='replace', index=False)
print("Customer done")
deliverer_df.to_sql("deliverer", con=engine, schema='public', if_exists='replace', index=False)
print("deliverer done")
discount_df.to_sql("discount", con=engine, schema='public', if_exists='replace', index=False)
print("discount done")
category_df.to_sql("category", con=engine, schema='public', if_exists='replace', index=False)
print("category done")
subcategory_df.to_sql("subcategory", con=engine, schema='public', if_exists='replace', index=False)
print("subcategory done")
order_df.to_sql("order", con=engine, schema='public', if_exists='replace', index=False)
print("order done")
product_df.to_sql("product", con=engine, schema='public', if_exists='replace', index=False)
print("product done")
order_details_df.to_sql("order_details", con=engine, schema='public', if_exists='replace', index=False)
print("order_details done")
