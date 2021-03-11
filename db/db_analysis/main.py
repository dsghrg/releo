import functools
import itertools
import time
import csv
import hashlib

import numpy as np
import psycopg2

from dbconnection import postgres_connection

QUERY_BASE = "SELECT * FROM"


class Join:

    def __init__(self, source, source_key, destination, destination_key):
        self.source = source
        self.source_key = source_key
        self.destination = destination
        self.destination_key = destination_key


class Table:

    def __init__(self, name, alias, tablename_to_join):
        self.name = name
        self.alias = alias
        self.tablename_to_join = tablename_to_join


def create_schema():
    customer = Table('customer', 'cus', [])
    category = Table('category', 'cat', [])
    deliverer = Table('deliverer', 'del', [])
    discount = Table('discount', 'dis', [])
    # requires quotation since order is a restricted word
    order = Table('"order"', 'ord', [])
    order_detail = Table('order_details', 'ord_det', [])
    product = Table('product', 'prod', [])
    subcategory = Table('subcategory', 'sub', [])

    cust_joinables_order = Join(customer, 'id', order, 'customer_id')
    customer.tablename_to_join = {order.name: cust_joinables_order}

    cat_joinables_subcat = Join(category, 'id', subcategory, 'category_id')
    category.tablename_to_join = {subcategory.name: cat_joinables_subcat}

    del_joinables_order = Join(deliverer, 'id', order, 'deliverer_id')
    deliverer.tablename_to_join = {order.name: del_joinables_order}

    dis_joinables_order = Join(discount, 'id', order, 'discount_id')
    discount.tablename_to_join = {order.name: dis_joinables_order}

    order_joinables_cust = Join(order, 'customer_id', customer, 'id')
    order_joinables_del = Join(order, 'deliverer_id', deliverer, 'id')
    order_joinables_disc = Join(order, 'discount_id', discount, 'id')
    order_joinables_order_det = Join(order, 'id', order_detail, 'order_id')
    order.tablename_to_join = {customer.name: order_joinables_cust, deliverer.name: order_joinables_del,
                               discount.name: order_joinables_disc, order_detail.name: order_joinables_order_det}

    order_det_joinables_order = Join(order_detail, 'order_id', order, 'id')
    order_det_joinables_prod = Join(order_detail, 'product_id', product, 'id')
    order_detail.tablename_to_join = {order.name: order_det_joinables_order, product.name: order_det_joinables_prod}

    prod_joinables_subcat = Join(product, 'subcategory_id', subcategory, 'id')
    prod_joinables_order_detail = Join(product, 'id', order_detail, 'product_id')
    product.tablename_to_join = {order_detail.name: prod_joinables_order_detail,
                                 subcategory.name: prod_joinables_subcat}

    subcat_joinables_cat = Join(subcategory, 'category_id', category, 'id')
    subcat_joinables_prod = Join(subcategory, 'id', product, 'subcategory_id')
    subcategory.tablename_to_join = {category.name: subcat_joinables_cat, product.name: subcat_joinables_prod}

    return {'customer': customer, 'category': category, 'deliverer': deliverer, 'discount': discount, 'order': order,
            'order_detail': order_detail, 'product': product, 'subcategory': subcategory}


def powerset(iterable):
    "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
    s = list(iterable)
    return itertools.chain.from_iterable(itertools.combinations(s, r) for r in range(len(s)+1))

def get_n_joinable_tables(n, schema):
    power_set = list(powerset(schema.keys()))
    n_power_set = [comb for comb in power_set if len(comb) == n]

    all_possible_joinable_tables = []
    for comb in n_power_set:
        join_possible = True
        for table in comb:
            joinable_tables_for_current = schema[table].tablename_to_join

            # iter joins
            for joinable_table_name in joinable_tables_for_current.keys():
                if not joinable_table_name in comb:
                    join_possible = False
                    break
            if not join_possible:
                break

        if join_possible:
            all_possible_joinable_tables.append(comb)
            
    joinable_tables = []
    return all_possible_joinable_tables

def generate_queries(schema, query_patcher, n):
    # create_adjacency(schema)
    # joinable_tables = get_n_joinable_tables(n, schema)
    permutations = itertools.permutations(schema.keys())
    queries = []
    for permutation in permutations:
        used_relations = {}
        impossible_query = False
        first_table = schema[permutation[0]]
        query = "SELECT * FROM " + first_table.name + " as " + first_table.alias
        used_relations[first_table.name] = first_table

        for i in range(1, len(permutation)):
            next_relation = schema[permutation[i]]

            # looks for an already joined relation to join with
            rel_to_join_with = functools.reduce(
                lambda rel_to_join, linked_relation: rel_to_join if rel_to_join is not None else used_relations[
                    linked_relation] if linked_relation in used_relations else None,
                list(reversed(list(next_relation.tablename_to_join.keys()))),
                None)
            if rel_to_join_with is not None:
                join = next_relation.tablename_to_join[rel_to_join_with.name]
                query += "\nJOIN " + next_relation.name + " as " + next_relation.alias \
                         + "\nON " + next_relation.alias + "." + join.source_key + " = " + join.destination.alias + "." + join.destination_key
                used_relations[next_relation.name] = next_relation
            else:
                impossible_query = True
                break

        if not impossible_query:
            query += ";"
            query = query_patcher(query)
            queries.append(query)
    return queries


def create_adjacency(schema):
    idx_lookup = {name: i for i, name in enumerate(schema)}
    adj = np.zeros(shape=(len(schema), len(schema)))

    for n in schema:
        for m in schema:
            adj[idx_lookup[m], idx_lookup[n]] = 1 if m in schema[n].tablename_to_join else 0

    print(adj)


def mssql_patcher(query):
    return (query.replace(";", "")) + "\nOPTION(FORCE ORDER);"


# preserve/force join order in postgres
def postgres_patcher(query):
    return "BEGIN;\nSET LOCAL join_collapse_limit = 1;\n" + query + "\nCOMMIT;"


def connect():
    conn = psycopg2.connect(**postgres_connection())
    cursor = conn.cursor()
    return conn, cursor


def reconnect(conn, cursor):
    conn.close()
    cursor.close()
    return connect()


if __name__ == '__main__':
    conn, cursor = connect()
    schema = create_schema()
    print(get_n_joinable_tables(4, schema))
    queries = generate_queries(schema, postgres_patcher)
    for query in queries:
        try:
            print("Executing:\n" + query)
            start_time = time.time()
            cursor.execute(query)
            result = cursor.statusmessage
            end_time = time.time()
            elapsed_time = end_time - start_time
            print("\t->\t" + str(elapsed_time))
            print("\n\n")
            with open("./query_execution_times.csv", "a") as csv_file:
                writer = csv.writer(csv_file, delimiter=',')
                writer.writerow([hashlib.sha256(query.encode('utf-8')).hexdigest(), query, elapsed_time])
        except Exception as ex:
            print(ex)
            conn, cursor = reconnect(conn, cursor)
    print(len(queries))
