import csv
import functools
import hashlib
import itertools
import random
import time
import re

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
    order = Table('order', 'ord', [])
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
            'order_details': order_detail, 'product': product, 'subcategory': subcategory}


def powerset(iterable):
    "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
    s = list(iterable)
    return itertools.chain.from_iterable(itertools.combinations(s, r) for r in range(len(s) + 1))


def dfs(visited, schema, comb, current):
    if current.name not in visited:
        visited.add(current.name)
        for neighbour in schema[current.name].tablename_to_join.keys():
            if neighbour in comb:
                dfs(visited, schema, comb, schema[neighbour])


# generate all possible logical queries with n involved relations
def get_n_joinable_tables(n, schema):
    power_set = list(powerset(schema.keys()))
    n_power_set = [comb for comb in power_set if len(comb) == n]
    all_possible_joinable_tables = []

    for i, comb in enumerate(n_power_set):
        visited = set()
        dfs(visited, schema, comb, schema[comb[0]])
        if len(visited) == n:
            all_possible_joinable_tables.append(comb)
    return all_possible_joinable_tables


def generate_join_orders(schema, query_patcher, logical_query):

    permutations = itertools.permutations(logical_query)
    queries = []

    for permutation in permutations:
        used_relations = {}
        impossible_query = False
        first_table = schema[permutation[0]]
        query = "SELECT count(*) FROM " + first_table.name + " as " + first_table.alias
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
            query = re.sub(r'\border\b', '"order"', query)
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


def enable_auto_explain(cursor):
    cursor.execute("LOAD 'auto_explain';")
    cursor.execute("SET auto_explain.log_min_duration = 0;")
    cursor.execute("SET auto_explain.log_analyze = true;")


if __name__ == '__main__':
    conn, cursor = connect()
    enable_auto_explain(cursor)
    schema = create_schema()
    for i in range(4, 9):
        logical_queries = get_n_joinable_tables(i, schema)
        logical_query = random.choice(logical_queries)
        queries = generate_join_orders(schema, postgres_patcher, logical_query)
        no_of_possible_executions = len(queries)
        if (len(queries) > 100):
            queries = random.choices(queries, k=100)
        ratio_executed_to_possible_executions = len(queries) / no_of_possible_executions
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
                with open("./query_execution_times_4-7.csv", "a") as csv_file:
                    writer = csv.writer(csv_file, delimiter=',')
                    writer.writerow([hashlib.sha256(query.encode('utf-8')).hexdigest(), query, logical_query,
                                     ratio_executed_to_possible_executions, len(logical_query), elapsed_time])
            except Exception as ex:
                print(ex)
                conn, cursor = reconnect(conn, cursor)
