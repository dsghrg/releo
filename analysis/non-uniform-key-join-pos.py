import matplotlib.pyplot as plt
import pandas as pd


def get_table_names(query_string):
    return [qry.strip() for qry in query_string.replace('[', '').replace(']', '').replace("'", '').split(',')]


def get_key_join_idx(join_order):
    tables = get_table_names(join_order)
    # plus 1 since idx 2 => third table aka second join and we want table idx
    return max(tables.index('category'), tables.index('subcategory'))


all_recs = pd.read_csv('../src/db/executor/lookup-files/postgres/shop-db-non-uniform-reference.csv')

all_recs['complexity'] = all_recs['logical-query'].apply(
    lambda x: str(len(x.replace('[', '').replace(']', '').split(', '))))
all_recs['join-order'] = all_recs['join-order'].apply(lambda x: x.replace('(', '[').replace(')', ']'))

qps = all_recs[all_recs['complexity'] == '8']

qps['key-join-pos'] = qps['join-order'].apply(lambda order: get_key_join_idx(order))
qps['color-group'] = qps['key-join-pos'].apply(lambda idx: 'green' if idx <= 2 else 'red')

ax = qps.plot.scatter(x='key-join-pos', y='exec-time', c='color-group', figsize=(8,4))
ax.set_ylabel('execution time (ms)')
ax.set_xlabel('index of key-join ')
ax.set_title('Impact of early key-join in regard to query execution time')

plt.show()

