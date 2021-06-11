import matplotlib.pyplot as plt
import pandas as pd


def get_table_names(query_string):
    return [qry.strip() for qry in query_string.replace('[', '').replace(']', '').replace("'", '').split(',')]


def logicall_eq(query1, query2):
    tb1 = set(get_table_names(query1))
    tb2 = set(get_table_names(query2))
    return tb1 == tb2

vals = [{'eps-min': 0.05, 'eps-decay':0.1, 'name': 'min-random'}]



xs = [x for x in range(0,200)]
plt.plot(xs,  [min(1*0.941**x, 5) for x in xs])
plt.show()

all_recs = pd.read_csv('../src/db/executor/lookup-files/postgres/shop-db-uniform-reference.csv')
qname_lookup = pd.read_csv('query-name-lookup.csv')

all_recs['complexity'] = all_recs['logical-query'].apply(
    lambda x: str(len(x.replace('[', '').replace(']', '').split(', '))))
all_recs['join-order'] = all_recs['join-order'].apply(lambda x: x.replace('(', '[').replace(')', ']'))
all_recs['query-name'] = all_recs['logical-query'].apply(lambda log_query: qname_lookup[
    qname_lookup['logical-query'].apply(lambda r: logicall_eq(r, log_query))]['query-name'].values[0])
all_recs['query-nr'] = all_recs['query-name'].apply(lambda x: int(x.replace('Q','')))

all_recs = all_recs.sort_values(['complexity'], ascending=True)
groups = {qry: idx for idx, qry in enumerate(
    all_recs.sort_values(['query-nr'], ascending=True)['query-name'].unique())}
order = [groups[qry] for qry in all_recs.groupby(['query-name']).groups]

ax = all_recs.boxplot(by='query-name', column='exec-time', rot=45, figsize=(8, 4), positions=list(order))
ax.set_ylabel('execution time (ms)')
ax.set_title('Distribution of execution time by query')
ax.get_figure().suptitle('')
ax.set_xlabel('query name')
ax.get_figure().suptitle('')
plt.show()
