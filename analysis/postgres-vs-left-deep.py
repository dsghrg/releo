import matplotlib.pyplot as plt
import pandas as pd


def get_table_names(query_string):
    return [qry.strip() for qry in query_string.replace('[', '').replace(']', '').replace("'", '').split(',')]


def logicall_eq(query1, query2):
    tb1 = set(get_table_names(query1))
    tb2 = set(get_table_names(query2))
    return tb1 == tb2


def logical_eq(series, query):
    return series.apply(lambda r: logicall_eq(r, query))


def add_bench(qps_bench, query):
    ref = qps_bench[logical_eq(qps_bench['logical-query'], query)]['exec-time']
    if len(ref.values) > 0:
        return ref.values[0]
    return -1


qps = pd.read_csv('../src/db/executor/lookup-files/postgres/shop-db-non-uniform-reference.csv')
qps_post = pd.read_csv('../src/db/executor/lookup-files/postgres/shop-db-non-uniform-testset-reference.csv')
qname_lookup = pd.read_csv('query-name-lookup.csv')

qps['complexity'] = qps['logical-query'].apply(
    lambda x: str(len(x.replace('[', '').replace(']', '').split(', '))))
qps['join-order'] = qps['join-order'].apply(lambda x: x.replace('(', '[').replace(')', ']'))
qps_post['complexity'] = qps_post['logical-query'].apply(
    lambda x: str(len(x.replace('[', '').replace(']', '').split(', '))))

qps['query-name'] = qps['logical-query'].apply(lambda log_query: qname_lookup[
    qname_lookup['logical-query'].apply(lambda r: logicall_eq(r, log_query))]['query-name'].values[0])
qps['ref-time'] = qps['logical-query'].apply(lambda q: add_bench(qps_post, q))
qps = qps[qps['ref-time'] >= 0]

ldeep_min = qps.groupby('logical-query')['exec-time'].min()
post_min = qps.groupby('logical-query')['ref-time'].min()

exec_times = []
bench_times = []
xs = []
# for name, group in qps.groupby('query-name'):
#     exec_times.append(group['exec-time'].min())
#     bench_times.append(group['ref-time'].min())
#     xs.append(name)


ax = qps.groupby('query-name')['exec-time', 'ref-time'].min().plot(kind='bar', label=['best performing left-deep (ms)',
                                                                                      'postgres optimized plan (ms)'],
                                                                   figsize=(6.4, 4))
# plt.bar(xs, exec_times, color='blue', label='absolute minimum of left-deep plan (ms)')
# plt.bar(xs, bench_times, color='red', label='postgres optimized query plan (ms)')
# ax = qps.plot.scatter(x='key-join-pos', y='exec-time', c='color-group', fontsize=12)
# ax.set_ylabel('execution time (ms)')
# ax.set_xlabel('index of key-join ')
# ax.set_title('Impact of early key-join in regard to query execution time')
plt.title('Best performing left-deep vs. PostgreSQL optimized')
plt.legend(['best performing left-deep (ms)', 'postgres optimized plan (ms)'])
plt.ylabel('(ms)')
plt.xlabel('query name')
plt.show()
