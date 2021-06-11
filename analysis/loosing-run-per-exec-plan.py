import pandas as pd
import matplotlib.pyplot as plt
import os
import glob


def get_table_names(query_string):
    return [qry.strip() for qry in query_string.replace('[', '').replace(']', '').replace("'", '').split(',')]


def logicall_eq(query1, query2):
    tb1 = set(get_table_names(query1))
    tb2 = set(get_table_names(query2))
    return tb1 == tb2


all_permutations_files = glob.glob('logs/releo-all-test-permutations/**/testset-all-permutations-log.csv')
all_perms = []
qname_lookup = pd.read_csv('query-name-lookup.csv')

for idx, file in enumerate(all_permutations_files):
    df = pd.read_csv(file, sep=',')
    df['run-nr'] = 'run-' + str(idx + 1)
    df['query-name'] = df['logical-query'].apply(lambda log_query: qname_lookup[
        qname_lookup['logical-query'].apply(lambda r: logicall_eq(r, log_query))]['query-name'].values[0])

    all_perms.append(df)

all_recs = pd.concat(all_perms)

all_recs['order'] = all_recs['order'].apply(lambda x: x.replace('(', '[').replace(')', ']'))
all_recs = all_recs[all_recs[
                        'logical-query'] == "['category', 'order_details', 'product', 'subcategory', 'customer', 'order', 'deliverer', 'discount']"]
grouped_order = all_recs.groupby('order')

res = all_recs[grouped_order['exec-time'].transform(max) == all_recs['exec-time']].sort_values(['exec-time'])

ax = res.groupby('run-nr')['record-id'].count().plot(kind='bar', figsize=(6.4, 4), rot=45)
ax.set_title('Count of execution plans with worst execution time grouped by the run in which they were observerd')
ax.set_title('Count of worst measured plans by run in which it occurred')
ax.set_ylabel('count of execution plans')
ax.set_xlabel('run')
plt.show()
# grouped_order = grouped_order[grouped_order['query-name'] == 'Q38']
# desc = all_recs.describe()['exec-time']
# ax = all_recs.boxplot(['exec-time'], showmeans=True, figsize=(6.4, 4))
# ax.set_title('Distribution of execution time for Q38')
# text = ' n={:2f}\nmean={:2f}\n std={:2f}\n max={:2f}\n min={:2f}'.format(desc['count'], desc['mean'], desc['std'],
#                                                                          desc['max'], desc['min'])
# ax.annotate(text, xy=(0.05, 0.8), xycoords='axes fraction')
# plt.ylabel('execution time (ms)')
# plt.xlabel('execution time')
# plt.show()
