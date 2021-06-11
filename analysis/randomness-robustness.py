import glob
import os

import matplotlib.pyplot as plt
import pandas as pd


def get_table_names(query_string):
    return [qry.strip() for qry in query_string.replace('[', '').replace(']', '').replace("'", '').split(',')]


def logicall_eq(query1, query2):
    tb1 = set(get_table_names(query1))
    tb2 = set(get_table_names(query2))
    return tb1 == tb2


def get_key_join_idx(join_order):
    tables = get_table_names(join_order)
    if 'category' not in tables or 'subcategory' not in tables:
        return -1
    # plus 1 since idx 2 => third table aka second join and we want table idx
    return max(tables.index('category'), tables.index('subcategory'))


def fetch_files(experiments, base_dir, unif, force=False):
    reference = None
    train_eval = None
    train = None
    qname_lookup = pd.read_csv('query-name-lookup.csv')
    path = '../src/db/executor/lookup-files/postgres/shop-db-'
    path = path + ('non-' if not unif else '') + 'uniform-testset-reference.csv'
    bench = pd.read_csv(path)
    bench['query-name'] = bench['logical-query'].apply(lambda log_query: qname_lookup[
        qname_lookup['logical-query'].apply(lambda r: logicall_eq(r, log_query))]['query-name'].values[0])

    if os.path.isfile('./explore-exploit-ref.csv') and not force:
        reference = pd.read_csv('./explore-exploit-ref.csv')
    else:
        path = '../src/db/executor/lookup-files/postgres/shop-db-'
        path = path + ('non-' if not unif else '') + 'uniform-reference.csv'
        reference = pd.read_csv(path)
        reference['query-name'] = reference['logical-query'].apply(lambda log_query: qname_lookup[
            qname_lookup['logical-query'].apply(lambda r: logicall_eq(r, log_query))]['query-name'].values[0])
        reference.to_csv('./explore-exploit-ref.csv')

    if os.path.isfile('./explore-exploit-train-eval.csv') and not force:
        train_eval = pd.read_csv('./explore-exploit-train-eval.csv')
    else:
        train_eval = []
        for group_run in experiments:
            df = pd.concat([pd.read_csv(path) for path in glob.glob(base_dir + group_run + '/**/train-eval-log.csv')])
            df['experiment'] = df['logical-query'].apply(lambda x: group_run)
            df['query-name'] = df['logical-query'].apply(lambda log_query: qname_lookup[
                qname_lookup['logical-query'].apply(lambda r: logicall_eq(r, log_query))]['query-name'].values[0])
            train_eval.append(df)
        train_eval = pd.concat(train_eval)
        train_eval.to_csv('./explore-exploit-train-eval.csv')
    if os.path.isfile('./explore-exploit-train.csv') and not force:
        train = pd.read_csv('./explore-exploit-train.csv')

    else:
        train = []
        for group_run in experiments:
            for filename in glob.glob(base_dir + group_run + '/**/train-log.csv'):
                df = pd.read_csv(filename)
                df['run-name'] = df['logical-query'].apply(lambda x: filename.split('/')[-2])
                df['experiment'] = df['logical-query'].apply(lambda x: group_run)
                df['query-name'] = df['logical-query'].apply(lambda log_query: qname_lookup[
                    qname_lookup['logical-query'].apply(lambda r: logicall_eq(r, log_query))]['query-name'].values[0])
                train.append(df)
        train = pd.concat(train)
        train.to_csv('./explore-exploit-train.csv')
    return reference, train_eval, train, bench


# exps = ['min-random-0', 'min-random-5', 'min-random-10', 'min-random-20', 'min-random-33', 'min-random-50',
#         'min-random-75', 'min-random-100']
# exps = ['nn-l', 'nn-m', 'nn-s', 'nn-xl', 'nn-xs', 'nn-xxl', 'min-random-20']
# exps = ['query-gen-rnd', 'query-gen-epsilon-rnd', 'epsilon-rnd']
# exps = ['ba-epsilon-rnd', 'ba-query-gen-rnd', 'query-gen-rnd', 'query-gen-epsilon-rnd', 'epsilon-rnd',
#         'epsilon-rnd-min-random-0', 'epsilon-rnd-min-random-10', 'query-gen-rnd-min-random-0',
#         'query-gen-rnd-min-random-10']

exps = ['epsilon-rnd-min-random-0', 'epsilon-rnd-min-random-10', 'epsilon-rnd', 'ba-epsilon-rnd']
label_lookup = {'epsilon-rnd-min-random-0': 'epsilon random - explore 0%',
                'epsilon-rnd-min-random-10': 'epsilon random - explore 10%',
                'epsilon-rnd': 'epsilon random - explore 20%',
                'ba-epsilon-rnd': 'epsilon random - explore 100%'}
# order_lookup = {'epsilon-rnd-min-random-0': 0, 'epsilon-rnd-min-random-10': 1, 'epsilon-rnd': 2, 'epsilon-rnd-min-random-100': 3}

# exps = ['query-gen-rnd-min-random-0', 'query-gen-rnd-min-random-10', 'query-gen-rnd', 'ba-query-gen-rnd']
# label_lookup = {'query-gen-rnd-min-random-0': 'query random - explore 0%',
#                 'query-gen-rnd-min-random-10': 'query random - explore 10%',
#                 'query-gen-rnd': 'query random - explore 20%',
#                 'ba-query-gen-rnd': 'query random - explore 100%'}

# exps = ['query-gen-rnd-epsilon-rnd-min-random-0', 'query-gen-rnd-epsilon-rnd-min-random-5',
#         'query-gen-rnd-epsilon-rnd-min-random-10', 'query-gen-epsilon-rnd', 'query-gen-rnd-epsilon-rnd-min-random-50',
#         'query-gen-rnd-epsilon-rnd-min-random-75', 'query-gen-rnd-epsilon-rnd-min-random-100']
# label_lookup = {'query-gen-rnd-epsilon-rnd-min-random-0': 'both - explore 0%',
#                 'query-gen-rnd-epsilon-rnd-min-random-5': 'both - explore 5%',
#                 'query-gen-rnd-epsilon-rnd-min-random-10': 'both - explore 10%',
#                 'query-gen-epsilon-rnd': 'both - explore 20%',
#                 'query-gen-rnd-epsilon-rnd-min-random-50': 'both - explore 50%',
#                 'query-gen-rnd-epsilon-rnd-min-random-75': 'both - explore 75%',
#                 'query-gen-rnd-epsilon-rnd-min-random-100': 'both - explore 100%'}
# order_lookup = {'query-gen-rnd-min-random-0': 0, 'query-gen-rnd-min-random-10': 1, 'query-gen-rnd': 2, 'epsilon-rnd-min-random-100': 3}
# exps = ['100ep', '150ep', 'min-random-20']

# label_lookup = {'epsilon-rnd-min-random-0': 'epsilon random - explore 0%',
#                 'epsilon-rnd-min-random-10': 'epsilon random - explore 10%',
#                 'epsilon-rnd': 'epsilon random - explore 20%', 'ba-epsilon-rnd': 'epsilon random - explore 100%'}

base_dir = '../src/runs/postgres/non-uniform/'

ref, eval, train, bench = fetch_files(exps, base_dir, False, True)

train['key-join-pos'] = train['join-order'].apply(lambda order: get_key_join_idx(order))

min_per_query = ref.groupby('query-name', as_index=False)['exec-time'].min()
eval_by_experiment = eval.groupby(['experiment'])
eval['mean-exec'] = eval.groupby(['experiment', 'query-name', 'episode']).transform('mean')['exec-time']
eval['std-exec'] = eval.groupby(['experiment', 'query-name', 'episode']).transform('std')['exec-time']
eval['std-rel'] = eval['std-exec'] / eval['mean-exec']
performance_per_query = eval.groupby(['query-name', 'experiment', 'episode'])['exec-time'].agg(exec_mean='mean',
                                                                                               exec_std='std').reset_index()
performance_per_query['std-rel'] = performance_per_query['exec_std'] / performance_per_query['exec_mean']
performance_per_query['mean-rel-to-min'] = performance_per_query.apply(
    lambda tup: tup['exec_mean'] / min_per_query[min_per_query['query-name'] == tup['query-name']]['exec-time'].values[
        0], axis=1)
bench['best-left-deep'] = bench['query-name'].apply(
    lambda n: min_per_query[min_per_query['query-name'] == n]['exec-time'].values[0])
bench['rel-to-best'] = bench['exec-time'] / bench['best-left-deep']
bench['diff-to-best'] = bench.apply(
    lambda r: abs(
        r['exec-time'] - min_per_query[min_per_query['query-name'] == r['query-name']]['exec-time'].values[0]), axis=1)
# eval['mean-rel-lowest'] = eval[]


for query_name, group in performance_per_query.groupby(['query-name'], as_index=False):
    postgres = bench[bench['query-name'] == query_name]
    plt.plot(group['episode'].unique(), [1 for x in group['episode'].unique()], label='best left-deep',
             linestyle='dashed', color='black')
    plt.plot(group['episode'].unique(), [postgres['rel-to-best'] for x in group['episode'].unique()],
             label='postgres optimized',
             linestyle='dashed', color='red')
    for exp in group['experiment'].unique():
        to_plot = group[group['experiment'] == exp]
        plt.plot(to_plot['episode'], to_plot['mean-rel-to-min'], label=label_lookup[exp])
    plt.xlabel('episode')
    plt.ylabel('relative performance to baseline')
    plt.title('Performance relative to best left-deep for query ' + str(query_name))
    plt.legend(loc='lower left')
    plt.show()

xs = performance_per_query['episode'].unique()
plt.plot(xs, [1 for x in xs], label='baseline (best left deep)', linestyle='dashed', color='black')
mean_bench_rel = bench['rel-to-best'].mean()
plt.plot(xs, [mean_bench_rel for x in xs], linestyle='dashed', color='red', label='postgres optimized')
for exp, group in performance_per_query.groupby(['experiment'], as_index=False):
    plt.plot(group['episode'].unique(), [1 for x in group['episode'].unique()], label='best left-deep',
             linestyle='dashed', color='black')
    by_ep = group.groupby('episode')
    mean_mean = by_ep['mean-rel-to-min'].mean()
    std_mean = by_ep['std-rel'].mean()
    sem = by_ep['std-rel'].sem()
    lower = mean_mean - sem
    upper = mean_mean + sem
    xs = group['episode'].unique()
    plt.plot(group['episode'].unique(), mean_mean, label=label_lookup[exp])
    plt.fill_between(xs, lower, upper, color='blue', alpha=0.04)
    plt.xlabel('episode')
    plt.ylabel('relative performance to baseline')
    plt.legend()
    plt.title('Query sampling variation - Performance overall per exploration rate')
plt.show()

ax = performance_per_query[performance_per_query['episode'] == performance_per_query['episode'].max()].boxplot(
    by='experiment', column='std-rel', showmeans=True, figsize=(9, 6), rot=45)
ax.set_title(None)
plt.ylabel('difference of standard deviation in relation to mean')
plt.xlabel('experimental setup')
plt.title('Distribution of standard deviation relative to the respective mean per experiment after training')
plt.show()

unique_qps_per_query = ref.groupby('query-name')['join-order'].agg(no_of_plans='count').reset_index()
mean_unique_plans = \
    train.groupby(['experiment', 'query-name', 'run-name'], as_index=False)['join-order'].nunique().groupby(
        ['experiment', 'query-name'], as_index=False)['join-order'].mean().rename(
        columns={'join-order': 'mean-unique-qps'})

mean_query_count = \
    train.groupby(['experiment', 'run-name', 'query-name'])['query-name'].agg(count='count').reset_index().groupby(
        ['experiment', 'query-name'], as_index=False)['count'].mean()
mean_query_count['no_of_plans'] = mean_query_count['query-name'].apply(
    lambda n: unique_qps_per_query[unique_qps_per_query['query-name'] == n]['no_of_plans'].values[0])
mean_query_count['key-join-present'] = mean_query_count['query-name'].apply(
    lambda n: get_key_join_idx(ref[ref['query-name'] == n]['join-order'].values[0]) >= 0)

ref_dist = mean_query_count[mean_query_count['experiment'] == 'ba-epsilon-rnd']

# ref_dist['bin-filled'] = ref_dist.apply(lambda r: min(r['count'], unique_qps_per_query[
#     unique_qps_per_query['query-name'] == r['query-name']]['no_of_plans'].values[0]), axis=1)

mean_unique_plans['percentage-of-space'] = mean_unique_plans.apply(lambda r: r['mean-unique-qps'] /
                                                                             unique_qps_per_query[
                                                                                 unique_qps_per_query['query-name'] ==
                                                                                 r['query-name']]['no_of_plans'].values[
                                                                                 0], axis=1)
mean_unique_plans['no_of_plans'] = mean_unique_plans['query-name'].apply(
    lambda n: unique_qps_per_query[unique_qps_per_query['query-name'] == n]['no_of_plans'].values[0])

mean_query_count = mean_query_count.sort_values(['experiment', 'no_of_plans'])
mean_unique_plans = mean_unique_plans.sort_values(['experiment', 'no_of_plans'])
mean_unique_plans['key-join-present'] = mean_unique_plans['query-name'].apply(
    lambda n: get_key_join_idx(ref[ref['query-name'] == n]['join-order'].values[0]) >= 0)

_, ax = plt.subplots(1, 1, figsize=(10, 4))

mean_query_count[mean_query_count['experiment'] == 'query-gen-rnd-epsilon-rnd-min-random-100'].plot(kind='bar', x='query-name',
                                                                                        y='count',
                                                                                        rot=45,
                                                                                        ax=ax,
                                                                                        label='mean count of query being sampled')

mean_unique_plans[mean_unique_plans['experiment'] == 'query-gen-rnd-epsilon-rnd-min-random-100'].plot(kind='bar', x='query-name',
                                                                                          y='mean-unique-qps',
                                                                                          rot=45, ax=ax, color='orange',
                                                                                          label='mean unique plans per query')
plt.ylabel('number of samples')
plt.xlabel('query name')
plt.title('Mean number of query sampled vs. mean unique query plans per query (100% Exploration)')
plt.show()

_, ax = plt.subplots(1, 1, figsize=(10, 4))
mean_query_count[mean_query_count['experiment'] == 'query-gen-epsilon-rnd'].plot(kind='bar', x='query-name', y='count',
                                                                         rot=45,
                                                                         ax=ax,
                                                                         label='mean count of query being sampled')

mean_unique_plans[mean_unique_plans['experiment'] == 'query-gen-epsilon-rnd'].plot(kind='bar', x='query-name',
                                                                           y='mean-unique-qps',
                                                                           rot=45, ax=ax, color='orange',
                                                                           label='mean unique plans per query')
plt.ylabel('number of samples')
plt.xlabel('query name')
plt.title(('Mean number of query sampled vs. mean unique query plans per query (20% Exploration)'))
plt.show()

_, ax = plt.subplots(1, 1, figsize=(10, 4))
mean_query_count[mean_query_count['experiment'] == 'query-gen-rnd-epsilon-rnd-min-random-0'].plot(kind='bar', x='query-name',
                                                                                      y='count',
                                                                                      rot=45,
                                                                                      ax=ax,
                                                                                      label='mean count of query being sampled')

mean_unique_plans[mean_unique_plans['experiment'] == 'query-gen-rnd-epsilon-rnd-min-random-0'].plot(kind='bar', x='query-name',
                                                                                        y='mean-unique-qps',
                                                                                        rot=45, ax=ax, color='orange',
                                                                                        label='mean unique plans per query')
plt.ylabel('number of samples')
plt.xlabel('query name')
plt.ylabel('number of samples')
plt.xlabel('query name')
plt.title(('Mean number of query sampled vs. mean unique query plans per query (0% Exploration)'))
plt.show()

#
#
#
#
#
#
#
#
#
#
#


# ax = mean_query_count[mean_query_count['experiment'] == 'ba-query-gen-rnd'].boxplot(by='experiment', column='count',
#                                                                                     showmeans=True)
# ax.set_title('')
# ax.set_xlabel(None)
# plt.xlabel(None)
# plt.ylabel('number of occurance for query in trainig set')
# plt.title('Distribution of query occurance during training')
# plt.legend()
# plt.show()

# _, axs = plt.subplots(1, 1, figsize=(10, 3))
# reff = ref2[ref2['experiment'] == 'min-random-20']
# ref_dist.plot(kind='bar', x='query-name', y='count', rot=45,
#               label='number of times query is sampled during training', ax=axs)
# ref_dist.plot(kind='bar', x='query-name', y='bin-filled', rot=45,
#               label='number of unique existing query plans', ax=axs, color='orange')
# reff.plot(kind='bar', x='query-name', y='join-order', rot=45,
#           label='unique plans seen', ax=axs, color='green')
# axs.set_title('Mean number of unique query plans for queries during training (20% exploring)')
# axs.set_ylabel('number of plans')
# axs.set_xlabel('query name')

# _, axs = plt.subplots(2, 1)
# mean_unique_plans[mean_unique_plans['experiment'] == 'ba-epsilon-rnd'].plot(kind='bar', x='query-name',
#                                                                             y='percentage-of-space', rot=45, ax=axs[0],
#                                                                             stacked=False)
# mean_unique_plans[mean_unique_plans['experiment'] == 'ba-epsilon-rnd'].plot(kind='bar', x='query-name',
#                                                                               y='percentage-of-space', rot=45,
#                                                                               ax=axs[1],
#                                                                               color='orange', stacked=False)
# plt.ylabel('number of samples')
# plt.xlabel('query name')
# plt.title('Number of query sampled vs. mean unique query plans observed (100% Exploration)')
# plt.show()

#
#
#
#
#
#
#
#
#
#
#
#


# mean_unique_plans['sampled'] = mean_unique_plans.apply(lambda r: mean_query_count[
#     (mean_query_count['experiment'] == r['experiment']) & (mean_query_count['query-name'] == r['query-name'])][
#     'count'].values[0], axis=1)
# _, axs = plt.subplots(1, 1, figsize=(10, 4))
# mean_unique_plans = mean_unique_plans.sort_values(['experiment', 'sampled'], ascending=False)
# mean_unique_plans[mean_unique_plans['experiment'] == 'epsilon-rnd-min-random-100'].plot(kind='bar', y='sampled',
#                                                                                         label='number of samples for query',
#                                                                                         x='query-name', ax=axs)
# mean_unique_plans[mean_unique_plans['experiment'] == 'epsilon-rnd-min-random-100'].plot(kind='bar', y='mean-unique-qps',
#                                                                                         x='query-name', ax=axs,
#                                                                                         color='orange',
#                                                                                         label='mean number of unique query plans',
#                                                                                         linestyle='dashed',
#                                                                                         edgecolor='orange')
# plt.ylabel('number of samples')
# plt.xlabel('query name')
# plt.title('Number of query sampled vs. mean unique query plans observed (100% Exploration)')
# plt.show()
#
# mean_unique_plans['sampled'] = mean_unique_plans.apply(lambda r: mean_query_count[
#     (mean_query_count['experiment'] == r['experiment']) & (mean_query_count['query-name'] == r['query-name'])][
#     'count'].values[0], axis=1)
# _, axs = plt.subplots(1, 1, figsize=(10, 4))
# mean_unique_plans = mean_unique_plans.sort_values(['experiment', 'sampled'], ascending=False)
# # key_join = mean_unique_plans[mean_unique_plans['key-join-presen']]
# colors = mean_unique_plans[mean_unique_plans['experiment'] == 'epsilon-rnd']['key-join-present'].apply(
#     lambda y: 'green' if y else '#1f77b4')
#
# mean_unique_plans[(mean_unique_plans['experiment'] == 'epsilon-rnd')].plot(kind='bar', y='sampled', x='query-name',
#                                                                            ax=axs, color=colors,
#                                                                            label='number of samples for query')
# mean_unique_plans[mean_unique_plans['experiment'] == 'epsilon-rnd'].plot(kind='bar', y='mean-unique-qps',
#                                                                          x='query-name', ax=axs,
#                                                                          color='orange',
#                                                                          label='mean number of unique query plans',
#                                                                          linestyle='dashed',
#                                                                          edgecolor='orange')
# qs = mean_unique_plans[mean_unique_plans['experiment'] == 'epsilon-rnd']['query-name']
# plt.plot([q for q in qs], [200 / 30 for q in qs], linestyle='dashed', color='black',
#          label='mean number of queries sampled')
# plt.ylabel('number of samples')
# plt.xlabel('query name')
# plt.title('Number of query sampled vs. mean unique query plans observed (20% Exploration)')
# plt.show()
#
# mean_unique_plans['sampled'] = mean_unique_plans.apply(lambda r: mean_query_count[
#     (mean_query_count['experiment'] == r['experiment']) & (mean_query_count['query-name'] == r['query-name'])][
#     'count'].values[0], axis=1)
# _, axs = plt.subplots(1, 1, figsize=(10, 4))
# mean_unique_plans = mean_unique_plans.sort_values(['experiment', 'sampled'], ascending=False)
# mean_unique_plans[mean_unique_plans['experiment'] == 'epsilon-rnd-min-random-0'].plot(kind='bar', y='sampled',
#                                                                                       label='number of samples for query',
#                                                                                       x='query-name', ax=axs)
# mean_unique_plans[mean_unique_plans['experiment'] == 'epsilon-rnd-min-random-0'].plot(kind='bar', y='mean-unique-qps',
#                                                                                       x='query-name', ax=axs,
#                                                                                       color='orange',
#                                                                                       label='mean number of unique query plans',
#                                                                                       linestyle='dashed',
#                                                                                       edgecolor='orange')
# plt.ylabel('number of samples')
# plt.xlabel('query name')
# plt.title('Number of query sampled vs. mean unique query plans observed (0% Exploration)')
# plt.show()


#
#
#
#
#
#
#
#


# mean_unique_plans['sampled'] = mean_unique_plans.apply(lambda r: mean_query_count[
#     (mean_query_count['experiment'] == r['experiment']) & (mean_query_count['query-name'] == r['query-name'])][
#     'count'].values[0], axis=1)
# _, axs = plt.subplots(1, 1, figsize=(10, 4))
# mean_unique_plans = mean_unique_plans.sort_values(['experiment', 'no_of_plans', 'query-name'])
# mean_unique_plans[mean_unique_plans['experiment'] == 'epsilon-rnd-min-random-0'].plot(kind='bar', y='sampled',
#                                                                                       x='query-name', ax=axs)
# mean_unique_plans[mean_unique_plans['experiment'] == 'epsilon-rnd-min-random-0'].plot(kind='bar', y='mean-unique-qps',
#                                                                                       x='query-name', ax=axs,
#                                                                                       color='orange',
#                                                                                       linestyle='dashed',
#                                                                                       edgecolor='orange')
# plt.ylabel('number of samples')
# plt.xlabel('query name')
# plt.title('Number of query sampled vs. mean unique query plans observed (100% Exploration)')
# plt.title('Number of query being sampled vs. mean unique qps observed')
# plt.show()
#
# mean_unique_plans[mean_unique_plans['experiment'] == 'epsilon-rnd'].boxplot(column='mean-unique-qps')
# plt.show()

print('lol')
