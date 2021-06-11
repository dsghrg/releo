import glob
import os

import matplotlib.pyplot as plt
import numpy as np
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
# label_lookup = {'min-random-0': 'exploration rate - 0%', 'min-random-5': 'exploration rate - 5%',
#                 'min-random-10': 'exploration rate - 10%', 'min-random-20': 'exploration rate - 20%',
#                 'min-random-33': 'exploration rate - 33%', 'min-random-50': 'exploration rate - 50%',
#                 'min-random-75': 'exploration rate - 75%', 'min-random-100': 'exploration rate - 100%'}
# exps = ['nn-l', 'nn-m', 'nn-s', 'nn-xl', 'nn-xs', 'nn-xxl', 'min-random-20']
# label_lookup = {'nn-l': 'nn-l', 'nn-m': 'nn-m', 'nn-s': 'nn-s', 'nn-xl': 'nn-xl', 'nn-xs': 'nn-xs', 'nn-xxl': 'nn-xxl',
#                 'min-random-20': 'default'}
# exps = ['query-gen-rnd', 'query-gen-epsilon-rnd', 'epsilon-rnd']
# exps = ['100ep', '150ep', 'min-random-20']


# exps = ['epsilon-rnd-min-random-0', 'epsilon-rnd-min-random-10', 'epsilon-rnd', 'ba-epsilon-rnd']
# label_lookup = {'epsilon-rnd-min-random-0': 'epsilon random - explore 0%',
#                 'epsilon-rnd-min-random-10': 'epsilon random - explore 10%',
#                 'epsilon-rnd': 'epsilon random - explore 20%',
#                 'ba-epsilon-rnd': 'epsilon random - explore 100%'}

base_dir = '../src/runs/postgres/uniform/'

ref, eval, train, bench = fetch_files(exps, base_dir, True, True)

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
    plt.figure(figsize=(6.4, 4))
    postgres = bench[bench['query-name'] == query_name]
    plt.plot(group['episode'].unique(), [1 for x in group['episode'].unique()], label='best left-deep',
             linestyle='dashed', color='black')
    plt.plot(group['episode'].unique(), [postgres['rel-to-best'] for x in group['episode'].unique()],
             label='postgres optimized',
             linestyle='dashed', color='red')
    for exp in group['experiment'].unique():
        to_plot = group[group['experiment'] == exp]
        plt.plot(to_plot['episode'], to_plot['mean-rel-to-min'], label=exp)
    plt.xlabel('episode')
    plt.ylabel('relative performance to baseline')
    plt.title('Performance relative to best left-deep for query ' + str(query_name))
    plt.legend(loc='lower left')
    plt.show()

xs = performance_per_query['episode'].unique()
plt.figure(figsize=(6.4, 4))
plt.plot(xs, [1 for x in xs], label='baseline (best left deep)', linestyle='dashed', color='black')
mean_bench_rel = bench['rel-to-best'].mean()
plt.plot(xs, [mean_bench_rel for x in xs], linestyle='dashed', color='red', label='postgres optimized')
for exp, group in performance_per_query.groupby(['experiment'], as_index=False):
    # plt.plot(group['episode'].unique(), [1 for x in group['episode'].unique()], label='best left-deep',
    #          linestyle='dashed', color='black')
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
    plt.legend(loc='lower left')
    plt.title('Exploration rate - Performance overall')
plt.show()

ax = performance_per_query[performance_per_query['episode'] == performance_per_query['episode'].max()].boxplot(
    by='experiment', column='std-rel', showmeans=True, figsize=(8, 4), rot=45)
ax.get_figure().suptitle('')
plt.ylabel('standard deviation relative to mean')
plt.xlabel(None)
plt.title('Distribution of standard deviation relative to the mean after training')
labels = [label_lookup[item.get_text()] for item in ax.get_xticklabels()]
ax.set_xticklabels(labels)
plt.show()

unique_qps_per_query = ref.groupby('query-name')['join-order'].agg(no_of_plans='count').reset_index()
mean_unique_plans = \
    train.groupby(['experiment', 'query-name', 'run-name'], as_index=False)['join-order'].nunique().groupby(
        ['experiment', 'query-name'], as_index=False)['join-order'].mean()

mean_query_count = \
    train.groupby(['experiment', 'run-name', 'query-name'])['query-name'].agg(count='count').reset_index().groupby(
        ['experiment', 'query-name'], as_index=False)['count'].mean()

ref_dist = mean_query_count[mean_query_count['experiment'] == 'min-random-20']
ref_dist['bin-filled'] = ref_dist.apply(lambda r: min(r['count'], unique_qps_per_query[
    unique_qps_per_query['query-name'] == r['query-name']]['no_of_plans'].values[0]), axis=1)

_, ax = plt.subplots()
ref_dist.plot(kind='bar', x='query-name', y='count', rot=45, figsize=(8, 4),
              label='number of times query is sampled during training', ax=ax)
ref_dist.plot(kind='bar', x='query-name', y='bin-filled', rot=45, figsize=(8, 4),
              label='number of unique existing query plans', ax=ax, color='orange')

# ref_dist.plot(kind='bar', x='query')
plt.xlabel('query name')
plt.ylabel('number of plans')
plt.legend()
plt.title('Distribution of queries seen for a single training run vs. existing query plans for a query')
plt.show()

ref2 = train.groupby(['experiment', 'run-name', 'query-name'], as_index=False)['join-order'].nunique().groupby(
    ['experiment', 'query-name'], as_index=False)['join-order'].mean()

_, axs = plt.subplots(1, 1, figsize=(8, 4))
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

# reff = ref2[ref2['experiment'] == 'min-random-100']
# ref_dist.plot(kind='bar', x='query-name', y='count', rot=45,
#               label='number of times query is sampled during training', ax=axs[1])
# ref_dist.plot(kind='bar', x='query-name', y='bin-filled', rot=45,
#               label='number of unique existing query plans', ax=axs[1], color='orange')
# reff.plot(kind='bar', x='query-name', y='join-order', rot=45,
#           label='unique plans seen', ax=axs[1], color='green')
# axs[1].set_title('Mean number of unique query plans for queries during training (20% exploring)')
# axs[1].set_ylabel('number of plans')
# axs[1].set_xlabel('query name')
#
reff = ref2[ref2['experiment'] == 'min-random-20']
ref_dist.plot(kind='bar', x='query-name', y='count', rot=45,
              label='number of times query is sampled during training', ax=axs)
ref_dist.plot(kind='bar', x='query-name', y='bin-filled', rot=45,
              label='number of unique existing query plans', ax=axs, color='orange')
reff.plot(kind='bar', x='query-name', y='join-order', rot=45,
          label='unique plans seen', ax=axs, color='green')
axs.set_title('Mean number of unique query plans for queries during training (20% exploring)')
axs.set_ylabel('number of plans')
axs.set_xlabel('query name')

plt.show()

mean_unique_plans['percentage-of-sub-searchspace'] = mean_unique_plans.apply(
    lambda rec: rec['join-order'] / mean_query_count[
        mean_query_count['query-name'] == rec['query-name']]['count'].values[0], axis=1)

# mean_unique_plans['percentage-of-sub-searchspace'] = mean_unique_plans.apply(lambda rec: rec['join-order'] / (200 / 30),
#                                                                              axis=1)

mean_unique_plans['eps-val'] = mean_unique_plans['experiment'].apply(lambda x: int(x.split('-')[-1]))
mean_unique_plans = mean_unique_plans.sort_values('eps-val', ascending=True)
mean_unique_plans['max-qps'] = mean_unique_plans['query-name'].apply(
    lambda n: unique_qps_per_query[unique_qps_per_query['query-name'] == n]['no_of_plans'].values[0])

# plt.scatter(mean_unique_plans['max-qps'], mean_unique_plans['eps-val'],
#             c=(1 - mean_unique_plans['percentage-of-sub-searchspace']), s=175, cmap='gray')
# plt.gray()
b = mean_unique_plans.groupby(['eps-val', 'max-qps'])['percentage-of-sub-searchspace'].agg(
    coverage='mean').reset_index()
t = b.pivot(columns='max-qps', index='eps-val', values='coverage')
X, Y = np.meshgrid(t.columns.values, t.index.values)
plt.pcolormesh(X, Y, t.values)
plt.colorbar()
plt.ylabel('exploration rate (%)')
plt.xlabel('number of unique query plans for a query')
plt.title('Coverage of unique qps in rel to exploration rate and possible qps')
plt.show()

complex_queries = ['Q37', 'Q36', 'Q31', 'Q20']
easy_query = ['Q1', 'Q7', 'Q5']
mean_unique_plans['coverage-by-exp'] = mean_unique_plans.groupby('experiment').transform('mean')[
    'percentage-of-sub-searchspace']
mean_unique_plans.plot(x='eps-val', y='coverage-by-exp', by='experiment', label='mean coverage (%)')
for query_name, group in mean_unique_plans[mean_unique_plans['query-name'].isin(complex_queries + easy_query)].groupby(
        'query-name',
        as_index=False):
    # for query_name, group in mean_unique_plans.groupby('query-name', as_index=False):
    plt.plot(group['eps-val'], group['percentage-of-sub-searchspace'], '--o', label=query_name)
plt.ylabel('percentage of search space for query covered')
plt.xlabel('exploration rate')
plt.title('Percentage of covered search space for a query vs. exploration rate')
plt.ylim((0, 1))
plt.legend()
plt.show()

print('lol')
