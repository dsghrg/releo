import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import glob


def get_table_names(query_string):
    return [qry.strip() for qry in query_string.replace('[', '').replace(']', '').replace("'", '').split(',')]


def logicall_eq(query1, query2):
    tb1 = set(get_table_names(query1))
    tb2 = set(get_table_names(query2))
    return tb1 == tb2


base_dir = './input_runs/local-force-parallel'
# all_bench_files = glob.glob(base_dir + '/local-hard-no-parallel/**/eval-set-benchmarking-log.csv')
# all_bench_files = glob.glob(base_dir + '/local-runs/**/eval-set-benchmarking-log.csv')
# all_bench_files = glob.glob(base_dir + '/**/eval-set-benchmarking-log.csv')
all_bench_files = glob.glob(base_dir + '/**/eval-set-benchmarking-log.csv')

runs_bench = []
for file in all_bench_files:
    runs_bench.append(pd.read_csv(file, sep=','))

# runs_bench = [run1_bench, run2_bench, run3_bench]
#
merged_bench = pd.concat(runs_bench)
grouped_by_query = merged_bench.groupby(['logical-query'], as_index=False)['exec-time']
mean_exec = grouped_by_query.mean()
std_exec = grouped_by_query.std()

for run in runs_bench:
    run['mean-exec-time'] = np.nan
    run['mean-exec-time'] = run['logical-query'].apply(
        lambda x: mean_exec[mean_exec['logical-query'] == x]['exec-time'].values[0])
    run['std-exec-time'] = run['logical-query'].apply(
        lambda x: std_exec[mean_exec['logical-query'] == x]['exec-time'].values[0])

reference_bench = runs_bench[0]
reference_bench.to_csv('benchmark-reference-time-local-force-parallel.csv', sep=',')

all_perms_files = glob.glob(base_dir + '/**/testset-all-permutations-log.csv')
runs_perm = []
for file in all_perms_files:
    runs_perm.append(pd.read_csv(file, sep=','))

for idx, run in enumerate(runs_perm):
    run = run.sort_values(['exec-time'], ascending=True)
    runs_perm[idx]['position'] = run.groupby('logical-query').cumcount()
    runs_perm[idx]['query-name'] = run['logical-query'].apply(lambda log_query: qname_lookup[
        qname_lookup['logical-query'].apply(lambda r: logicall_eq(r, log_query))]['query-name'].values[0])
    runs_perm[idx] = runs_perm[idx].rename(columns={'order': 'join-order'})

merged = pd.concat(runs_perm)
merged = merged[merged['query-name'] == "Q38"]

t = merged.groupby('join-order')['exec-time'].agg(std='std', mean='mean').reset_index()
t = t.sort_values('std', ascending=False)

plt.figure(figsize=(6.4, 4))
a = merged[merged['join-order'] == t.loc[372]['join-order']]
ax = a.boxplot(column='exec-time', showmeans=True)
plt.ylabel('execution time (ms)')
plt.title('Distribution of execution time for query plan with high deviation')
plt.show()

grouped_by_order = merged.groupby(['join-order'], as_index=False)
mean_exec = grouped_by_order['exec-time'].mean()
std_exec = grouped_by_order['exec-time'].std()
mean_position = grouped_by_order['position'].mean()
std_position = grouped_by_order['position'].std()

for idx, run in enumerate(runs_perm):
    run = run[run['query-name'] == "Q38"].copy()
    run['mean-exec-time'] = run['join-order'].apply(
        lambda x: mean_exec[mean_exec['join-order'] == x]['exec-time'].values[0])
    run['std-exec-time'] = run['join-order'].apply(
        lambda x: std_exec[std_exec['join-order'] == x]['exec-time'].values[0])
    run['mean-position'] = run['join-order'].apply(
        lambda x: mean_position[mean_position['join-order'] == x]['position'].values[0])
    run['std-position'] = run['join-order'].apply(
        lambda x: std_position[std_position['join-order'] == x]['position'].values[0])
    runs_perm[idx] = run
    # run['position-diff'] = run['join-order'].apply(lambda x: abs(
    #     run[run['join-order'] == x]['position'].values[0] - mean_position[mean_position['join-order'] == x]['position'].values[
    #         0]))

reference_set = runs_perm[0]
reference_set.to_csv('testset-reference-time-local-force-parallel.csv', sep=',')

reference_set = reference_set.sort_values(['mean-exec-time'], ascending=True).groupby('query-name')

i = 0
for name, group in reference_set:
    fig, ax = plt.subplots(2, 1, figsize=(8, 6))
    logical_query_name = group['logical-query'].unique()[0]
    x_vals = range(0, group.shape[0])
    reference_record = reference_bench[reference_bench['logical-query'] == logical_query_name]
    bench_mean = reference_record['mean-exec-time'].values[0]
    ax[0].bar(x_vals, group['mean-exec-time'])
    ax[0].plot(x_vals, [bench_mean for x in x_vals], color='red')
    ax[0].set_title('mean Ausführungszeit aufsteigend von ' + name)
    ax[0].set_ylim(
        [min([min(group['mean-exec-time'])]) * 0.95,
         max([max(group['mean-exec-time'])]) * 1.05])
    ax[0].set_ylabel('(ms)')
    ax[0].legend()

    ax[1].bar(x_vals, group['std-exec-time'],
              label='standard deviation of execution time (ms), n=' + str(len(all_perms_files)))
    ax[1].set_title('Latency standard deviation of execution plans for ' + name)
    ax[1].set_ylabel('(ms)')
    ax[1].legend()
    # ax[2].bar(x_vals, group['position-diff'])
    # ax[2].set_title(name + 'standardabweichung der position in der Ordnung')
    # plt.savefig('./mean-execution-time/local/' + str(i))
    plt.legend()
    plt.show()
    i += 1
print('finito')
