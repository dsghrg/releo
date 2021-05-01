import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

base_dir = './logs/releo-all-test-permutations'
run1_bench = pd.read_csv(base_dir + '/releo-run-2021-04-26-14-42-40/eval-set-benchmarking-log.csv')
run2_bench = pd.read_csv(base_dir + '/releo-run-2021-04-28-07-14-58/eval-set-benchmarking-log.csv')
run3_bench = pd.read_csv(base_dir + '/releo-run-2021-04-29-17-48-19/eval-set-benchmarking-log.csv')
runs_bench = [run1_bench, run2_bench, run3_bench]

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

reference_bench = run1_bench
reference_bench.to_csv('benchmark-reference-time.csv', sep=',')

run1_perms = pd.read_csv(base_dir + '/releo-run-2021-04-26-14-42-40/testset-all-permutations-log.csv')
run2_perms = pd.read_csv(base_dir + '/releo-run-2021-04-28-07-14-58/testset-all-permutations-log.csv')
run3_perms = pd.read_csv(base_dir + '/releo-run-2021-04-29-17-48-19/testset-all-permutations-log.csv')
runs_perm = [run1_perms, run2_perms, run3_perms]

merged = pd.concat(runs_perm)
grouped_by_order = merged.groupby(['order'], as_index=False)['exec-time']
mean_exec = grouped_by_order.mean()
std_exec = grouped_by_order.std()

for run in runs_perm:
    run['mean-exec-time'] = np.nan
    run['mean-exec-time'] = run['order'].apply(lambda x: mean_exec[mean_exec['order'] == x]['exec-time'].values[0])
    run['std-exec-time'] = run['order'].apply(lambda x: std_exec[mean_exec['order'] == x]['exec-time'].values[0])

reference_set = run1_perms
reference_set.to_csv('testset-reference-time.csv', sep=',')

reference_set = reference_set.sort_values(['mean-exec-time'], ascending=True).groupby('logical-query')

for name, group in reference_set:
    fig, ax = plt.subplots(2, 1, figsize=(15, 10))
    logical_query_name = group['logical-query'].unique()[0]
    x_vals = range(0, group.shape[0])
    reference_record = reference_bench[reference_bench['logical-query'] == logical_query_name]
    bench_mean = reference_record['mean-exec-time'].values[0]
    ax[0].bar(x_vals, group['mean-exec-time'])
    ax[0].plot(x_vals, [bench_mean for x in x_vals], color='red')
    ax[0].set_title(name + ' mean execution time ordered asc')
    ax[0].set_ylim(
        [min([min(group['mean-exec-time']), bench_mean]) * 0.95,
         max([max(group['mean-exec-time']), bench_mean]) * 1.05])

    ax[1].bar(x_vals, group['std-exec-time'])
    ax[1].set_title(name + ' std execution time ordered by mean asc')
    plt.show()
print('finito')
