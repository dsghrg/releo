import glob
import matplotlib.pyplot as plt
import pandas as pd

base_dir = '../src/runs/postgres/uniform/'


# base_dir = '../src/runs/postgres/non-uniform/'
# group_runs = ['100ep', '150ep', 'epsilon-rnd', 'min-epsilon-33', 'min-random-20', 'nn-l', 'nn-m', 'nn-s', 'nn-xl',
#               'nn-xs', 'nn-xxl', 'query-gen-epsilon-rnd', 'query-gen-rnd']
#

def get_table_names(query_string):
    return [qry.strip() for qry in query_string.replace('[', '').replace(']', '').replace("'", '').split(',')]


def logicall_eq(query1, query2):
    tb1 = set(get_table_names(query1))
    tb2 = set(get_table_names(query2))
    return tb1 == tb2


# group_runs = ['min-random-0', 'min-random-5', 'min-random-10', 'min-random-20', 'min-random-33', 'min-random-50', 'min-random-75', 'min-random-100']
group_runs = ['nn-xl', 'min-random-20']
# group_runs = ['min-random-20']
# group_runs = ['nn-l', 'nn-m', 'nn-s', 'nn-xl', 'nn-xs', 'nn-xxl', 'min-random-20']
reference = pd.read_csv('../src/db/executor/lookup-files/postgres/shop-db-uniform-reference.csv')
qname_lookup = pd.read_csv('query-name-lookup.csv')

reference['query-name'] = reference['logical-query'].apply(lambda log_query: qname_lookup[
    qname_lookup['logical-query'].apply(lambda r: logicall_eq(r, log_query))]['query-name'].values[0])
reference_by_query = reference.groupby('logical-query', as_index=False)

for group_run in group_runs:
    train_eval_logs = [pd.read_csv(filepath) for filepath in glob.glob(base_dir + group_run + '/**/train-eval-log.csv')]
    benchmarking_logs = [pd.read_csv(filepath) for filepath in
                         glob.glob(base_dir + group_run + '/**/eval-set-benchmarking-log.csv')]

    train_eval_log = pd.concat(train_eval_logs)
    benchmarking_log = pd.concat(benchmarking_logs)
    train_eval_log['query-name'] = train_eval_log['logical-query'].apply(lambda log_query: qname_lookup[
        qname_lookup['logical-query'].apply(lambda r: logicall_eq(r, log_query))]['query-name'].values[0])

    benchmarking_log['query-name'] = benchmarking_log['logical-query'].apply(lambda log_query: qname_lookup[
        qname_lookup['logical-query'].apply(lambda r: logicall_eq(r, log_query))]['query-name'].values[0])

    benchmark_by_query = benchmarking_log.groupby('query-name', as_index=False)
    for test_query, group in train_eval_log.groupby('query-name'):
        name = '-'.join(get_table_names(test_query))
        by_episode = group.groupby('episode', as_index=False)['exec-time']
        max_episodes = group['episode'].max()
        mean_exec = by_episode.mean()
        sem = by_episode.sem()
        benchmark = benchmarking_log[benchmarking_log['query-name'] == test_query]['exec-time'].mean()

        ref_queries = reference[reference['query-name'].apply(lambda r: logicall_eq(r, test_query))]

        min = ref_queries['exec-time'].min()
        max = ref_queries['exec-time'].max()

        xs = [ep for ep in mean_exec['episode']]
        bench = [benchmark for x in xs]
        lower = mean_exec - sem
        upper = mean_exec + sem

        fig = plt.figure(figsize=(7, 3.5))
        plt.plot(xs, mean_exec['exec-time'], color='blue', label='Mean execution time (ms)')
        plt.fill_between(xs, lower['exec-time'], upper['exec-time'], color='blue', alpha=0.1,
                         label='Std-error of mean (interval)')
        plt.plot(xs, [benchmark for x in xs], color='red', label='PostgreSQL optimized (ms)')
        plt.plot(xs, [min for x in xs], color='black', linestyle='--', label='Best left-deep plan (ms)')
        plt.title('Performance of query ' + test_query + ' over time (episode)')
        plt.ylabel('execution time (ms)')
        plt.xlabel('evaluation at episode')
        plt.legend()
        # plt.savefig(base_dir + group_run + '/' + test_query + '.png')
        # plt.savefig('../../ba-doc/report/img/experiments/explore-exploit/non-uniform/' + group_run + '/' + test_query.lower() + '-performance.png')
        plt.show()
        print('lol')
