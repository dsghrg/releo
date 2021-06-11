import matplotlib.pyplot as plt
import pandas as pd
import glob


def get_table_names(query_string):
    return [qry.strip() for qry in query_string.replace('[', '').replace(']', '').replace("'", '').split(',')]


def logicall_eq(query1, query2):
    tb1 = set(get_table_names(query1))
    tb2 = set(get_table_names(query2))
    return tb1 == tb2


# experiment_names = ['min-random-0', 'min-random-5', 'min-random-10', 'min-random-20', 'min-random-33', 'min-random-50',
#                     'min-random-75', 'min-random-100']

experiment_names = ['nn-l', 'nn-m', 'nn-s', 'nn-xl', 'nn-xs', 'nn-xxl', 'min-random-20']
# experiment_names = ['query-gen-rnd', 'query-gen-epsilon-rnd', 'epsilon-rnd']
# experiment_names = ['100ep', '150ep', 'min-random-20']
order_lookup = {'nn-l': 3, 'nn-m': 2, 'nn-s': 1, 'nn-xl': 4, 'nn-xs': 0, 'nn-xxl': 5, 'min-random-20': 6}
label_lookup = {'nn-l': 'nn-l', 'nn-m': 'nn-m', 'nn-s': 'nn-s', 'nn-xl': 'nn-xl', 'nn-xs': 'nn-xs', 'nn-xxl': 'nn-xxl',
                'min-random-20': 'default'}
# order_lookup = {'100ep': 0, '150ep': 1, 'min-random-20': 2}
# order_lookup = {'query-gen-rnd': 0, 'epsilon-rnd': 1, 'query-gen-epsilon-rnd': 2}
# order_lookup = {'min-random-0': 0, 'min-random-5': 1, 'min-random-10': 2, 'min-random-20': 3, 'min-random-33': 4,
#                 'min-random-50': 5,
#                 'min-random-75': 6, 'min-random-100': 7}
base_dir = '../src/runs/postgres/uniform/'
qname_lookup = pd.read_csv('query-name-lookup.csv')
reference = pd.read_csv('../src/db/executor/lookup-files/postgres/shop-db-uniform-reference.csv')
reference['query-name'] = reference['logical-query'].apply(lambda log_query: qname_lookup[
    qname_lookup['logical-query'].apply(lambda r: logicall_eq(r, log_query))]['query-name'].values[0])
experiments = []
train_eval_exp = []
train_exp = []
for group_run in experiment_names:
    df = pd.concat([pd.read_csv(path) for path in glob.glob(base_dir + group_run + '/**/evaluation-log.csv')])
    df['experiment'] = df['logical-query'].apply(lambda x: group_run)
    df_train_eval = []
    for path in glob.glob(base_dir + group_run + '/**/train-eval-log.csv'):
        df_new = pd.read_csv(path)
        df_new['run-name'] = df_new['logical-query'].apply(lambda x: path.split('/')[-2])
        df_new['experiment'] = df_new['logical-query'].apply(lambda x: group_run)
        # df_new['eps-threshold'] = df_new['experiment'].apply(
        #     lambda x: int(group_run.replace('min-random-', '')))
        df_new['query-name'] = df_new['logical-query'].apply(lambda log_query: qname_lookup[
            qname_lookup['logical-query'].apply(lambda r: logicall_eq(r, log_query))]['query-name'].values[0])
        df_train_eval.append(df_new)
    df_train_eval = pd.concat(df_train_eval)

    df_train = []
    for path in glob.glob(base_dir + group_run + '/**/train-log.csv'):
        df_new = pd.read_csv(path)
        df_new['run-name'] = df_new['logical-query'].apply(lambda x: path.split('/')[-2])
        df_new['experiment'] = df_new['logical-query'].apply(lambda x: group_run)
        # df_new['eps-threshold'] = df_new['experiment'].apply(
        #     lambda x: int(group_run.replace('min-random-', '')))
        df_new['query-name'] = df_new['logical-query'].apply(lambda log_query: qname_lookup[
            qname_lookup['logical-query'].apply(lambda r: logicall_eq(r, log_query))]['query-name'].values[0])
        df_train.append(df_new)
    df_train = pd.concat(df_train)
    # df['order'] = df['experiment'].apply(lambda r: int(r.replace('min-random-', '')))
    # df['order'] = df['experiment'].apply(lambda r: int(r.replace('min-random-', '')))
    experiments.append(df)
    train_eval_exp.append(df_train_eval)
    train_exp.append(df_train)

all = pd.concat(experiments)
all['query-name'] = all['logical-query'].apply(lambda log_query: qname_lookup[
    qname_lookup['logical-query'].apply(lambda r: logicall_eq(r, log_query))]['query-name'].values[0])
all_train_eval = pd.concat(train_eval_exp)
all_train = pd.concat(train_exp)

grouped = all_train.groupby(['experiment', 'run-name'], as_index=False)
grouped = grouped['join-order'].nunique().groupby('experiment', as_index=False)['join-order'].mean()
grouped['mean-unique-plans'] = grouped['join-order']
# grouped = grouped.sort_values('eps-threshold')

# grouped.plot(kind='bar', rot=45)
# plt.show()

all_train_eval['mean-unique-plans'] = all_train_eval['experiment'].apply(
    lambda x: grouped[grouped['experiment'] == x]['mean-unique-plans'].values[0])

final_ep = all_train_eval[all_train_eval['episode'] == all_train_eval['episode'].max()]

for name, group in final_ep.groupby('query-name'):
    relevant_eval = final_ep[final_ep['query-name'] == name]
    exp_group = relevant_eval.groupby('experiment')
    mean_exec = exp_group['exec-time'].mean()
    mean_unique_plans = exp_group['mean-unique-plans'].mean()
    # plt.plot(mean_unique_plans, mean_exec, label=name, marker='o', linestyle='dashed')
    plt.scatter(mean_unique_plans, mean_exec, label=name)
plt.ylabel('execution time (ms)')
plt.xlabel('mean number of unique plans seen during training for given exploration rate')
plt.title('Execution time after training based on number of unique plans seen during training phase')
plt.legend()
plt.show()

# all.groupby(['logical-query', 'experiment'])['exec-time'].plot(kind='bar', subplots=True)
for groupname, group in all.groupby('query-name'):
    relevant_train = all_train_eval[all_train_eval['query-name'] == groupname]
    relevant = relevant_train[relevant_train['episode'] == relevant_train['episode'].max()]
    # relevant = all_train_eval[
    #     (all_train_eval['logical-query'] == groupname) & (all_train_eval['epsiode'] == all_train_eval['episode'].max())]
    # relevant = relevant.sort_values(['order'])
    # groups = {qry: idx for idx, qry in enumerate(
    #     relevant.sort_values(['order'], ascending=True)['experiment'].unique())}
    order = [order_lookup[qry] for qry in relevant.groupby(['experiment']).groups]
    ax = relevant.boxplot(by='experiment', column='exec-time', rot=45, positions=list(order), showmeans=True)
    ax.set_title(groupname)
    ax.set_ylabel('execution time (ms)')
    ax.set_xlabel('experiment name')
    plt.title(None)
    plt.legend()
    plt.show()

    xs = relevant_train['episode'].max()
    # plt.plot(xs, [1 for x in xs])
    min = reference[reference['query-name'] == groupname]['exec-time'].min()
    plt.plot([x for x in range(0, xs)], [min for x in range(0, xs)], color='black', linestyle='dashed',
             label='best left-deep plan (ms)')
    for exp_name, group_exp in relevant_train.groupby('experiment', as_index=False):
        mean_exec = group_exp.groupby('episode').mean()['exec-time']
        sem = group_exp.groupby('episode')['exec-time'].sem()
        lower = mean_exec - sem
        upper = mean_exec + sem
        xs = group_exp.sort_values('episode')['episode'].unique()
        # plt.plot(xs, mean_exec, label=label_lookup[exp_name])
        plt.plot(xs, mean_exec, label=exp_name)
        # plt.fill_between(xs, lower, upper, color='blue', alpha=0.1)
    # relevant_train.groupby(['experiment', 'episode'])['exec-time'].plot()
    # for _, group_train in relevant_train.groupby(['experiment','episode']):
    #     plt.plot(xs, group_train['exec-time'].mean())
    plt.legend()
    plt.title('Comparison of execution time for the different setups for query ' + groupname)
    plt.ylabel('execution time (ms)')
    plt.xlabel('epoch')
    plt.show()
