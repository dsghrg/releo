import pandas as pd
import glob

# all_permutations_files = glob.glob('../../input_runs/local-no-parallel/releo-run-2021-05-08-14-57-42/train-log.csv')
# all_permutations_files = glob.glob('../input_runs/local-hard-no-parallel/releo-run-2021-05-09-14-16-23/train-log.csv')
# all_permutations_files = glob.glob('../input_runs/local-hard-no-parallel/releo-run-2021-05-09-22-14-40/train-log.csv')
all_permutations_files = glob.glob('../input_runs/local-runs/releo-run-2/train-log.csv')
all_perms = []

for file in all_permutations_files:
    all_perms.append(pd.read_csv(file, sep=','))

all_recs = all_perms[0]

all_recs['order'] = all_recs['join-order'].apply(lambda x: x.replace('(', '[').replace(')', ']'))
grouped_order = all_recs.groupby('order')
top_var_n = grouped_order['exec-time'].std().sort_values(ascending=False).head(100)
top_var_n['order'] = top_var_n.index
relevant_recs = pd.merge(all_recs, top_var_n, how='inner', on=['order'])
relevant_recs = relevant_recs.sort_values(['exec-time_y'], ascending=False)
# relevant_recs.to_csv('run-train-highest-variance-local-bad-server.csv', sep=',')

print(top_var_n)
