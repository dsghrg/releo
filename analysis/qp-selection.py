import glob
import pandas as pd
import matplotlib.pyplot as plt

base_dir = './input_runs/all-permutations/uniform'

files = glob.glob(base_dir + '/**/all-permutations-log.csv')

frames = [pd.read_csv(file) for file in files]

all_qp = pd.concat(frames)

# all_qp.groupby(['logical-query']).boxplot(column=['exec-time'], figsize=(40, 20))
# plt.show()

std_lookup = {name: group['exec-time'].std() for name, group in all_qp.groupby(['join-order'])}
all_qp['std-exec'] = all_qp['join-order'].apply(lambda join_order: std_lookup[join_order])

less_than_100ms = all_qp[all_qp['std-exec'] < 100]
remaining = all_qp[all_qp['std-exec'] >= 100]
remaining = remaining.groupby(['join-order'], as_index=False)['exec-time'].max()

all_remainder = None
for idx, row in remaining.iterrows():
    rem = all_qp[(all_qp['join-order'] == row['join-order']) & (all_qp['exec-time'] == row['exec-time'])]
    if all_remainder is None:
        all_remainder = rem
    else:
        all_remainder = pd.concat([all_remainder, rem])
    all_remainder.append(rem)

less_than_100ms = less_than_100ms.drop_duplicates(subset=['join-order'])
ref_queries = pd.concat([all_remainder, less_than_100ms])

ref_queries.to_csv(base_dir + '/all-permutations-reference.csv')
all_remainder.to_csv(base_dir + '/max-exec-time-qps.csv')
less_than_100ms.to_csv(base_dir + '/less-than-100-ms.csv')

print('lol')
# groups = all_qp.groupby(['join-order'])
# groups['exec-time'].std().plot(kind='bar')
# plt.show()

# for name, group in groups:
#     group['exec-time'].std().plot(kind='bar')
