import pandas as pd
import matplotlib.pyplot as plt
import os
import glob

all_permutations_files = glob.glob('../logs/releo-all-test-permutations/**/testset-all-permutations-log.csv')
all_perms = []

for file in all_permutations_files:
    all_perms.append(pd.read_csv(file, sep=','))

all_recs = pd.concat(all_perms)

all_recs['order'] = all_recs['order'].apply(lambda x: x.replace('(', '[').replace(')', ']'))
all_recs = all_recs[(all_recs['order'] == "['order_details', 'order', 'product', 'customer', 'subcategory', 'category', 'deliverer', 'discount']")
                    | (all_recs['order'] == "['order_details', 'product', 'order', 'deliverer', 'subcategory', 'category', 'discount', 'customer']") ]
grouped_order = all_recs.groupby('order')
for name, group in grouped_order:
    desc = group.describe()['exec-time']
    ax = group.boxplot(['exec-time'], showmeans=True, figsize=(12, 5))
    ax.set_title(name)
    text = ' n={:2f}\nmean={:2f}\n std={:2f}\n max={:2f}\n min={:2f}'.format(desc['count'], desc['mean'], desc['std'],
                                                                             desc['max'], desc['min'])
    ax.annotate(text, xy=(0.05, 0.8), xycoords='axes fraction')
    plt.show()

# top_n_var = grouped_order['exec-time'].std().sort_values(ascending=False).head(100)
# top_n_var['order'] = top_n_var.index
# relevant_recs = pd.merge(all_recs, top_n_var, how='inner', on=['order'])
# relevant_recs.to_csv('plans-with-highest-variance.csv', sep=',')


# least_n_var = grouped_order['exec-time'].std().sort_values(ascending=True).head(100)
# least_n_var['order'] = least_n_var.index
# relevant_recs_least = pd.merge(all_recs, least_n_var, how='inner', on=['order'])
# relevant_recs_least.to_csv('plans-with-lowest-variance.csv', sep=',')

# grouped_order['mean-exec-time'] = grouped_order['exec-time'].mean()
#
# grouped_order = grouped_order.sort_values(['mean-exec-time'], ascending=True)

# print(top_n_var)
