import pandas as pd
import matplotlib.pyplot as plt

mean_times = pd.read_csv('./testset-reference-time.csv')
train_eval = pd.read_csv('./logs/asdf/train-eval-log.csv')

mean_times = mean_times.sort_values(['mean-exec-time'], ascending=True).groupby('logical-query')

for name, group in mean_times:
    group = group.reset_index()
    group['order'] = group['order'].apply(lambda x: x.replace('(', '[').replace(')', ']'))
    x_vals = [str(x_val) for x_val in group.index.values]
    y_vals = group['mean-exec-time']
    lim_min_y = y_vals.min() * 0.95
    lim_max_y = y_vals.max() * 1.05
    bars = plt.bar(x_vals, y_vals)
    for idx, bar in enumerate(bars):
        if train_eval[train_eval['join-order'] == group.iloc[idx]['order']].size > 0:
            bar.set_color('red')
    plt.ylim((lim_min_y, lim_max_y))
    plt.show()
