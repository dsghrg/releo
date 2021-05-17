import pandas as pd
import matplotlib.pyplot as plt

train_eval = pd.read_csv('./input_runs/local-runs/releo-run-2/train-eval-log.csv', sep=',')[['join-order', 'exec-time']]

grouped = train_eval.groupby('join-order')
i = 0
for name, group in grouped:
    desc = group.describe()['exec-time']
    ax = group.boxplot(showmeans=True, figsize=(12, 5))
    ax.set_title(name)
    text = ' n={:2f}\nmean={:2f}\n std={:2f}\n max={:2f}\n min={:2f}'.format(desc['count'], desc['mean'], desc['std'],
                                                                             desc['max'], desc['min'])
    ax.annotate(text, xy=(0.05, 0.8), xycoords='axes fraction')
    plt.savefig('./candle-charts/' + str(i))
    plt.show()
    i += 1

