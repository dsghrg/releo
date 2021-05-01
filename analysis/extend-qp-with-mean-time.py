import pandas as pd

train_eval = pd.read_csv('./logs/asdf/train-eval-log.csv')

mean_times = pd.read_csv('./testset-reference-time.csv')
mean_times['order'] = mean_times['order'].apply(lambda x: x.replace('(', '[').replace(')', ']'))

train_eval['mean-exec-time'] = train_eval['join-order'].apply(
    lambda x: mean_times[mean_times['order'] == x]['mean-exec-time'].values[0])

train_eval.to_csv('./train-eval-log-extended.csv')
