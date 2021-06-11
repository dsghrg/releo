import pandas as pd
import matplotlib.pyplot as plt


def get_table_names(query_string):
    return [qry.strip() for qry in query_string.replace('[', '').replace(']', '').replace("'", '').split(',')]


def logicall_eq(query1, query2):
    tb1 = set(get_table_names(query1))
    tb2 = set(get_table_names(query2))
    return tb1 == tb2

xs = [x for x in range(0,200)]

plt.figure(figsize=(5.3,3.25))
plt.plot(xs,  [0 for x in xs], label='min-random-0')
plt.plot(xs,  [max(1*0.9705**x, 0.05) for x in xs], label='min-random-5')
plt.plot(xs,  [max(1*0.97725**x, 0.1) for x in xs], label='min-random-10')
plt.plot(xs,  [max(1*0.984**x, 0.2) for x in xs], label='min-random-20')
plt.plot(xs,  [max(1*0.989**x, 0.33) for x in xs], label='min-random-33')
plt.plot(xs,  [max(1*0.9932**x, 0.50) for x in xs], label='min-random-50')
plt.plot(xs,  [max(1*0.9971475**x, 0.75) for x in xs], label='min-random-75')
plt.plot(xs,  [1 for x in xs], label='min-random-100')
plt.title('Epsilon decay over episode for different setups')
plt.ylabel('epsilon')
plt.xlabel('episode')
plt.legend()
plt.show()


train_eval = pd.read_csv('./input_runs/local-runs/releo-run-2/train-eval-log.csv', sep=',')

qname_lookup = pd.read_csv('query-name-lookup.csv')

train_eval['query-name'] = train_eval['logical-query'].apply(lambda log_query: qname_lookup[
    qname_lookup['logical-query'].apply(lambda r: logicall_eq(r, log_query))]['query-name'].values[0])

grouped = train_eval.groupby('query-name')
i = 0
for name, group in grouped:
    desc = group.describe()['exec-time']
    ax = group.boxplot(showmeans=True, figsize=(6.4, 4), column='exec-time')
    ax.set_title('Execution time distribution for ' + name)
    text = ' n={:2f}\nmean={:2f}\n std={:2f}\n max={:2f}\n min={:2f}'.format(desc['count'], desc['mean'], desc['std'],
                                                                             desc['max'], desc['min'])
    ax.annotate(text, xy=(0.05, 0.8), xycoords='axes fraction')
    plt.savefig('./candle-charts/' + str(i))
    plt.show()
    i += 1

