import collections
import matplotlib.pyplot as plt
import pandas as pd

eval_per_logical_q = collections.defaultdict(dict)


def __get_plot(line_dict, relative):
    fig = plt.figure()
    fig.set_figwidth(15)
    fig.set_figheight(5)
    ax = plt.subplot(111)
    time_index = 2 if relative else 1
    for logical_query, times in line_dict.items():
        ax.plot(times[0], times[time_index], label=logical_query)
    # Shrink current axis by 50%
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.5, box.height])
    if relative:
        plt.axhline(y=1, color='black', linestyle='--', label='optimized / benchmark')
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    plt.xlabel('episode (query plan)')
    plt.ylabel('our prediction(ms) / optimizer(ms)')
    return plt


def plot_results(filepath, benchmark_filename, train_eval_filename):
    eval_df = pd.read_csv(filepath + '/' + train_eval_filename)
    benchmark_df = pd.read_csv(filepath + '/' + benchmark_filename)
    benchmark_dict = dict(zip(benchmark_df['logical-query'], benchmark_df['exec-time']))
    eval_line_dict = {}
    all_logical_queries = eval_df['logical-query'].unique()

    # init dict with the logical query and two empty arrays
    for logical_query in all_logical_queries:
        eval_line_dict[logical_query] = [[], [], []]

    # fill the dict with episode and the corresponding exec time to the logical query
    for i, row in eval_df.iterrows():
        eval_logical_query = row['logical-query']
        exec_time = float(row['exec-time'])
        episode = int(float(row['episode']))
        eval_line_dict[eval_logical_query][0].append(episode)
        exec_time_rel = exec_time / float(benchmark_dict[eval_logical_query])
        eval_line_dict[eval_logical_query][1].append(exec_time)
        eval_line_dict[eval_logical_query][2].append(exec_time_rel)

    __get_plot(eval_line_dict, True).savefig(filepath + '/relative')
    __get_plot(eval_line_dict, False).savefig(filepath + '/absolute')
