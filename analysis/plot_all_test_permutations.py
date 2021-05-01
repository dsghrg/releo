import pandas as pd
import matplotlib.pyplot as plt
from colour import Color



ALL_PERMUTATIONS = './testset-reference-time.csv'
BENCHMARK = './benchmark-reference-time.csv'
TRAIN_EVAL = './logs/asdf/train-eval-log.csv'


def lighten_color(color, percentage):
    num = int(color, 16)
    amt = round(2.55 * percentage)
    r = (num >> 16) + amt
    b = (num >> 8 & 0x00FF) + amt
    g = (num & 0x0000FF) + amt

    if r < 255:
        if r < 1:
            r = 0
    else:
        r = 255

    if b < 255:
        if b < 1:
            b = 0
    else:
        b = 255

    if g < 255:
        if g < 1:
            g = 0
    else:
        g = 255

    return str(hex((0x1000000 + r*0x10000 + b*0x100 + g)))


def plot_results(filepath, benchmark_filepath, train_eval_filepath):
    all_permutations = pd.read_csv(filepath)
    sorted_perm = all_permutations.sort_values('mean-exec-time')

    benchmark_df = pd.read_csv(benchmark_filepath)
    benchmark_dict = dict(zip(benchmark_df['logical-query'], benchmark_df['mean-exec-time']))

    train_eval_df = pd.read_csv(train_eval_filepath)
    train_eval_df['join-order'] = train_eval_df['join-order'].str.replace('[', '(').str.replace(']', ')')
    ep_order = dict(zip(train_eval_df['join-order'], train_eval_df['episode']))
    num_episodes = max(train_eval_df['episode'].unique())
    eval_dict = {}

    all_logical_queries = sorted_perm['logical-query'].unique()
    bar_dict = {}

    for logical_query in all_logical_queries:
        bar_dict[logical_query] = [[], [], []]
        eval_dict[logical_query] = [[], []]

    for i, row in train_eval_df.iterrows():
        logical_query = row['logical-query']
        episode = row['episode']
        join_order = row['join-order']

        eval_dict[logical_query][0].append(episode)
        eval_dict[logical_query][1].append(join_order)

    for i, row in sorted_perm.iterrows():
        logical_query = row['logical-query']
        order = row['order']
        record_id = row['record-id']
        exec_time = row['mean-exec-time']

        bar_dict[logical_query][0].append(record_id)
        bar_dict[logical_query][1].append(order)
        bar_dict[logical_query][2].append(exec_time)

    # fig, ax = plt.subplots(len(all_logical_queries))
    #
    # fig.set_figwidth(15)
    # fig.set_figheight(15)
    # for i, ax_i in enumerate(ax):
    #     curren_logical_query = all_logical_queries[i]
    #     current_entry = bar_dict[curren_logical_query]
    #     ax_i.bar(current_entry[0], current_entry[2])
    #     # ax_i.axhline(y=csv_mean, color='orange', label='mean')
    #     # ax_i.axhline(y=csv_std, color='green', label='standard deviation')
    #     # ax_i.legend()
    #     ax_i.set_title(curren_logical_query)
    red = Color("yellow")
    color_grade = list(red.range_to(Color("red"), num_episodes+1))
    for current_logical_query in all_logical_queries:
        f = plt.figure()
        f.set_figwidth(15)
        plt.axhline(y=benchmark_dict[current_logical_query], color='red', linestyle='--', label='optimized / benchmark')
        # f.set_figheight(1)
        current_entry = bar_dict[current_logical_query]
        curr_orders = eval_dict[current_logical_query][1]

        colors = []
        for entry in current_entry[1]:
        # for entry in curr_orders:
            try:
                # index = current_entry[1].index(entry)
                index = curr_orders.index(entry)
            except ValueError:
                index = None
            if index is not None:
                # ep_ratio = e§ / num_episodes
                colors.append(color_grade[ep_order[entry]].hex)
                # colors.append(lighten_color('FFA500', ep_ratio).replace('0x', '#'))
            else:
                colors.append('lightgrey')


        # colors = ['orange' if entry in curr_orders else 'lightgrey' for entry in current_entry[1]]
        plt.bar(current_entry[1], current_entry[2], color=colors)
        plt.title(current_logical_query)
        plt.xlabel('query plan')
        plt.ylabel('execution time (ms)')

        plt.show()

    # print(bar_dict)

    # benchmark_df = pd.read_csv(filepath + '/' + benchmark_filename)
    # benchmark_dict = dict(zip(benchmark_df['logical-query'], benchmark_df['exec-time']))
    # eval_line_dict = {}
    #
    # # init dict with the logical query and two empty arrays
    # for logical_query in all_logical_queries:
    #     eval_line_dict[logical_query] = [[], [], []]
    #
    # # fill the dict with episode and the corresponding exec time to the logical query
    # for i, row in eval_df.iterrows():
    #     eval_logical_query = row['logical-query']
    #     exec_time = float(row['exec-time'])
    #     episode = int(float(row['episode']))
    #     eval_line_dict[eval_logical_query][0].append(episode)
    #     exec_time_rel = exec_time / float(benchmark_dict[eval_logical_query])
    #     eval_line_dict[eval_logical_query][1].append(exec_time)
    #     eval_line_dict[eval_logical_query][2].append(exec_time_rel)
    #
    # __get_plot(eval_line_dict, True).savefig(filepath + '/relative')
    # __get_plot(eval_line_dict, False).savefig(filepath + '/absolute')


plot_results(ALL_PERMUTATIONS, BENCHMARK, TRAIN_EVAL)
# print(lighten_color('1', 0.5))
