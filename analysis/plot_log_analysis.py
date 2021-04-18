import csv
import pprint as pp
import collections
import matplotlib.pyplot as plt

EVAL_FILE_LOCATION = 'logs/train-eval-log.csv'
BENCHMARK_FILE_LOCATION = 'logs/eval-set-benchmarking-log.csv'

eval_per_logical_q = collections.defaultdict(dict)
# episode_dict = {}

with open(EVAL_FILE_LOCATION) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            print(f'Column names are {", ".join(row)}')
            line_count += 1
        else:
            logical_query = row[4]
            episode = row[1]
            exec_time_in_ms = row[8]

            eval_per_logical_q[logical_query][episode] = exec_time_in_ms
    print(f'Processed {line_count} lines.')

    # pp.pprint(eval_per_logical_q)

benchmark_dict = {}
with open(BENCHMARK_FILE_LOCATION) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            print(f'Column names are {", ".join(row)}')
            line_count += 1
        else:
            logical_query = row[4]
            exec_time_in_ms = float(row[6])

            benchmark_dict[logical_query] = exec_time_in_ms
    print(f'Processed {line_count} lines.')

line_dict = {}

for k, v in eval_per_logical_q.items():
    episodes = []
    time_ratio = []
    for episode, time in v.items():
        episodes.append(int(float(episode)))
        time_ratio.append(float(time) / benchmark_dict[k])

    line_dict[k] = [episodes, time_ratio]

fig = plt.figure()
fig.set_figwidth(15)
fig.set_figheight(4)

ax = plt.subplot(111)
for logical_query, times in line_dict.items():
    ax.plot(times[0], times[1], label=logical_query)



# Shrink current axis by 50%
box = ax.get_position()
ax.set_position([box.x0, box.y0, box.width * 0.5, box.height])

plt.axhline(y=1, color='black', linestyle='--', label='optimized / benchmark')
ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
plt.xlabel('episode')
plt.ylabel('our prediction(ms) / optimizer(ms)')
plt.show()
pp.pprint(line_dict)
