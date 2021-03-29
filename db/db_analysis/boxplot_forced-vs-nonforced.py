import csv
import matplotlib.pyplot as plt

non_forced = []
forced = []

with open('logfiles_mssql_four_times_join.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=';')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            print(f'Column names are {", ".join(row)}')
            line_count += 1
        else:
            forced.append(float(row[0]))
            non_forced.append(float(row[1]))
            line_count += 1
    print(f'Processed {line_count} lines.')


fig = plt.figure()
ax = fig.add_subplot()

ax.set_xticklabels(["forced", "non_forced"])

plt.boxplot([forced, non_forced])
plt.title("forced vs. non_forced query (4)")
plt.show()

