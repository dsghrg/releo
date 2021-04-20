import pandas as pd
from dbconnection import postgres_connection
from sqlalchemy import create_engine
import time

creds = postgres_connection()
engine = create_engine(
    'postgresql://{}:{}@{}:5432/{}'.format(creds['user'], creds['password'],
                                           creds['host'], creds['database']))

df = pd.read_csv('./results/query_execution_times_4-8-2021-03-21.csv')

for run in range(1, 3):
    exec_times = []
    nr_queries = len(df)
    for nr, query in enumerate(df['query']):
        # query = query.replace('BEGIN;\nSET LOCAL join_collapse_limit = 1;\n', '')
        # query = query.replace('COMMIT;', '')
        print("Executing:\n" + query)
        start_time = time.time()
        engine.execute(query)
        end_time = time.time()
        elapsed_time = end_time - start_time
        exec_times.append(elapsed_time)
        print("\n" + str(nr) + '/' + str(nr_queries) + "\n")

    new_df = pd.DataFrame(data=exec_times, columns=['run-' + str(run)])
    df = pd.concat([df, new_df], axis=1)

now = time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime(time.time()))
df.to_csv('./new' + str(now) + '.csv')
