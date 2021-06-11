import glob
import pandas as pd

base_dir = './input_runs/all-permutations/uniform'

files = glob.glob(base_dir + '/**/all-permutations-log.csv')

frames = [pd.read_csv(file) for file in files]

logical_queries = set()


def create_logical_query(join_order):
    tables_str = join_order.replace('[', '').replace(']', '')
    tables = tables_str.split(', ')
    tables = [table.strip() for table in tables]
    tables.sort()
    logical_query = "[" + (", ".join(tables)) + "]"
    return logical_query


for idx, row in frames[0].iterrows():
    logical_queries.add(create_logical_query(row['join-order']))

for idx, frame in enumerate(frames):
    frame['logical-query'] = frame['join-order'].apply(lambda join_order: create_logical_query(join_order))
    frame.to_csv(files[idx])

