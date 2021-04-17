import csv
import itertools
import math
import random
import time
import copy

import numpy as np

CFG_TEST_SET_LOCATION = 'testset-location'
CFG_TRAIN_SET_LOCATION = 'trainset-location'

CFG_TRAIN_SET_OUT_LOCATION = 'trainset-out-location'
CFG_TEST_SET_OUT_LOCATION = 'testset-out-location'

CFG_TRAIN_SIZE = 'train-size'
CFG_MAX_JOINS = 'max-joins'
CFG_MIN_JOINS = 'min-joins'


def _write_set_to_csv(file, query_set):
    with open(file, "a") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        for query in list(query_set):
            writer.writerow(query)


def _fill_set(file, query_set):
    with open(file) as csv_file:
        query_set += ([line.rstrip().split(',') for line in csv_file])


class TrainTestSetQueryGenerator:

    def __init__(self, schema, cfg):
        self.schema = schema
        now = time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime(time.time()))
        self.csv_train_name = 'train_set-' + now + ".csv"
        self.csv_test_name = 'test_set-' + now + ".csv"

        cfg = cfg if cfg is not None else {}
        self.train_set_location = cfg[CFG_TRAIN_SET_LOCATION] if CFG_TRAIN_SET_LOCATION in cfg else None
        self.test_set_location = cfg[CFG_TEST_SET_LOCATION] if CFG_TEST_SET_LOCATION in cfg else None
        self.train_out_location = cfg[CFG_TRAIN_SET_OUT_LOCATION] if CFG_TRAIN_SET_OUT_LOCATION in cfg else './'
        self.test_out_location = cfg[CFG_TEST_SET_OUT_LOCATION] if CFG_TEST_SET_OUT_LOCATION in cfg else './'
        self.train_size = cfg[CFG_TRAIN_SIZE] if CFG_TRAIN_SIZE in cfg else 0.8
        self.max_joins = min(len(schema), cfg[CFG_MAX_JOINS]) if CFG_MAX_JOINS in cfg else min(len(schema), 15)
        self.min_joins = max(3, cfg[CFG_MIN_JOINS]) if CFG_MIN_JOINS in cfg else 3

        if self.train_set_location is None and self.test_set_location is None:
            self._generate_sets()
            _write_set_to_csv(self.train_out_location + self.csv_train_name, self.train_set)
            _write_set_to_csv(self.test_out_location + self.csv_test_name, self.test_set)
        else:
            self.test_set = []
            self.train_set = []
            _fill_set(self.test_set_location, self.test_set)
            _fill_set(self.train_set_location, self.train_set)

    def generate_train(self):
        return random.choice(self.train_set).copy()

    def _generate_sets(self):
        self.all = []
        for i in range(self.min_joins, self.max_joins + 1):
            self.all = self.all + get_n_joinable_tables(i, self.schema)

        self.all = np.array(self.all)
        np.random.shuffle(self.all)
        self.train_set = list(self.all[:math.floor(self.train_size * len(self.all))])
        self.test_set = list(self.all[math.ceil(self.train_size * len(self.all)):])

    def get_train_set(self):
        return copy.deepcopy(self.train_set)

    def get_test_set(self):
        return copy.deepcopy(self.test_set.copy())


def powerset(iterable):
    """powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"""
    s = list(iterable)
    return itertools.chain.from_iterable(itertools.combinations(s, r) for r in range(len(s) + 1))


def dfs(visited, schema, comb, current):
    if current.name not in visited:
        visited.add(current.name)
        for neighbour in schema[current.name].tablename_to_join.keys():
            if neighbour in comb:
                dfs(visited, schema, comb, schema[neighbour])


# generate all possible logical queries with n involved relations
def get_n_joinable_tables(n, schema):
    power_set = list(powerset(schema.keys()))
    n_power_set = [comb for comb in power_set if len(comb) == n]
    all_possible_joinable_tables = []

    for i, comb in enumerate(n_power_set):
        visited = set()
        dfs(visited, schema, comb, schema[comb[0]])
        if len(visited) == n:
            all_possible_joinable_tables.append(comb)
    return all_possible_joinable_tables
