import random

import gym
import numpy as np

MAX_JOINS = 'max-joins'
RANDOM_SEED = 'seed'


class OneHotHistory(gym.Env):
    # list of tablenames left to join ex. ['order', 'customer', 'order_details']
    relations_to_join = []
    # list of tables that were joined. indicates order in which joins were done
    # ex ['order', 'deliverer', 'order_details'] : ... from ORDER join DELIVERER on ... join ORDER_DETAILS on...
    join_history = []

    # list of tablenames which can be joined next
    possible_actions = []

    # history of all the states for attributing every state during query generation with the costs
    state_history = []

    def __init__(self, schema, query_generator, sql_generator, executor, cfg):
        random.seed = cfg[RANDOM_SEED] if RANDOM_SEED in cfg else 1

        self.query_generator = query_generator
        self.sql_generator = sql_generator
        self.schema = schema
        self.executor = executor
        self.max_joins = cfg[MAX_JOINS] if MAX_JOINS in cfg else len(schema)
        self.no_of_relations = len(schema)
        self.tablename_to_index = {tablename: index for index, tablename in enumerate(schema.keys())}
        self.index_to_tablename = {self.tablename_to_index[tablename]: tablename for tablename in
                                   self.tablename_to_index.keys()}

    def get_possible_actions_enc(self):
        return sum([self._tablename_to_vector(tablename) for tablename in self.possible_actions])

    def step(self, action):
        action = self.index_to_tablename[action]
        is_first_action = True if len(self.join_history) == 0 else False
        self.possible_actions.remove(action)
        if is_first_action:
            self.possible_actions = []

        # all the tables which can be joined with the newly joined table and are involved in the query
        new_possible_actions = [new_action for new_action in self.schema[action].tablename_to_join.keys() if
                                new_action in self.relations_to_join]
        self.possible_actions = self.possible_actions + new_possible_actions

        self.relations_to_join.remove(action)
        self.join_history.append(action)
        self.state_history.append(self._map_to_state_enc())

        is_done = True if len(self.relations_to_join) == 0 else False
        cost_infos = []
        if is_done:
            sql_statement = self.sql_generator(self.schema, self.join_history)
            costs = self.executor.execute(sql_statement)
            self._traverse_join_tree(costs, cost_infos, 0)

        return self._map_to_state_enc(), cost_infos, is_done, {}

    def reset(self):
        logical_query = self.query_generator.generate()
        self.relations_to_join = logical_query
        self.join_history = []
        # it is possible to start with any table in the query
        self.possible_actions = logical_query.copy()
        pass

    def render(self, mode='human'):
        return 1

    # merges every state during query generation with the actual cost of the step taken
    def _traverse_join_tree(self, current_join, costs, join_nr):
        cost = {'state': self.state_history[join_nr], 'cost': -1 * current_join['cost']}
        costs.append(cost)
        for child in current_join['children']:
            self._traverse_join_tree(child, costs, join_nr + 1)

    def _map_to_state_enc(self):
        # concatenation of joins and relations left. One join is represented by a one hot encoding of the newly joined
        # table and the previously joined tables
        # [ join1   join2 .... joinN   relations left]
        # ex: max_joins = 3, no_of_relations = 4
        # initial:
        # -> [0 0 0 0    0 0 0 0   0 0 0 0   1 1 0 1]
        # first action: start with table '4'  (select from '4')
        # -> [0 0 0 1    0 0 0 0   0 0 0 0   1 1 0 0]
        # second action: join table '2' (select from '4' join '2' on ..)
        # -> [0 0 0 1    0 1 0 1   0 0 0 0   1 0 0 0]
        # third action (last): join table '1' (select from '4' join '2' on  .. join '1' on ..)
        # -> [0 0 0 1    0 1 0 1   1 1 0 1   0 0 0 0]

        # length of the state is determined by max allowed joins and the number of tables in the db
        state_enc_size = self.max_joins * self.no_of_relations + self.no_of_relations
        state = np.zeros(state_enc_size)
        tables_left_enc_offset = self.max_joins * self.no_of_relations
        tables_to_join_adder = np.zeros(state_enc_size)

        # prepare an array of same length as state with all zeros except the last no_of_relations
        # set to 1 if table still needs to be joined
        for table_to_join in self.relations_to_join:
            idx = self.tablename_to_index[table_to_join] + tables_left_enc_offset
            tables_to_join_adder[idx] = 1

        # add join history
        prev_join = np.zeros(self.no_of_relations)
        for join_nr, join in enumerate(self.join_history):
            join_enc = prev_join + self._tablename_to_vector(join)
            state[join_nr * self.no_of_relations:join_nr * self.no_of_relations + self.no_of_relations] = join_enc
            prev_join = join_enc

        # add info which tables still need to be joined
        state = state + tables_to_join_adder
        return state

    # tablename to one-hot table encoding. length of vec is given by number of tables in the schema
    def _tablename_to_vector(self, tablename):
        table_enc = np.zeros(self.no_of_relations)
        table_enc[self.tablename_to_index[tablename]] = 1
        return table_enc

    # one hot encoding of table to tablename
    def _vector_to_table_name(self, one_hot_table):
        return self.index_to_tablename[np.where(one_hot_table == 1)[0]]
