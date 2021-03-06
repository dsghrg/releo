import gym
import numpy as np
from gym import spaces

MAX_JOINS = 'max-joins'


class OneHotHistory(gym.Env):
    # list of tablenames left to join ex. ['order', 'customer', 'order_details']
    relations_to_join = []
    # list of tables that were joined. indicates order in which joins were done
    # ex ['order', 'deliverer', 'order_details'] : ... from ORDER join DELIVERER on ... join ORDER_DETAILS on...
    join_order = []

    # list of tablenames which can be joined next
    possible_actions = []

    # history of all the states for associating every state with the respective cost after the query execution
    state_history = []

    def __init__(self, schema, query_generator, sql_creator, executor, cfg):
        self.logger = cfg['global']['logger']
        self.query_generator = query_generator
        self.sql_creator = sql_creator
        self.schema = schema
        self.executor = executor
        self.max_joins = cfg[MAX_JOINS] if MAX_JOINS in cfg else len(schema)
        self.no_of_relations = len(schema)
        self.action_space = spaces.Discrete(self.no_of_relations)
        self.observation_space = spaces.Box(
            np.zeros(self.no_of_relations * self.max_joins + self.no_of_relations, dtype=int),
            np.ones(self.no_of_relations * self.max_joins + self.no_of_relations, dtype=int),
            dtype=np.int)
        self.tablename_to_index = {tablename: index for index, tablename in enumerate(schema.keys())}
        self.index_to_tablename = {self.tablename_to_index[tablename]: tablename for tablename in
                                   self.tablename_to_index.keys()}

    def get_possible_actions_enc(self):
        return sum([self._tablename_to_vector(tablename) for tablename in self.possible_actions])

    def step(self, action):
        # action is the index of the encoded table referring to the joined table
        action = self.index_to_tablename[action]
        is_first_action = True if len(self.join_order) == 0 else False
        self.possible_actions.remove(action)

        # it is possible to start with any table.
        # Therefore, after choosing a starting table, we need to clear all possible actions
        if is_first_action:
            self.possible_actions = []

        # all the tables which can be joined with the newly joined table and are involved in the query
        new_possible_actions = [new_action for new_action in self.schema[action].tablename_to_join.keys() if
                                new_action in self.relations_to_join]
        self.possible_actions = self.possible_actions + new_possible_actions

        # the action taken can be removed from the remaining tables
        self.relations_to_join.remove(action)
        # append the chosen table to generate a join order
        self.join_order.append(action)
        # append the whole state to the history in order to associate every state with the final cost after execution
        self.state_history.append(self._map_to_state_enc())

        is_done = True if len(self.relations_to_join) == 0 else False
        cost_infos = []

        # we can only retrieve the actual costs once the join order is determined
        if is_done:
            self.logger.log('join-order', str(self.join_order))
            sql_statement = self.sql_creator(self.schema, self.join_order)
            costs = self.executor.execute(sql_statement)
            costs['cost'] = 0
            self._traverse_join_tree(costs, cost_infos, 0)

        return self._map_to_state_enc(), cost_infos, is_done, {}

    def reset_with_query(self, logical_query):
        self.relations_to_join = logical_query
        self.join_order = []
        self.state_history = []
        # it is possible to start with any table in the query
        self.possible_actions = logical_query.copy()
        return self._map_to_state_enc()

    def reset(self):
        logical_query = self.query_generator.generate_train()
        return self.reset_with_query(logical_query)

    def render(self, mode='human'):
        return 1

    def possible_steps(self):
        return self.get_possible_actions_enc()

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
        # ex: max_joins = 3, no_of_relations = 4, query involves 3 tables
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
        state = np.zeros(state_enc_size, dtype=int)
        tables_left_enc_offset = self.max_joins * self.no_of_relations
        tables_to_join_adder = np.zeros(state_enc_size, dtype=int)

        # prepare an array of same length as state with all zeros except the last no_of_relations
        # set to 1 if table still needs to be joined
        for table_to_join in self.relations_to_join:
            idx = self.tablename_to_index[table_to_join] + tables_left_enc_offset
            tables_to_join_adder[idx] = 1

        # add join history
        prev_join = np.zeros(self.no_of_relations, dtype=int)
        for join_nr, join in enumerate(self.join_order):
            join_enc = prev_join + self._tablename_to_vector(join)
            state[join_nr * self.no_of_relations:join_nr * self.no_of_relations + self.no_of_relations] = join_enc
            prev_join = join_enc

        # add info which tables still need to be joined
        state = state + tables_to_join_adder
        return state

    # tablename to one-hot table encoding. length of vec is given by number of tables in the schema
    def _tablename_to_vector(self, tablename):
        table_enc = np.zeros(self.no_of_relations, dtype=int)
        table_enc[self.tablename_to_index[tablename]] = 1
        return table_enc

    # one hot encoding of table to tablename
    def _vector_to_table_name(self, one_hot_table):
        return self.index_to_tablename[np.where(one_hot_table == 1)[0]]
