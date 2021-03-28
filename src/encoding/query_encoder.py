import numpy as np


def get_key_from_value(value, dictionary):
    return list(dictionary.keys())[list(dictionary.values()).index(value)]


class QueryEncoder:
    def __init__(self, schema):
        self.table_count = len(schema)
        self.schema_dict = {tablename: idx for idx, tablename in enumerate(schema.keys())}

    def init_vector(self):
        return [0] * self.table_count

    def encode(self, tables_involved):
        encoded_query = self.init_vector()
        for table in tables_involved:
            index = self.schema_dict.get(table) - 1
            encoded_query[index] = 1

        return np.array(encoded_query)

    # example input: [0 2 1 0 0 0 3 0]
    # example output: [order, discount, order_details]
    def decode_query(self, order_vector):
        order_to_table = {orderIdx: get_key_from_value(vec_idx, self.schema_dict) for vec_idx, orderIdx in enumerate(order_vector) if
                          orderIdx != 0}
        table_vector = [order_to_table[pos] for pos in range(1, len(order_to_table) + 1)]

        return np.array(table_vector)
