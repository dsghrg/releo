from environment.one_hot_history.one_hot_history import OneHotHistory


def get_environment(name, schema, query_generator, sql_generator, executor, cfg):
    if name == 'one-hot-history':
        return OneHotHistory(schema, query_generator, sql_generator, executor, cfg)
