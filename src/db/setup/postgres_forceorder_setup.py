def setup(engine, schema):
    engine.execute('SET join_collapse_limit = 1;')


def teardown(engine, schema):
    engine.execute('SET join_collapse_limit = 8;')
