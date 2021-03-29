def setup(engine, schema):
    engine.execute('SET STATISTICS XML ON;')


def teardown(engine, schema):
    engine.execute('SET STATISTICS XML OFF;')
