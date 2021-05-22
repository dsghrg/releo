def setup(engine, schema):
    con = engine.connect()
    db = engine.url.database
    con.execute('ALTER DATABASE "{}" SET join_collapse_limit = 1;'.format(db))
    #con.execute('ALTER DATABASE "{}" SET max_parallel_workers_per_gather = 2;'.format(db))
    #con.execute('ALTER DATABASE "{}" SET parallel_setup_cost = 1.79769e+308;'.format(db))
    #con.execute('ALTER DATABASE "{}" SET parallel_setup_cost = 0;'.format(db))
    #con.execute('ALTER DATABASE "{}" SET parallel_tuple_cost = 1.79769e+308;'.format(db))
    #con.execute('ALTER DATABASE "{}" SET parallel_tuple_cost = 0;'.format(db))
    #con.execute('ALTER DATABASE "{}" SET enable_parallel_hash = on;'.format(db))
    #con.execute('ALTER DATABASE "{}" SET enable_parallel_append = on;'.format(db))
    #con.execute('ALTER DATABASE "{}" SET max_parallel_workers = 2;'.format(db))
    # con.execute('ALTER DATABASE "{}" SET max_worker_processes = 0;'.format(db))
    con.execute('SELECT pg_reload_conf();')
    engine.execute('SHOW join_collapse_limit;')


def teardown(engine, schema):
    db = engine.url.database
    engine.execute('ALTER DATABASE "{}" SET join_collapse_limit = 8;'.format(db))
    engine.execute('ALTER DATABASE "{}" SET max_parallel_workers_per_gather = 2;'.format(db))
    engine.execute('ALTER DATABASE "{}" SET parallel_setup_cost = 1000;'.format(db))
    engine.execute('ALTER DATABASE "{}" SET parallel_tuple_cost = 0.1;'.format(db))
    engine.execute('ALTER DATABASE "{}" SET enable_parallel_hash = on;'.format(db))
    engine.execute('ALTER DATABASE "{}" SET enable_parallel_append = on;'.format(db))
    engine.execute('ALTER DATABASE "{}" SET max_parallel_workers = 2;'.format(db))
    # engine.execute('ALTER DATABASE "{}" SET max_worker_processes = 0;'.format(db))
    engine.execute('SELECT pg_reload_conf();')
