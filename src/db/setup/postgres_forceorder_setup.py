def setup(engine, schema):
    con = engine.connect()
    con.execute('ALTER DATABASE shop_db SET join_collapse_limit = 1;')
    con.execute('SELECT pg_reload_conf();')
    engine.execute('SHOW join_collapse_limit;')


def teardown(engine, schema):
    engine.execute('ALTER DATABASE shop_db SET join_collapse_limit = 8;')
    engine.execute('SELECT pg_reload_conf();')
