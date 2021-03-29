from db.setup.postgres_forceorder_setup import setup, teardown


def get_setup_teardown(name, cfg):
    if name == 'postgres-default':
        return (lambda engine, schema: setup(engine, schema), lambda engine, schema: teardown(engine, schema))
