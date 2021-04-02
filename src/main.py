import sys
import yaml
import numpy as np
import random

from db.connector.connection_factory import create_engine
from db.executor.executor_factory import get_executor
from db.schema import create_schema
from db.sql_generator.sql_query_factory import get_sql_generator
from db.setup.setup_factory import get_setup_teardown
from query_generator.query_generator_factory import get_query_generator_creator
from environment.environment_factory import get_environment

CFG_DBMS = 'dbms'
CFG_DBMS_CONF = 'db-connection'
CFG_QUERY_GEN = 'query-generator'
CFG_QUERY_GEN_CONF = 'query-generator-config'
CFG_SQL_CREATOR = 'sql-creator'
CFG_SQL_CREATOR_CONF = 'sql-creator-config'
CFG_EXECUTOR = 'executor'
CFG_EXECUTOR_CONF = 'executor-config'
CFG_DB_SETUP = 'db-setup'
CFG_DB_SETUP_CONF = 'db-setup-config'
CFG_ENV = 'environment'
CFG_ENV_CONF = 'environment-config'


def load_cfg():
    cfg = {}
    cfg_file = sys.argv[1] if len(sys.argv) > 1 else './config/postgres-default.yaml'
    with open(cfg_file) as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)

    return cfg


if __name__ == '__main__':
    cfg = load_cfg()
    engine = create_engine(cfg[CFG_DBMS], cfg[CFG_DBMS_CONF])
    schema = create_schema(engine)
    setup, teardown = get_setup_teardown(cfg[CFG_DB_SETUP], cfg[CFG_DB_SETUP_CONF])
    setup(engine, schema)
    # plot_schema(schema)
    generator = get_query_generator_creator(cfg[CFG_QUERY_GEN], cfg[CFG_QUERY_GEN_CONF])(schema)
    sql_creator = get_sql_generator(cfg[CFG_SQL_CREATOR], cfg[CFG_SQL_CREATOR_CONF])
    executor = get_executor(cfg[CFG_EXECUTOR], cfg[CFG_EXECUTOR_CONF], engine, schema)
    env = get_environment(cfg[CFG_ENV], schema, generator, sql_creator, executor, cfg[CFG_ENV_CONF])

    env.reset()
    done = False
    while not done:
        action_indexes = np.where(env.get_possible_actions_enc() == 1)[0]
        action_index = random.choice(action_indexes)
        new_state, reward, done, _info = env.step(action_index)
        if done:
            print(new_state)
