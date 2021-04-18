import sys
import time
import os
import shutil

import numpy as np
import pandas as pd
import yaml
from db.connector.connection_factory import create_engine
from db.executor.executor_factory import get_executor
from db.schema.schema_factory import get as get_schema_creator
from db.setup.setup_factory import get_setup_teardown
from db.sql_generator.sql_query_factory import get_sql_generator
from environment.environment_factory import get_environment
from query_generator.query_generator_factory import get_query_generator_creator
from rl_algorithms.rl_agent_factory import get_rl_agent
from logger.logger import Logger

CFG_GLOBAL = 'global'
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
CFG_RL_AGENT = 'agent'
CFG_RL_AGENT_CONF = 'agent-config'
CFG_SCHEMA_CREATOR = 'schema-creator'
CFG_SCHEMA_CREATOR_CFG = 'schema-creator-config'
CFG_LOGGER_CONF = 'logger-conf'

cfgs_to_extend_with_global = [CFG_DBMS_CONF,
                              CFG_QUERY_GEN_CONF,
                              CFG_SQL_CREATOR_CONF,
                              CFG_EXECUTOR_CONF,
                              CFG_DB_SETUP_CONF,
                              CFG_ENV_CONF,
                              CFG_RL_AGENT_CONF,
                              CFG_SCHEMA_CREATOR_CFG,
                              CFG_LOGGER_CONF]


def create_order(schema, join_order, current, left_to_join):
    if current.name not in join_order:
        join_order.append(current.name)
        left_to_join.remove(current.name)
        for neighbour in schema[current.name].tablename_to_join.keys():
            if neighbour in left_to_join:
                create_order(schema, join_order, schema[neighbour], left_to_join)


def load_cfg(cfg_file):
    with open(cfg_file) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    return config


def extend_global_info(glob_cfg):
    now = time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime(time.time()))
    log_dir = 'releo-run-' + now
    glob_cfg['log-dir'] = log_dir
    glob_cfg['log-path'] = glob_cfg['log-location'] + '/' + log_dir

    # the whole system communicates additional info to use in different parts
    glob_cfg['context'] = {}


def extend_with_global_conf(cfg):
    for sub_cfg_name in cfgs_to_extend_with_global:
        if sub_cfg_name not in cfg:
            cfg[sub_cfg_name] = {}
        sub_cfg = cfg[sub_cfg_name]
        if sub_cfg is None:
            sub_cfg = {}
            cfg[sub_cfg_name] = sub_cfg
        sub_cfg['global'] = cfg[CFG_GLOBAL]


def setup_run_dir(cfg_file, cfg):
    log_path = cfg[CFG_GLOBAL]['log-path']
    os.makedirs(log_path, exist_ok=True)
    shutil.copyfile(cfg_file, log_path + '/' + cfg_file.split('/')[-1])


if __name__ == '__main__':
    cfg_file = sys.argv[1] if len(sys.argv) > 1 else './config/postgres-default.yaml'
    cfg = load_cfg(cfg_file)
    extend_global_info(cfg[CFG_GLOBAL])
    extend_with_global_conf(cfg)
    setup_run_dir(cfg_file, cfg)
    logger = Logger(cfg)
    cfg[CFG_GLOBAL]['logger'] = logger
    system_context = cfg['global']['context']

    engine = create_engine(cfg[CFG_DBMS], cfg[CFG_DBMS_CONF])
    schema = get_schema_creator(cfg[CFG_SCHEMA_CREATOR], engine, cfg[CFG_SCHEMA_CREATOR_CFG]).create()
    generator = get_query_generator_creator(cfg[CFG_QUERY_GEN], cfg[CFG_QUERY_GEN_CONF])(schema)
    sql_creator = get_sql_generator(cfg[CFG_SQL_CREATOR], cfg[CFG_SQL_CREATOR_CONF])
    executor = get_executor(cfg[CFG_EXECUTOR], cfg[CFG_EXECUTOR_CONF], engine, schema)

    test_set = generator.get_test_set()
    test_queries = {}

    logger.select_log('eval-set-benchmarking')
    for idx, test_plan in enumerate(test_set):
        logger.new_record()
        logger.log('test-query-nr', idx)
        logger.log('logical-query', str(test_plan.copy()))
        order = []
        create_order(schema, order, schema[test_plan[0]], test_plan.copy())
        sql_query = sql_creator(schema, order)
        res = executor.execute(sql_query)
        # keep in mind the hashes of the queries in the db_analysis dir are created with hashlib (maybe patch the files)
        test_queries[hash(tuple(test_plan))] = res['cost']

    setup, teardown = get_setup_teardown(cfg[CFG_DB_SETUP], cfg[CFG_DB_SETUP_CONF])
    setup(engine, schema)
    env = get_environment(cfg[CFG_ENV], schema, generator, sql_creator, executor, cfg[CFG_ENV_CONF])

    rl_algo = get_rl_agent(cfg[CFG_RL_AGENT], env, cfg[CFG_RL_AGENT_CONF])
    rl_algo.train()

    print('training finito')

    query_times = {}
    logger.select_log('evaluation')
    for idx, query in enumerate(test_set):
        logger.new_record()
        logger.log('test-query-nr', idx)
        logger.log('logical-query', str(query.copy()))
        query = query.copy()
        state = env.reset_with_query(query.copy())
        state = state.reshape((1, env.observation_space.shape[0]))
        done = False
        while not done:
            possible_steps = env.possible_steps()
            state = state.reshape((1, env.observation_space.shape[0]))
            prediction = rl_algo.model.predict(state)[0]
            for idx, action in enumerate(possible_steps):
                if action == 0:
                    prediction[idx] = -np.inf
            action = np.argmax(prediction)
            state, reward, done, _info = env.step(action)

        sql = sql_creator(schema, env.join_order)
        res = executor.execute(sql)
        cost = res['cost']
        logger.log('time-releo', cost)
        hashed_query = hash(tuple(query))
        logger.log('hash', hashed_query)
        logger.log('time-optimizer', test_queries[hashed_query])
        query_times[hash(tuple(query))] = cost
        print("\n\n" + sql)

    logger.save_logs()

    teardown(engine, schema)
