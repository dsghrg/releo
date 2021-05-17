import sys
import yaml
import time
import itertools

from db.connector.connection_factory import create_engine
from db.executor.executor_factory import get_executor
from db.schema.schema_factory import get as get_schema_creator
from db.schema.schema_definition import plot_schema
from db.setup.setup_factory import get_setup_teardown
from db.sql_generator.sql_query_factory import get_sql_generator
from query_generator.query_generator_factory import get_query_generator_creator
from logger.logger import Logger
from utils.queryplan import create_all_plans, get_n_joinable_tables

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
                              CFG_LOGGER_CONF]


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


if __name__ == "__main__":
    cfg_file = sys.argv[1] if len(sys.argv) > 1 else './config/postgres-default.yaml'

    cfg = load_cfg(cfg_file)
    extend_global_info(cfg[CFG_GLOBAL])
    extend_with_global_conf(cfg)
    logger = Logger(cfg)
    cfg[CFG_GLOBAL]['logger'] = logger
    system_context = cfg['global']['context']
    global_random_seed = cfg['global']['random-seed']

    engine = create_engine(cfg[CFG_DBMS], cfg[CFG_DBMS_CONF])
    schema = get_schema_creator(cfg[CFG_SCHEMA_CREATOR], engine, cfg[CFG_SCHEMA_CREATOR_CFG]).create()
    # plot_schema(schema)
    generator = get_query_generator_creator(cfg[CFG_QUERY_GEN], cfg[CFG_QUERY_GEN_CONF])(schema)
    sql_creator = get_sql_generator(cfg[CFG_SQL_CREATOR], cfg[CFG_SQL_CREATOR_CONF])
    executor = get_executor(cfg[CFG_EXECUTOR], cfg[CFG_EXECUTOR_CONF], engine, schema)

    setup, teardown = get_setup_teardown(cfg[CFG_DB_SETUP], cfg[CFG_DB_SETUP_CONF])
    setup(engine, schema)

    all_permutations = []
    for query_length in range(3, len(schema) + 1):
        for query in get_n_joinable_tables(query_length, schema):
            all_permutations += create_all_plans(schema, query)

    logger.select_log("all-permutations")
    for idx, query_order in enumerate(all_permutations):
        logger.new_record()
        logger.log('join-order', str(list(query_order)))
        print('progress: ' + str(idx + 1) + '/' + str(len(all_permutations)))
        sql_query = sql_creator(schema, query_order)
        if cfg[CFG_DBMS] == 'mssql':
            sql_query = sql_query.replace('OPTION(FORCE ORDER);', ';')
        res = executor.execute(sql_query)
    print(len(all_permutations))
    logger.save_logs()
    teardown(engine, schema)
