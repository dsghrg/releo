from db.executor.executor_factory import get_executor
from utils.queryplan import create_valid_order

CFG_EXECUTOR = 'executor'
CFG_EXECUTOR_CONF = 'executor-config'


class PostgressBenchmark:

    def __init__(self, schema, engine, sql_creator, test_set, cfg):
        self.cfg = cfg
        self.engine = engine
        self.schema = schema
        self.sql_creator = sql_creator
        self.test_set = test_set
        self.global_log_path = cfg['global']['log-path']
        self.local_log_path = self.global_log_path + '/executor'
        self.system_context = cfg['global']['context']
        self.logger = cfg['global']['logger']
        if CFG_EXECUTOR_CONF not in cfg or cfg[CFG_EXECUTOR_CONF] is None:
            cfg[CFG_EXECUTOR_CONF] = {}
        cfg[CFG_EXECUTOR_CONF]['global'] = cfg['global']

        self.executor = get_executor(cfg[CFG_EXECUTOR], cfg[CFG_EXECUTOR_CONF], engine, schema)

    def create_benchmark_by_set(self, test_set):
        res = []
        self.logger.select_log('eval-set-benchmarking')
        for idx, query in enumerate(test_set):
            self.logger.new_record()
            self.logger.log('test-query-nr', idx)
            # hash(tuple(test_plan))
            rec = self.benchmark_query(query)
            res.append(rec)
        return res

    def benchmark_query(self, logical_query):
        con = self.engine.connect()
        db = self.engine.url.database
        con.execute('ALTER DATABASE "{}" SET join_collapse_limit = 8;'.format(db))
        con.execute('SELECT pg_reload_conf();')

        logical_query = logical_query.copy()
        self.logger.select_log('eval-set-benchmarking')
        self.logger.log('logical-query', str(logical_query.copy()))
        order = []
        create_valid_order(self.schema, order, self.schema[logical_query[0]], logical_query.copy())
        sql_query = self.sql_creator(self.schema, order)
        res = self.executor.execute(sql_query)  # TODO Force Order muss deaktiviert werden!!!
        # keep in mind the hashes of the queries in the db_analysis dir are created with hashlib (maybe patch the files)
        return {'query': logical_query, 'query-hash': hash(tuple(logical_query)), 'cost': res['cost']}

    def benchmark(self):
        res = []
        self.logger.select_log('eval-set-benchmarking')
        for idx, query in enumerate(self.test_set):
            self.logger.new_record()
            self.logger.log('test-query-nr', idx)
            res.append(self.benchmark_query(query))
        return res
