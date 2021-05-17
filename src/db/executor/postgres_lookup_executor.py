import json
import os

import db.executor.utils.postgres_utils as utils
import pandas as pd


class PostgresLookupJoinBreakdownJson():

    def __init__(self, cfg, engine, schema):
        self.cfg = cfg
        self.engine = engine
        self.schema = schema
        self.global_log_path = cfg['global']['log-path']
        self.local_log_path = self.global_log_path + '/executor'
        self.system_context = cfg['global']['context']
        self.logger = cfg['global']['logger']
        self.lookup_csv = cfg['lookup-csv-path']
        self.lookup_frame = pd.read_csv(self.lookup_csv)
        os.makedirs(self.local_log_path)

    def execute(self, sql_query):
        self.logger.log('stmt', sql_query)
        rec = json.loads(self.lookup_frame[self.lookup_frame['stmt'] == sql_query]['resp'][0])
        elapes_time = rec[0]['Execution Time']
        self.logger.log('exec-time', elapes_time)
        self.logger.log('resp', json.dumps(rec))
        costs = {'children': [], 'isRoot': True, 'cost': elapes_time}
        utils.traverse(rec[0]['Plan'], self.schema, costs)
        return costs

    def _get_rl_context(self):
        return self.system_context['rl-agent'] if 'rl-agent' in self.system_context else {}
