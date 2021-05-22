import xml.etree.ElementTree as ET
import time
import db.executor.utils.mssql_utils as utils


class MssqlJoinBreakdownXml:

    def __init__(self, cfg, engine, schema):
        self.cfg = cfg
        self.engine = engine
        self.schema = schema
        self.global_log_path = cfg['global']['log-path']
        dir = 'executor' if 'log-dir-name' not in cfg else cfg['log-dir-name']
        self.local_log_path = self.global_log_path + '/' + dir
        self.system_context = cfg['global']['context']
        self.logger = cfg['global']['logger']

    def execute(self, sql_query):
        self.logger.log('stmt', sql_query)
        con = self.engine.raw_connection()
        cursor = con.cursor()
        start_time = time.time()
        cursor.execute(sql_query)
        elapsed_time = 1000 * (time.time() - start_time)
        cursor.nextset()
        res_two = cursor.fetchall()
        xml_plan = res_two[0][0]
        self.logger.log('exec-time', elapsed_time)
        self.logger.log('resp', xml_plan)
        parsed = ET.ElementTree(ET.fromstring(xml_plan))
        root = {'children': [], 'isRoot': True, 'cost': elapsed_time}
        utils.traverse(parsed.getroot(), self.schema, root)
        return root


