import xml.etree.ElementTree as ET

import db.executor.utils.mssql_utils as utils
import pandas as pd

namespace = {'mw': 'http://schemas.microsoft.com/sqlserver/2004/07/showplan'}


class MssqlLookupJoinBreakdownXml:

    def __init__(self, cfg, engine, schema):
        self.cfg = cfg
        self.engine = engine
        self.schema = schema
        self.global_log_path = cfg['global']['log-path']
        dir = 'executor' if 'log-dir-name' not in cfg else cfg['log-dir-name']
        self.local_log_path = self.global_log_path + '/' + dir
        self.system_context = cfg['global']['context']
        self.logger = cfg['global']['logger']
        self.lookup_csv = cfg['lookup-csv-path']
        self.lookup_frame = pd.read_csv(self.lookup_csv)

    def execute(self, sql_query):
        self.logger.log('stmt', sql_query)
        xml_plan = self.lookup_frame[self.lookup_frame['stmt'] == sql_query]['resp'].values[0]
        parsed = ET.ElementTree(ET.fromstring(xml_plan))
        elapsed_time = parsed.findall('.//mw:QueryTimeStats', namespaces=namespace)[0].attrib['ElapsedTime']
        self.logger.log('exec-time', elapsed_time)
        self.logger.log('resp', xml_plan)
        root = {'children': [], 'isRoot': True, 'cost': elapsed_time}
        utils.traverse(parsed.getroot(), self.schema, root)
        return root
