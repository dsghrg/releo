import pandas as pd
import json
import copy


class Logger:
    logs = {}
    current_log = {}

    def __init__(self, cfg):
        self.cfg = cfg
        self.system_context = cfg['global']['context']
        self.system_context['logger'] = {}
        self.global_log_path = cfg['global']['log-path']

    def new_record(self):
        if self.current_log is not None:
            stdout_rec = copy.deepcopy(self.current_log['current-record'])
            if stdout_rec and 'resp' in stdout_rec:
                stdout_rec['resp'] = '...'
            print(json.dumps(stdout_rec))
            new_rec = self._create_record()
            if 'current-record' in self.current_log and self.current_log['current-record'] is not None:
                new_rec['record-id'] = self.current_log['current-record']['record-id'] + 1
            self.current_log['records'][new_rec['record-id']] = new_rec
            self.current_log['current-record'] = new_rec

    def select_log(self, log_name):
        if log_name not in self.logs:
            new_log = self._create_log(log_name)
            self.logs[log_name] = new_log
        self.current_log = self.logs[log_name]

    def log(self, key, value):
        if self.current_log is not None:
            self.current_log['current-record'][key] = value

    def save_logs(self):
        for log_name in self.logs:
            log = self.logs[log_name]
            columns = set()
            for record_id in log['records']:
                record = log['records'][record_id]
                columns.update(list(record.keys()))

            data = []
            for record_id in log['records']:
                record = log['records'][record_id]
                rec_data = [record[col] if col in record else None for col in columns]
                data.append(rec_data)
            df = pd.DataFrame(data=data, columns=columns)
            df.to_csv(self.global_log_path + '/' + log_name + '-log.csv', encoding='utf-8', sep=',')

    def _create_record(self):
        return {'record-id': 0}

    def _create_log(self, name):
        return {'name': name, 'records': {}, 'current-record': None}
