import json
from db.schema.schema_definition import Table, Join

CFG_FILE_LOCATION = 'json-file-location'


class SchemaCreatorFileDefinition:

    def __init__(self, engine, cfg):
        self.engine = engine
        self.schema_definition = cfg
        if CFG_FILE_LOCATION in cfg:
            with open(cfg[CFG_FILE_LOCATION]) as f:
                self.schema_definition = json.load(f)

    def create(self):
        schema = {}
        for tablename in self.schema_definition:
            table_cfg = self.schema_definition[tablename]

            alias = self.schema_definition[tablename]['alias'] if table_cfg is not None and 'alias' in \
                                                                  self.schema_definition[
                                                                      tablename] else tablename
            table = Table(tablename, alias, {})
            schema[tablename] = table

        for tablename in self.schema_definition:
            table_config = self.schema_definition[tablename]
            if table_config is None:
                continue
            table = schema[tablename]

            joins = table_config['joins'] if 'joins' in table_config else []
            for join_def in joins:
                join = Join(table, join_def['source-column'], schema[join_def['referred-table']],
                            join_def['referred-column'])
                # symmetry
                join_back = Join(schema[join_def['referred-table']], join_def['referred-column'], table,
                                 join_def['source-column'])
                table.tablename_to_join[join.destination.name] = join
                schema[join_def['referred-table']].tablename_to_join[join_back.destination.name] = join_back
        return schema
