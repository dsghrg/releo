from db.schema.schema_definition import Table, Join
from sqlalchemy import inspect


class SchemaAutoCreate:

    def __init__(self, engine, cfg):
        self.engine = engine
        self.cfg = cfg

    def create(self):
        schema = {}
        inspector = inspect(self.engine)
        for tablename in inspector.get_table_names():
            table = Table(tablename, tablename, {})
            schema[tablename] = table

        for tablename in inspector.get_table_names():
            table = schema[tablename]
            foreign_keys = inspector.get_foreign_keys(tablename)
            for fk in foreign_keys:
                join = Join(table, fk['constrained_columns'][0], schema[fk['referred_table']],
                            fk['referred_columns'][0])
                # symmetry
                join_back = Join(schema[fk['referred_table']], fk['referred_columns'][0], table,
                                 fk['constrained_columns'][0])
                table.tablename_to_join[join.destination.name] = join
                schema[fk['referred_table']].tablename_to_join[join_back.destination.name] = join_back
        return schema
