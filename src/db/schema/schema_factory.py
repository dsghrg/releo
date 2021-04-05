from db.schema.schema_autocreate import SchemaAutoCreate
from db.schema.schema_filedefinition import SchemaCreatorFileDefinition


def get(name, engine, cfg):
    if name == 'autocreate':
        return SchemaAutoCreate(engine, cfg)
    if name == 'definition-file':
        return SchemaCreatorFileDefinition(engine, cfg)
