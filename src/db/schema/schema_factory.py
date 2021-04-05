from db.schema.schema_autocreate import SchemaAutoCreate


def get(name, engine, cfg):
    if name == 'autocreate':
        return SchemaAutoCreate(engine, cfg)
    # if name == 'definition-file':
    #     return