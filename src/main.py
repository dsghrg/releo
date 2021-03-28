import sys

from db.connector.connection_factory import create_engine
from db.schema import create_schema, plot_schema

if __name__ == '__main__':
    dbms = sys.argv[1] if len(sys.argv) > 1 else 'postgres'
    engine = create_engine(dbms)
    schema = create_schema(engine)
    plot_schema(schema)
    print(engine)
