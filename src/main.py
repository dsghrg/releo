import sys

from db.connector.connection_factory import create_engine
from db.schema import create_schema, plot_schema
from query_generator.query_generator_factory import get_query_generator_creator

if __name__ == '__main__':
    dbms = sys.argv[1] if len(sys.argv) > 1 else 'postgres'
    engine = create_engine(dbms)
    schema = create_schema(engine)
    plot_schema(schema)
    generator = get_query_generator_creator('random')(schema)

    for i in range(0, 10):
        print(generator.generate())

    print(engine)
