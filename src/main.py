import sys

from db.connector.connection_factory import create_engine
from db.executor.executor_factory import get_executor
from db.schema import create_schema
from db.sql_generator.sql_query_factory import get_sql_generator
from query_generator.query_generator_factory import get_query_generator_creator

if __name__ == '__main__':
    dbms = sys.argv[1] if len(sys.argv) > 1 else 'postgres'
    engine = create_engine(dbms)
    schema = create_schema(engine)
    # plot_schema(schema)
    generator = get_query_generator_creator('random')(schema)
    sql_creator = get_sql_generator(dbms)
    executor = get_executor(dbms)

    for i in range(0, 10):
        logical_query = generator.generate()
        sql = sql_creator(schema, logical_query)
        runtime_stats = executor(engine, schema, sql)
