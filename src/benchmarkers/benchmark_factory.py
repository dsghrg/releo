from benchmarkers.postgres_benchmark import PostgressBenchmark
from benchmarkers.mssql_benchmark import MssqlBenchmark


def get_benchmark_creator(name, schema, engine, sql_creator, test_set, cfg):
    if name == 'postgres':
        return PostgressBenchmark(schema, engine, sql_creator, test_set, cfg)
    elif name == 'mssql':
        return MssqlBenchmark(schema, engine, sql_creator, test_set, cfg)
