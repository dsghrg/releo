def execute_sql_query(db_engine, schema, sql_query):
    rs = db_engine.execute(sql_query)
    rec = rs.first()[0]
    costs = {}
    print(rec)
    return costs