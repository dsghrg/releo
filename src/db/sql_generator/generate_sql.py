import functools


def generate_sql_query(schema, order_vector, query_patcher):
    used_relations = {}
    first_table = schema[order_vector[0]]
    query = "SELECT count(*) FROM " + first_table.name + " as " + first_table.alias
    used_relations[first_table.name] = first_table

    for i in range(1, len(order_vector)):
        next_relation = schema[order_vector[i]]

        # looks for an already joined relation to join with
        rel_to_join_with = functools.reduce(
            lambda rel_to_join, linked_relation: rel_to_join if rel_to_join is not None
            else used_relations[linked_relation] if linked_relation in used_relations else None,
            list(reversed(list(next_relation.tablename_to_join.keys()))), None)
        join = next_relation.tablename_to_join[rel_to_join_with.name]
        query += "\nJOIN " + next_relation.name + " as " + next_relation.alias \
                 + "\nON " + next_relation.alias + "." + join.source_key + " = " \
                 + join.destination.alias + "." + join.destination_key
        used_relations[next_relation.name] = next_relation

    query += ";"
    query = query_patcher(query)

    return query
