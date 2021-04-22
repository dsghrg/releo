import itertools


def dfs(schema, visited, to_visit):
    nbr_tables_in_query = len(to_visit)
    for current in to_visit:
        can_join = True if len(visited) == 0 else False
        for included_table in visited:
            can_join = can_join if can_join is True else current in schema[included_table].tablename_to_join
        if can_join:
            visited.append(current)
    return len(visited) == nbr_tables_in_query


def create_all_plans(schema, logical_query):
    all_possible_joinable_tables = []
    for i, comb in enumerate(list(itertools.permutations(logical_query))):
        if dfs(schema, [], list(comb)):
            all_possible_joinable_tables.append(comb)
    return all_possible_joinable_tables
