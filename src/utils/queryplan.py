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


def powerset(iterable):
    """powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"""
    s = list(iterable)
    return itertools.chain.from_iterable(itertools.combinations(s, r) for r in range(len(s) + 1))


def dfs_2(visited, schema, comb, current):
    if current.name not in visited:
        visited.add(current.name)
        for neighbour in schema[current.name].tablename_to_join.keys():
            if neighbour in comb:
                dfs_2(visited, schema, comb, schema[neighbour])


# generate all possible logical queries with n involved relations
def get_n_joinable_tables(n, schema):
    power_set = list(powerset(schema.keys()))
    n_power_set = [comb for comb in power_set if len(comb) == n]
    all_possible_joinable_tables = []

    for i, comb in enumerate(n_power_set):
        visited = set()
        dfs_2(visited, schema, comb, schema[comb[0]])
        if len(visited) == n:
            all_possible_joinable_tables.append(list(comb))
    return all_possible_joinable_tables


def create_valid_order(schema, join_order, current, left_to_join):
    if current.name not in join_order:
        join_order.append(current.name)
        left_to_join.remove(current.name)
        for neighbour in schema[current.name].tablename_to_join.keys():
            if neighbour in left_to_join:
                create_valid_order(schema, join_order, schema[neighbour], left_to_join)