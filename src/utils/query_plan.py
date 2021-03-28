import itertools


def powerset(iterable):
    "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
    s = list(iterable)
    return itertools.chain.from_iterable(itertools.combinations(s, r) for r in range(len(s) + 1))


def dfs(visited, schema, comb, current):
    if current.name not in visited:
        visited.add(current.name)
        for neighbour in schema[current.name].tablename_to_join.keys():
            if neighbour in comb:
                dfs(visited, schema, comb, schema[neighbour])


def is_query_plan_executable(schema, logical_plan):
    visited = set()
    dfs(visited, schema, logical_plan, schema[logical_plan[0]])
    if len(visited) == len(logical_plan):
        return True


# generate all possible logical queries with n involved relations
def generate_query_plans_with_n_relations(n, schema):
    power_set = list(powerset(schema.keys()))
    n_power_set = [comb for comb in power_set if len(comb) == n]
    all_possible_joinable_tables = []

    for i, comb in enumerate(n_power_set):
        if is_query_plan_executable(schema, comb):
            all_possible_joinable_tables.append(comb)
    return all_possible_joinable_tables
