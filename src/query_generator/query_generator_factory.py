from query_generator.random_generator import RandomQueryGenerator


def get_query_generator_creator(name):
    if name == 'random':
        return lambda schema: RandomQueryGenerator(schema)
