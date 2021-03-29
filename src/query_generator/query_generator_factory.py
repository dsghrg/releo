from query_generator.random_generator import RandomQueryGenerator


def get_query_generator_creator(name, cfg):
    if name == 'random-generator':
        return lambda schema: RandomQueryGenerator(schema, cfg)
