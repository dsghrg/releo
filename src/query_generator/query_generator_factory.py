from query_generator.random_generator import RandomQueryGenerator
from query_generator.train_test_set_query_generator import TrainTestSetQueryGenerator


def get_query_generator_creator(name, cfg):
    if name == 'random-generator':
        return lambda schema: RandomQueryGenerator(schema, cfg)
    if name == 'train-split-random-generator':
        return lambda schema: TrainTestSetQueryGenerator(schema, cfg)
