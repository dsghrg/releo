from sqlalchemy import create_engine


def create_postgres_engine(cfg):
    return create_engine(
        'postgresql://{}:{}@{}:5432/{}'.format(cfg['username'], cfg['password'],
                                               cfg['host'], cfg['db']))
