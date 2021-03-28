from decouple import config
from sqlalchemy import create_engine


def postgres_connection():
    return {
        'host': config('DATABASE_CONNECTION__HOST'),
        'database': config('DATABASE_CONNECTION__DATABASE'),
        'user': config('DATABASE_CONNECTION__USER'),
        'password': config('DATABASE_CONNECTION__PASSWORD')
    }


def create_postgres_engine():
    creds = postgres_connection()
    return create_engine(
        'postgresql://{}:{}@{}:5432/{}'.format(creds['user'], creds['password'],
                                               creds['host'], creds['database']))
