from decouple import config


def postgres_connection():
    return {
        'host': config('DATABASE_CONNECTION__HOST'),
        'database': config('DATABASE_CONNECTION__DATABASE'),
        'user': config('DATABASE_CONNECTION__USER'),
        'password': config('DATABASE_CONNECTION__PASSWORD')
    }
