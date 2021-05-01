import sys

import psycopg2

PATHDATASETS = '../../data_pdok/'

DEFAULT_DB = 'cityjson'
DEFAULT_SCHEMA = 'api'
params_dic = {
    "host": "localhost",
    "database": DEFAULT_DB,
    "user": "postgres",
    "password": "1234"
}


def connect():
    """ Connect to the PostgreSQL database server """
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn