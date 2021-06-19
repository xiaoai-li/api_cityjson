import psycopg2
import sys
sys.path.append('../')
from config import connect, DEFAULT_SCHEMA, DEFAULT_DB


def create_database(db_name):
    # 1. Connect to database
    conn = connect()
    cur = conn.cursor()
    conn.autocommit = True

    # 2. Create database
    command_drop = "DROP DATABASE IF EXISTS " + db_name
    command_create = "CREATE DATABASE " + db_name + " TEMPLATE template0;"  # todo:

    commands = [command_drop, command_create]

    for command in commands:
        cur.execute(command)
        conn.commit()

    # 3. Add extensions
    conn = psycopg2.connect("""dbname={} user=postgres password=1234""".format(db_name))
    cur = conn.cursor()  # Open a cursor to perform database operations

    command_addExtension = "CREATE EXTENSION IF NOT EXISTS postgis; " \
                           "CREATE EXTENSION IF NOT EXISTS postgis_sfcgal"
    cur.execute(command_addExtension)
    conn.commit()

    conn.close()
    print("""The creation of database "{}" is done""".format(db_name))


def create_schema(db_name, schema_name):
    """
    Store some attributes as columns
    """
    # 1. Connect to database
    conn = connect()
    cur = conn.cursor()
    conn.autocommit = True

    command_drop = """DROP SCHEMA IF EXISTS {} CASCADE""".format(schema_name)
    command_create = """
        CREATE SCHEMA {}

        CREATE TABLE cityjson (
            id serial  PRIMARY KEY,
            name text,
            referenceSystem int,
            bbox box,
            datasetTitle text,
            metadata jsonb,
            meta_attr jsonb,
            transform jsonb
        )

        CREATE TABLE cityobject (
            id serial PRIMARY KEY,
            obj_id text,
            type text,
            children jsonb,
            parents jsonb,
            bbox box,
            attributes jsonb,
            vertices jsonb,
            object jsonb,
            cityjson_id int REFERENCES cityjson (id) on delete cascade on update cascade
        )
        """.format(schema_name)

    commands = [command_drop, command_create]

    for command in commands:
        cur.execute(command)
        conn.commit()

    conn.close()
    print("""The creation of schema "{}" in database "{}" is done""".format(schema_name, db_name))


def add_indices(schema_name='addcolumns'):
    conn = connect()
    cur = conn.cursor()

    command_addindices = """
        SET search_path to {}, public;

        CREATE INDEX ON cityobject(bouwjaar);

         """.format(schema_name)
    cur.execute(command_addindices)
    conn.commit()
    conn.close


# create_database(DEFAULT_DB)
create_schema(DEFAULT_DB, DEFAULT_SCHEMA)
# add_indices(DEFAULT_SCHEMA)
