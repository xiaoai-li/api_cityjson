import psycopg2

DEFAULT_DB = 'cityjson'
DEFAULT_SCHEMA = 'addcolumns'


def create_database(db_name):
    # 1. Connect to database
    conn = psycopg2.connect("dbname=postgres user=postgres password=1234")
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
    # todo: should store bbox as a geometry or an array??
    # 1. Connect to database
    conn = psycopg2.connect("""dbname={} user=postgres password=1234""".format(db_name))
    cur = conn.cursor()
    conn.autocommit = True

    command_drop = """DROP SCHEMA IF EXISTS {} CASCADE""".format(schema_name)
    command_create = """
        CREATE SCHEMA {}

        CREATE TABLE metadata (
            id serial  PRIMARY KEY,
            name text,
            version text,
            referenceSystem text,
            bbox geometry(POLYGON),
            datasetTitle text,
            object jsonb,
            UNIQUE (name, version)
        )

        CREATE TABLE city_object (
            id serial PRIMARY KEY,
            obj_id text,
            type text,
            parents text[],
            children text[],
            bbox geometry(POLYGON),
            attributes jsonb,
            vertices jsonb,
            object jsonb, -- store all properties
            metadata_id int REFERENCES metadata (id) on delete cascade on update cascade
        )

        CREATE TABLE geometries (
            id serial  PRIMARY KEY, 
            lod numeric(2,1),
            type text,
            city_object_id int REFERENCES city_object (id) on delete cascade on update cascade
        )

        CREATE TABLE surfaces (
            id serial  PRIMARY KEY,
            type text,
            attributes jsonb, -- other semantics
            geometry geometry(POLYGONZ),
            geometries_id integer REFERENCES geometries (id) on delete cascade on update cascade
        ) 
        """.format(schema_name)

    commands = [command_drop, command_create]

    for command in commands:
        cur.execute(command)
        conn.commit()

    conn.close()
    print("""The creation of schema "{}" in database "{}" is done""".format(schema_name, db_name))


def add_indices(db_name, schema_name='addcolumns'):
    conn = psycopg2.connect("""dbname={} user=postgres password=1234""".format(db_name))
    cur = conn.cursor()

    command_addindices = """
        SET search_path to {}, public;
        -- indexes on foreign keys
        CREATE INDEX ON city_object(metadata_id); 
        CREATE INDEX ON geometries(city_object_id); 
        CREATE INDEX ON surfaces(geometries_id); 

        -- geometries
        CREATE INDEX ON surfaces USING GIST(geometry);
        CREATE INDEX ON city_object USING GIST(bbox);
        CREATE INDEX ON metadata USING BTREE (bbox);
        
        -- attributs
        CREATE INDEX ON city_object(type);
         """.format(schema_name)
    cur.execute(command_addindices)
    conn.commit()
    conn.close


create_schema(DEFAULT_DB, DEFAULT_SCHEMA)
add_indices(DEFAULT_DB)
