import json
from time import sleep

import numpy as np
from cjio.cityjson import CityJSON
from pyproj import Transformer

transformer = Transformer.from_crs(4326, 2100)
points = [(22.95, 40.63), (22.81, 40.53), (23.51, 40.86)]

TOPLEVEL = ('Building',
            'Bridge',
            'CityObjectGroup',
            'CityFurniture',
            'GenericCityObject',
            'LandUse',
            'PlantCover',
            'Railway',
            'Road',
            'SolitaryVegetationObject',
            'TINRelief',
            'TransportSquare',
            'Tunnel',
            'WaterBody')
DEFAULT_DB = 'cityjson'
DEFAULT_SCHEMA = 'addcolumns'
FETCH_SIZE = 1000

CHILDREN = """
            -- get children of original query
            children AS(
            SELECT unnest(parents) as main_id, obj_id, object,vertices
            FROM city_object
            WHERE obj_id IN (SELECT unnest(children) FROM origin))
            
            SELECT main_id, obj_id, object,vertices FROM origin
            UNION SELECT * FROM children
            ORDER BY main_id
            """


def update_geom_indices(a, offset):
    for i, each in enumerate(a):
        if isinstance(each, list):
            update_geom_indices(each, offset)
        else:
            if each is not None:
                a[i] = each + offset


def add_cityobject(cityjson, id, object, vertices):
    offset = len(cityjson.j["vertices"])
    cityjson.j["vertices"] += vertices
    cityjson.j["CityObjects"][id] = object

    for g in cityjson.j['CityObjects'][id]['geometry']:
        update_geom_indices(g["boundaries"], offset)


def query_items(file_name=None, schema_name=DEFAULT_SCHEMA, limit=10, offset=0):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations

    query_origin = """
        SET search_path to {}, public;
                
        -- original query
        WITH origin AS (
        SELECT obj_id as main_id, obj_id, c.object, vertices,children
        FROM city_object AS c JOIN metadata AS m ON c.metadata_id=m.id
        WHERE name=%s and type In {}
        limit {} offset {}),
        """.format(schema_name, TOPLEVEL, limit, offset)

    query_cityobjects = query_origin + CHILDREN
    cur.execute(query_cityobjects, [file_name])

    object_cityobjects = cur.fetchall()
    threaded_postgreSQL_pool.putconn(conn)
    cityjson = CityJSON()
    cityjson.j['type'] = 'CityJSON'

    for queried_cityobject in object_cityobjects:
        # todo: add versions
        id = queried_cityobject[1]
        object = queried_cityobject[2]
        vertices = queried_cityobject[3]
        add_cityobject(cityjson, id, object, vertices)

    cityjson.remove_duplicate_vertices()
    return cityjson


def filter_col_bbox(file_name=None, schema_name=DEFAULT_SCHEMA, bbox=None, epsg=None):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations

    query_origin = """
        SET search_path to {}, public;

        -- original query
        WITH origin AS (
        SELECT obj_id as main_id, obj_id, c.object, vertices,children
        FROM city_object AS c JOIN metadata AS m ON c.metadata_id=m.id
        WHERE name=%s AND type IN {} AND
        (st_transform(c.bbox, {})&&ST_Envelope('SRID={};LINESTRING({} {}, {} {} )'::geometry))),
        """.format(schema_name, TOPLEVEL, epsg, epsg, bbox[0], bbox[1], bbox[2], bbox[3])

    query_cityobjects = query_origin + CHILDREN

    def generator():
        try:
            cur.execute(query_cityobjects, [file_name])
            yield from __stream_col(cur)
        finally:
            threaded_postgreSQL_pool.putconn(conn)

    return generator()


def filter_cols_bbox(schema_name=DEFAULT_SCHEMA, bbox=None, ):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations

    Line = 'SRID=4326;LINESTRING({} {} , {} {} )'.format(bbox[0], bbox[1], bbox[2], bbox[3])
    query_cityobjects = """
        SET search_path to {}, public;
        WITH cityjson AS (
        SELECT id, name, referencesystem
        FROM metadata
        WHERE (st_transform(bbox, 4326)&&ST_Envelope('{}'::geometry))),
        
        origin_top AS (
        SELECT obj_id as main_id, obj_id, c.object, vertices, name, referencesystem
        FROM city_object AS c JOIN cityjson AS m ON c.metadata_id=m.id
        WHERE metadata_id = m.id AND type IN {} AND
        (st_transform(bbox, 4326)&&ST_Envelope('{}'::geometry))),
        
        origin_part AS (
        SELECT obj_id, c.object, vertices,parents,name, referencesystem
        FROM city_object AS c JOIN cityjson AS m ON c.metadata_id=m.id
        WHERE metadata_id = m.id  AND type NOT IN {} AND
        (st_transform(bbox, 4326)&&ST_Envelope('{}'::geometry))),
        
        -- get parents of original query_part
        parents AS(
        SELECT obj_id as main_id, obj_id, object,vertices,children, name, referencesystem
        FROM city_object AS c JOIN cityjson AS m ON c.metadata_id=m.id
        WHERE metadata_id = m.id  AND obj_id IN (SELECT unnest(parents) FROM origin_part)),
        
        -- get siblings of original query
        siblings AS(
        SELECT unnest(parents) as main_id, obj_id, object,vertices, name, referencesystem
        FROM city_object AS c JOIN cityjson AS m ON c.metadata_id=m.id
        WHERE metadata_id = m.id  AND obj_id IN (SELECT unnest(children) FROM parents))
        
        
        SELECT main_id, obj_id, object,vertices, name, referencesystem FROM origin_top
        UNION SELECT main_id, obj_id, object,vertices, name, referencesystem FROM parents
        UNION SELECT * FROM siblings
        ORDER BY name, main_id
        """.format(schema_name, Line, TOPLEVEL, Line, TOPLEVEL, Line)
    print(query_cityobjects)

    def generator():
        try:
            cur.execute(query_cityobjects)
            yield from __stream_cols(cur)
        finally:
            threaded_postgreSQL_pool.putconn(conn)

    return generator()


def __stream_col(cursor):
    while True:
        rows = cursor.fetchmany(FETCH_SIZE)
        if not rows:
            break
        main_id = rows[0][0]
        cityjson = CityJSON()
        for row in rows:
            if row[0] != main_id:
                cityjson.remove_duplicate_vertices()
                cj_feature = {
                    "type": "CityJSONFeature",
                    "id": main_id,
                    "CityObjects": cityjson.j['CityObjects'],
                    "vertices": cityjson.j['vertices']
                }
                yield cj_feature
                sleep(0.5)

                cityjson = CityJSON()
                main_id = row[0]
            id = row[1]
            object = row[2]
            vertices = row[3]
            add_cityobject(cityjson, id, object, vertices)


def __stream_cols(cursor):
    while True:
        rows = cursor.fetchmany(FETCH_SIZE)
        if not rows:
            break
        main_id = rows[0][0]
        cityjson = CityJSON()
        for row in rows:
            if row[0] != main_id:
                cityjson.remove_duplicate_vertices()
                cj_feature = {
                    "type": "CityJSONFeature",
                    "id": main_id,
                    "CityObjects": cityjson.j['CityObjects'],
                    "vertices": cityjson.j['vertices']
                }
                yield cj_feature
                sleep(0.5)

                cityjson = CityJSON()
                main_id = row[0]
            id = row[1]
            object = row[2]
            object['file'] = row[4]
            vertices = row[3]
            crs = int(row[5])
            transformed_vertices = []

            if len(vertices) > 1:
                transformer = Transformer.from_crs(crs, 4326)
                for pt in transformer.itransform(vertices):
                    transformed_vertices.append(pt)

            add_cityobject(cityjson, id, object, transformed_vertices)


def query_collections(schema_name=DEFAULT_SCHEMA):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations
    collections = []

    query_metadata = """SELECT name,datasetTitle FROM {}.metadata""".format(schema_name)

    cur.execute(query_metadata)
    object_metadata = cur.fetchall()
    threaded_postgreSQL_pool.putconn(conn)

    for name, datasetTitle in object_metadata:
        collections.append({"name": name, "title": datasetTitle})
    return collections


def query_feature(file_name=None, schema_name=DEFAULT_SCHEMA, feature_id=None):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations

    query_origin = """
            SET search_path to {}, public;

            -- original query
            WITH origin AS (
            SELECT obj_id as main_id, obj_id, c.object, vertices,children
            FROM city_object AS c JOIN metadata AS m ON c.metadata_id=m.id
            WHERE name=%s and obj_id=%s),
            """.format(schema_name)

    query_cityobjects = query_origin + CHILDREN
    cur.execute(query_cityobjects, [file_name, feature_id])

    object_cityobjects = cur.fetchall()
    threaded_postgreSQL_pool.putconn(conn)
    cityjson = CityJSON()
    cityjson.j['type'] = 'CityJSON'

    main_id = object_cityobjects[0][0]

    for queried_cityobject in object_cityobjects:
        # todo: add versions
        id = queried_cityobject[1]
        object = queried_cityobject[2]
        vertices = queried_cityobject[3]
        add_cityobject(cityjson, id, object, vertices)

    cityjson.remove_duplicate_vertices()
    cj_feature = {
        "type": "CityJSONFeature",
        "id": main_id,
        "CityObjects": cityjson.j['CityObjects'],
        "vertices": cityjson.j['vertices']
    }
    return cj_feature


def query_col_bbox(file_name=None, schema_name=DEFAULT_SCHEMA):
    try:
        conn = threaded_postgreSQL_pool.getconn()
        cur = conn.cursor()  # Open a cursor to perform database operations
        bbox_wgs84, bbox_original, epsg = None, None, 0

        query_bbox = """
                SET search_path to {}, public;

                SELECT st_asgeojson(st_transform(bbox, 4326)),st_asgeojson(bbox), referencesystem
                FROM metadata
                WHERE name=%s and referencesystem is not null
                """.format(schema_name)
        cur.execute(query_bbox, [file_name])
        results = cur.fetchall()
        if len(results) > 0:
            geo_wgs84 = json.loads(results[0][0])
            bbox = geo_wgs84['coordinates'][0]
            pts_xy = np.array(bbox).T[:2]
            min_xy = pts_xy.min(axis=1)
            max_xy = pts_xy.max(axis=1)
            bbox_wgs84 = [[min_xy[1], min_xy[0]], [max_xy[1], max_xy[0]]]
            geo_original = json.loads(results[0][1])
            bbox = geo_original['coordinates'][0]
            pts_xy = np.array(bbox).T[:2]
            min_xy = pts_xy.min(axis=1)
            max_xy = pts_xy.max(axis=1)
            bbox_original = [[min_xy[1], min_xy[0]], [max_xy[1], max_xy[0]]]
            epsg = results[0][2]

        return bbox_wgs84, bbox_original, epsg
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return


def query_cols_bbox(schema_name=DEFAULT_SCHEMA):
    try:
        conn = threaded_postgreSQL_pool.getconn()
        cur = conn.cursor()  # Open a cursor to perform database operations

        query_bboxes = """
                SET search_path to {}, public;

                SELECT st_asgeojson(st_transform(bbox, 4326))
                FROM metadata
                WHERE referencesystem IS NOT null
                """.format(schema_name)
        cur.execute(query_bboxes)
        bboxes = []
        for result in cur.fetchall():
            bbox = json.loads(result[0])['coordinates'][0]
            pts_xy = np.array(bbox).T[:2]
            min_xy = pts_xy.min(axis=1)
            max_xy = pts_xy.max(axis=1)
            bboxes.append([[min_xy[1], min_xy[0]], [max_xy[1], max_xy[0]]])
        return bboxes
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return


import psycopg2
from psycopg2 import pool

threaded_postgreSQL_pool = None
try:
    threaded_postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool \
        (5, 20, user="postgres",
         password="1234",
         host="127.0.0.1",
         port="5432",
         database=DEFAULT_DB)
    if (threaded_postgreSQL_pool):
        print("Connection pool created successfully using ThreadedConnectionPool")
        # _test = query_col_bbox(file_name='DA13_3D_Buildings_Merged')
        # print(_test)
        # for i in filter_cols_bbox(bbox=[4.26787, 52.10574, 4.26954, 52.10669]):
        #     print(i)

        # Get String after substring occurrence
        # transformer = Transformer.from_crs(4326, 28992)
        # points = [(22.95, 40.63), (22.81, 40.53), (23.51, 40.86)]
        # for pt in transformer.itransform(points):
        #     print('{:.3f} {:.3f}'.format(*pt))




finally:
    if threaded_postgreSQL_pool:
        threaded_postgreSQL_pool.closeall
    print("Threaded PostgreSQL connection pool is closed")
