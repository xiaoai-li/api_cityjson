import json

import numpy as np
import ujson
from cjio.cityjson import CityJSON
from time import sleep


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

REALATIVES = """
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

    query_cityobjects = query_origin + REALATIVES
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

    query_cityobjects = query_origin + REALATIVES

    def generator():
        try:
            cur.execute(query_cityobjects, [file_name])
            yield from __dumps(cur)
        finally:
            threaded_postgreSQL_pool.putconn(conn)

    return generator()


def __dumps(cursor):
    while True:
        rows = cursor.fetchmany(FETCH_SIZE)
        if not rows:
            break
        main_id = rows[0][0]
        print(main_id)
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

    query_cityobjects = query_origin + REALATIVES
    cur.execute(query_cityobjects, [file_name, feature_id])

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


def query_col_bbox(file_name=None, schema_name=DEFAULT_SCHEMA):
    try:
        conn = threaded_postgreSQL_pool.getconn()
        cur = conn.cursor()  # Open a cursor to perform database operations

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
        else:
            query_bbox = """
                    SET search_path to {}, public;

                    SELECT st_asgeojson(bbox)
                    FROM metadata
                    WHERE name=%s 
                    """.format(schema_name)
            cur.execute(query_bbox, [file_name])
            results = cur.fetchall()
            bbox_wgs84 = None
            geo_original = json.loads(results[0][0])
            bbox = geo_original['coordinates'][0]
            pts_xy = np.array(bbox).T[:2]
            min_xy = pts_xy.min(axis=1)
            max_xy = pts_xy.max(axis=1)
            bbox_original = [[min_xy[1], min_xy[0]], [max_xy[1], max_xy[0]]]
            epsg = None

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
        # query_items(file_name='3-20-DELFSHAVEN_filtered_10', schema_name='addcolumns')

            # Get String after substring occurrence



finally:
    if threaded_postgreSQL_pool:
        threaded_postgreSQL_pool.closeall
    print("Threaded PostgreSQL connection pool is closed")
