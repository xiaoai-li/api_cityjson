import json

import numpy as np
from cjio.cityjson import CityJSON

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


def query_items(file_name=None, schema_name='addcolumns', limit=10, offset=0):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations

    # D. CityObjects
    query_cityobjects = """
        SET search_path to {}, public;
        (SELECT obj_id, c.object, vertices,version
        FROM city_object AS c JOIN metadata AS m ON c.metadata_id=m.id
        WHERE name=%s
        ORDER BY tile_id
        LIMIT {} OFFSET {})
        UNION
        SELECT obj_id, object, vertices,version
        FROM city_object, (SELECT children_flattened, version
        FROM (city_object AS c JOIN metadata AS m ON c.metadata_id=m.id), unnest(children) AS children_flattened
        WHERE name=%s
        ORDER BY tile_id
        LIMIT {} OFFSET {}) AS children
        WHERE obj_id = children_flattened;
        """.format(schema_name, limit, offset, limit, offset)
    cur.execute(query_cityobjects, [file_name, file_name])
    object_cityobjects = cur.fetchall()
    threaded_postgreSQL_pool.putconn(conn)

    cityjson = CityJSON()
    cityjson.j['type'] = 'CityJSON'

    for queried_cityobject in object_cityobjects:
        cityjson.j['version'] = queried_cityobject[3]
        id = queried_cityobject[0]
        object = queried_cityobject[1]
        vertices = queried_cityobject[2]
        add_cityobject(cityjson, id, object, vertices)

    cityjson.remove_duplicate_vertices()
    return cityjson


def query_collections(schema_name):
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


def query_feature(file_name=None, schema_name='addcolumns', feature_id=None):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations

    query_cityobjects = """
        SET search_path to {}, public;
        
        (SELECT obj_id, c.object, vertices,version
        FROM city_object AS c JOIN metadata AS m ON c.metadata_id=m.id
        WHERE name=%s and obj_id=%s)
        UNION
        SELECT obj_id, object, vertices,version
        FROM city_object, (SELECT children_flattened, version
        FROM (city_object AS c JOIN metadata AS m ON c.metadata_id=m.id), unnest(children) AS children_flattened
        WHERE name=%s AND obj_id=%s) AS children
        WHERE obj_id = children_flattened
        """.format(schema_name)
    cur.execute(query_cityobjects, [file_name, feature_id, file_name, feature_id])
    object_cityobjects = cur.fetchall()
    threaded_postgreSQL_pool.putconn(conn)

    cityjson = CityJSON()

    for queried_cityobject in object_cityobjects:
        id = queried_cityobject[0]
        object = queried_cityobject[1]
        vertices = queried_cityobject[2]
        add_cityobject(cityjson, id, object, vertices)

    cityjson.remove_duplicate_vertices()
    return cityjson


def query_col_bbox(file_name=None, schema_name='addcolumns'):
    try:
        conn = threaded_postgreSQL_pool.getconn()
        cur = conn.cursor()  # Open a cursor to perform database operations

        query_bbox = """
                SET search_path to {}, public;

                SELECT st_asgeojson(st_transform(bbox, 4326)),st_asgeojson(bbox), referencesystem
                FROM metadata
                WHERE name=%s
                """.format(schema_name)
        cur.execute(query_bbox, [file_name])
        results=cur.fetchall()[0]
        geo_wgs84 = json.loads(results[0])
        bbox = geo_wgs84['coordinates'][0]
        pts_xy = np.array(bbox).T[:2]
        min_xy = pts_xy.min(axis=1)
        max_xy = pts_xy.max(axis=1)
        bbox_wgs84 = [[min_xy[1], min_xy[0]], [max_xy[1], max_xy[0]]]
        geo_original = json.loads(results[1])
        bbox = geo_original['coordinates'][0]
        pts_xy = np.array(bbox).T[:2]
        min_xy = pts_xy.min(axis=1)
        max_xy = pts_xy.max(axis=1)
        bbox_original = [[min_xy[1], min_xy[0]], [max_xy[1], max_xy[0]]]
        epsg = results[2]

        return (bbox_wgs84, bbox_original, epsg)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return


def query_cols_bbox(schema_name='addcolumns'):
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
         database="cityjson")
    if (threaded_postgreSQL_pool):
        print("Connection pool created successfully using ThreadedConnectionPool")
        query_col_bbox('delft')


finally:
    if threaded_postgreSQL_pool:
        threaded_postgreSQL_pool.closeall
    print("Threaded PostgreSQL connection pool is closed")
