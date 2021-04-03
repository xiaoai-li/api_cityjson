import numpy as np
import ujson
from cjio.cityjson import CityJSON
import sys
sys.path.append('../')
from config import params_dic, DEFAULT_SCHEMA

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

CHUNK_SIZE = 50

CHILDREN = """
            -- get children of original query
            children AS(
            SELECT obj_id, object,vertices,id
            FROM city_object
            WHERE obj_id IN (SELECT unnest(children) FROM origin))

            SELECT obj_id, object,vertices,id FROM origin
            UNION SELECT * FROM children
            ORDER BY id
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


def filter_col(file_name=None, schema_name=DEFAULT_SCHEMA, attrs=None, bbox=None, epsg=None):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations
    cityjson = CityJSON()
    cityjson.j['type'] = 'CityJSON'
    query_cm_info = """
                SET search_path to {}, public;

                SELECT referencesystem, transform
                FROM cityjson
                WHERE name=%s
                """.format(schema_name)
    cur.execute(query_cm_info, [file_name])
    cm_info = cur.fetchall()

    cityjson.j["metadata"] = {}
    cityjson.set_epsg(cm_info[0][0])
    cityjson.j["transform"] = cm_info[0][1]
    cj_info = {
        "type": "MetadataCityJSONFeature",
        "metadata": cityjson.j["metadata"],
        "transform": cityjson.j["transform"]
    }

    if epsg and bbox:
        query_bbox = """
        (st_transform(c.bbox, {})&&ST_Envelope('SRID={};LINESTRING({} {}, {} {} )'::geometry)) 
        """.format(epsg, epsg, bbox[0], bbox[1], bbox[2], bbox[3])
    else:
        query_bbox = "True "

    if attrs:
        query_attr = ""
        for attr in attrs:
            value = attrs[attr]

            if isinstance(value, list):
                if len(value) == 1:
                    query_attr += "AND attributes->> '{}' IN ('{}') ".format(attr, value[0])
                else:
                    query_attr += "AND attributes->> '{}' IN {} ".format(attr, tuple(value))
            else:
                if "value" in value:
                    val = value["value"]
                    operator = value["operator"]
                    query_attr += "AND  (attributes->>'{}')::float {} {} ".format(attr, operator, val)
                if "range" in value:
                    query_attr += "AND (attributes->> '{}')::float >= {} AND (attributes->> '{}')::float <= {} ".format(
                        attr, value["range"][0], attr, value["range"][1])

    else:
        query_attr = "AND True "

    def generator():
        try:
            yield ujson.dumps(cj_info) + '\n'
            OFFSET = 0
            while True:
                query_sub = """
                    SET search_path to {}, public;
            
                    -- original query
                    WITH origin AS (
                    SELECT obj_id as main_id, obj_id, c.object, vertices,children
                    FROM cityobject AS c JOIN cityjson AS m ON c.cityjson_id=m.id
                    WHERE name=%s AND {} {} 
                    LIMIT {} OFFSET {}),
            
                    -- get children of original query
                    children AS(
                    SELECT unnest(parents)as main_id,obj_id, object,vertices
                    FROM cityobject
                    WHERE obj_id IN (SELECT unnest(children) FROM origin))
            
                    SELECT main_id, obj_id, object,vertices FROM origin
                    UNION SELECT * FROM children
                    ORDER BY main_id
                    """.format(schema_name, query_bbox, query_attr, CHUNK_SIZE, OFFSET)
                cur.execute(query_sub, [file_name])
                rows = cur.fetchall()
                if not rows:
                    break
                yield from __dumps(rows)

                OFFSET = OFFSET + CHUNK_SIZE
        finally:
            threaded_postgreSQL_pool.putconn(conn)

    return generator()


def __dumps(rows):
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
            yield ujson.dumps(cj_feature) + '\n'

            cityjson = CityJSON()
            main_id = row[0]
        id = row[1]
        object = row[2]
        vertices = row[3]
        add_cityobject(cityjson, id, object, vertices)
    cj_feature = {
        "type": "CityJSONFeature",
        "id": main_id,
        "CityObjects": cityjson.j['CityObjects'],
        "vertices": cityjson.j['vertices']
    }
    yield ujson.dumps(cj_feature) + '\n'


def query_item(file_name=None, schema_name=DEFAULT_SCHEMA, feature_id=None):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations

    cityjson = CityJSON()
    cityjson.j['type'] = 'CityJSON'
    query_cm_info = """
                SET search_path to {}, public;

                SELECT referencesystem, transform
                FROM cityjson
                WHERE name=%s
                """.format(schema_name)
    cur.execute(query_cm_info, [file_name])
    cm_info = cur.fetchall()

    cityjson.j["metadata"] = {}
    cityjson.set_epsg(cm_info[0][0])
    cityjson.j["transform"] = cm_info[0][1]

    query_cityfeature = """
        SET search_path to {}, public;

        -- original query
        WITH origin AS (
        SELECT obj_id, c.object, vertices,children
        FROM cityobject AS c JOIN cityjson AS m ON c.cityjson_id=m.id
        WHERE name=%s and obj_id=%s),

        -- get children of original query
        children AS(
        SELECT obj_id, object,vertices
        FROM cityobject
        WHERE obj_id IN (SELECT unnest(children) FROM origin))

        SELECT obj_id, object,vertices FROM origin
        UNION SELECT * FROM children
        """.format(schema_name, TOPLEVEL)
    cur.execute(query_cityfeature, [file_name, feature_id])

    results = cur.fetchall()
    threaded_postgreSQL_pool.putconn(conn)

    for result in results:
        # todo: add versions
        id = result[0]
        object = result[1]
        vertices = result[2]
        add_cityobject(cityjson, id, object, vertices)

    cityjson.remove_duplicate_vertices()
    cityjson.update_bbox()

    cj_feature = {
        "type": "CityJSONFeature",
        "id": feature_id,
        "CityObjects": cityjson.j['CityObjects'],
        "vertices": cityjson.j['vertices'],
        "metadata": cityjson.j["metadata"],
        "transform": cityjson.j["transform"]
    }

    return ujson.dumps(cj_feature)


def query_items(file_name=None, schema_name=DEFAULT_SCHEMA, limit=99999999, offset=0):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations

    cityjson = CityJSON()
    cityjson.j['type'] = 'CityJSON'
    query_cm_info = """
                SET search_path to {}, public;

                SELECT referencesystem, transform
                FROM cityjson
                WHERE name=%s
                """.format(schema_name)
    cur.execute(query_cm_info, [file_name])
    cm_info = cur.fetchall()

    cityjson.j["metadata"] = {}
    cityjson.set_epsg(cm_info[0][0])
    cityjson.j["transform"] = cm_info[0][1]
    query_cityobjects = """
        SET search_path to {}, public;

        -- original query
        WITH origin AS (
        SELECT obj_id, c.object, vertices,children
        FROM cityobject AS c JOIN cityjson AS m ON c.cityjson_id=m.id
        WHERE name=%s and attributes->> 'type' In {}
        limit {} offset {}),

        -- get children of original query
        children AS(
        SELECT obj_id, object,vertices
        FROM cityobject
        WHERE obj_id IN (SELECT unnest(children) FROM origin))

        SELECT obj_id, object,vertices FROM origin
        UNION SELECT * FROM children
        """.format(schema_name, TOPLEVEL, limit, offset)

    cur.execute(query_cityobjects, [file_name])
    cityobjects = cur.fetchall()
    threaded_postgreSQL_pool.putconn(conn)

    for queried_cityobject in cityobjects:
        # todo: add versions
        id = queried_cityobject[0]
        object = queried_cityobject[1]
        vertices = queried_cityobject[2]
        add_cityobject(cityjson, id, object, vertices)

    cityjson.remove_duplicate_vertices()
    cityjson.update_bbox()

    return ujson.dumps(cityjson.j)


def query_col_info(file_name=None, schema_name=DEFAULT_SCHEMA):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations

    info = {'id': file_name}
    query_info = """
            SET search_path to {}, public;

            SELECT st_asgeojson(st_transform(bbox, 4326)),st_asgeojson(bbox), referencesystem, meta_attr, datasettitle
            FROM cityjson
            WHERE name=%s and referencesystem is not null
            """.format(schema_name)
    cur.execute(query_info, [file_name])
    results = cur.fetchall()
    if len(results) > 0:
        info['metadata_attributes'] = results[0][3]
        info['description'] = results[0][4]
        info['crs'] = results[0][2]
        geo_original = ujson.loads(results[0][1])
        bbox = geo_original['coordinates'][0]
        pts_xy = np.array(bbox).T[:2]
        min_xy = pts_xy.min(axis=1)
        max_xy = pts_xy.max(axis=1)
        info['bbox'] = [[min_xy[1], min_xy[0]], [max_xy[1], max_xy[0]]]

        geo_wgs84 = ujson.loads(results[0][0])
        bbox = geo_wgs84['coordinates'][0]
        pts_xy = np.array(bbox).T[:2]
        min_xy = pts_xy.min(axis=1)
        max_xy = pts_xy.max(axis=1)
        info['bbox_wgs84'] = [[min_xy[1], min_xy[0]], [max_xy[1], max_xy[0]]]
    else:
        query_info = """
            SET search_path to {}, public;

            SELECT meta_attr,datasettitle 
            FROM cityjson
            WHERE name=%s 
            """.format(schema_name)
        cur.execute(query_info, [file_name])
        results = cur.fetchall()
        info['metadata_attributes'] = results[0][0]
        info['description'] = results[0][1]
    threaded_postgreSQL_pool.putconn(conn)
    return ujson.dumps(info)


def query_cols_info(schema_name=DEFAULT_SCHEMA):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations

    query_info = """
            SET search_path to {}, public;

            SELECT st_asgeojson(bbox),name,referencesystem,datasettitle
            FROM cityjson
            WHERE referencesystem IS NOT null
            """.format(schema_name)
    cur.execute(query_info)
    info = {}
    for result in cur.fetchall():
        info[result[1]] = {}
        bbox = ujson.loads(result[0])['coordinates'][0]
        pts_xy = np.array(bbox).T[:2]
        min_xy = pts_xy.min(axis=1)
        max_xy = pts_xy.max(axis=1)
        info[result[1]]['bbox'] = [[min_xy[1], min_xy[0]], [max_xy[1], max_xy[0]]]
        info[result[1]]['crs'] = result[2]
        info[result[1]]['description'] = result[3]

    query_info = """
            SELECT name, datasettitle
            FROM cityjson
            WHERE referencesystem IS null
            """
    cur.execute(query_info)
    for result in cur.fetchall():
        info[result[0]] = {}
        info[result[0]]['description'] = result[1]
    threaded_postgreSQL_pool.putconn(conn)
    return ujson.dumps(info)


import psycopg2
from psycopg2 import pool
import os

threaded_postgreSQL_pool = None
try:
    # threaded_postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool \
    #     (5, 1000, **params_dic)
    threaded_postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool(5, 100, os.environ['PSYCOPG2_POSTGRESQL_URI'])

    if (threaded_postgreSQL_pool):
        print("Connection pool created successfully using ThreadedConnectionPool")
except:
    print("Connection pool creation failed")
