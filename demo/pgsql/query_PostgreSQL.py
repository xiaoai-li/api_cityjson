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


def query_items(file_name=None, schema_name='addcolumns', limit=10, offset=0, is_tmp=False):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations
    if not is_tmp:
        query_origin = """
            SET search_path to {}, public;
            
            -- original query
            WITH origin AS (
            SELECT obj_id, c.metadata_id, c.object, vertices,version,parents,children
            FROM city_object AS c JOIN metadata AS m ON c.metadata_id=m.id
            WHERE name=%s
            ORDER BY tile_id
            LIMIT {} OFFSET {}),
            """.format(schema_name, limit, offset)
    else:
        query_origin = """
            SET search_path to {}, public;

            -- original query
            WITH origin AS (
            SELECT obj_id, c.metadata_id, c.object, vertices,version,parents,children
            FROM city_object AS c JOIN metadata AS m ON c.metadata_id=m.id
			WHERE c.id in (SELECT unnest(cityobjects) FROM metadata WHERE name =%s)
            ORDER BY tile_id
            LIMIT {} OFFSET {}),
            """.format(schema_name, limit, offset)

    query_cityobjects = """
        {}

        -- get parents of original query
        parents AS(
        SELECT obj_id, object,vertices,children
        FROM city_object
        WHERE obj_id IN (SELECT unnest(parents) FROM origin)),

        -- get children of original query
        children AS(
        SELECT obj_id, object,vertices
        FROM city_object
        WHERE obj_id IN (SELECT unnest(children) FROM origin)),

        -- get siblings of original query
        siblings AS(
        SELECT obj_id, object,vertices
        FROM city_object
        WHERE obj_id IN (SELECT unnest(children) FROM parents))

        SELECT obj_id, object,vertices FROM origin
        UNION
        SELECT obj_id, object,vertices FROM parents
        UNION SELECT * FROM children
        UNION SELECT * FROM siblings
        """.format(query_origin)
    cur.execute(query_cityobjects, [file_name])

    object_cityobjects = cur.fetchall()
    threaded_postgreSQL_pool.putconn(conn)
    cityjson = CityJSON()
    cityjson.j['type'] = 'CityJSON'

    for queried_cityobject in object_cityobjects:
        # todo: add versions
        id = queried_cityobject[0]
        object = queried_cityobject[1]
        vertices = queried_cityobject[2]
        add_cityobject(cityjson, id, object, vertices)

    cityjson.remove_duplicate_vertices()
    return cityjson


def filter_col_bbox(file_name=None, schema_name='addcolumns', bbox=None, epsg=None):
    print(bbox, epsg)
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations
    delete_tmp = """
        SET search_path TO {}, public; 
        DELETE FROM metadata WHERE cityobjects is not null AND timestamp < NOW() - INTERVAL '30 minute';
        """.format(schema_name)
    cur.execute(delete_tmp)
    conn.commit()

    query_version = """    
        SELECT version,referencesystem
        FROM metadata
        WHERE name=%s """
    cur.execute(query_version, [file_name])
    results = cur.fetchall()[0]
    version = results[0]
    ref_crs = results[1]
    geo_crs = 0

    if not epsg:
        query_origin = """
            SET search_path to {}, public;

            -- original query
            WITH origin AS (
            SELECT c.id,parents,children
            FROM city_object AS c JOIN metadata AS m ON c.metadata_id=m.id
            WHERE name=%s AND 
            (c.bbox&&ST_Envelope('LINESTRING({} {}, {} {})'::geometry))
            ORDER BY tile_id),
            """.format(schema_name, bbox[0], bbox[1], bbox[2], bbox[3])
    else:
        geo_crs = epsg
        query_origin = """
            SET search_path to {}, public;

            -- original query
            WITH origin AS (
            SELECT c.id,parents,children
            FROM city_object AS c JOIN metadata AS m ON c.metadata_id=m.id
            WHERE name=%s AND 
            (st_transform(c.bbox, {})&&ST_Envelope('SRID={};LINESTRING({} {}, {} {} )'::geometry))
            ORDER BY tile_id),
            """.format(schema_name, epsg, epsg, bbox[0], bbox[1], bbox[2], bbox[3])

    query_cityobjects = """
        {}

        -- get parents of original query
        parents AS(
        SELECT id,children
        FROM city_object
        WHERE obj_id IN (SELECT unnest(parents) FROM origin)),

        -- get children of original query
        children AS(
        SELECT id
        FROM city_object
        WHERE obj_id IN (SELECT unnest(children) FROM origin)),

        -- get siblings of original query
        siblings AS(
        SELECT id
        FROM city_object
        WHERE obj_id IN (SELECT unnest(children) FROM parents))

        SELECT id FROM origin
        UNION
        SELECT id FROM parents
        UNION SELECT * FROM children
        UNION SELECT * FROM siblings
        """.format(query_origin)
    cur.execute(query_cityobjects, [file_name])
    object_cityobjects = cur.fetchall()

    # todo: numpy
    cityobjects = []
    for queried_cityobject in object_cityobjects:
        # todo: add versions
        cityobjects.append(queried_cityobject[0])

    insert_metadata = """
        INSERT INTO metadata 
        (name, version,cityobjects, bbox,referencesystem) 
        VALUES (concat(%s::text,'_filtered_',CURRVAL('metadata_id_seq')::text), %s, %s,st_transform(ST_MakeEnvelope({}, {}, {}, {}, {}), {}),%s)
        """.format(bbox[0], bbox[1], bbox[2], bbox[3], geo_crs, ref_crs)
    cur.execute(insert_metadata, (file_name, version, cityobjects, ref_crs))
    conn.commit()

    query_name = """ SELECT name FROM metadata where id=CURRVAL('metadata_id_seq')"""
    cur.execute(query_name)
    name = cur.fetchall()[0][0]
    threaded_postgreSQL_pool.putconn(conn)
    return name


def query_collections(schema_name, with_tmps=False):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations
    collections = []
    if not with_tmps:
        query_metadata = """SELECT name,datasetTitle FROM {}.metadata WHERE cityobjects is null""".format(schema_name)
    else:
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
    file_name = file_name.partition('_filtered_')[0]

    query_origin = """
            SET search_path to {}, public;

            -- original query
            WITH origin AS (
            SELECT obj_id, c.metadata_id, c.object, vertices,version,parents,children
            FROM city_object AS c JOIN metadata AS m ON c.metadata_id=m.id
            WHERE name=%s and obj_id=%s),
            """.format(schema_name)

    query_cityobjects = """
            {}

            -- get parents of original query
            parents AS(
            SELECT obj_id, object,vertices,children
            FROM city_object
            WHERE obj_id IN (SELECT unnest(parents) FROM origin)),

            -- get children of original query
            children AS(
            SELECT obj_id, object,vertices
            FROM city_object
            WHERE obj_id IN (SELECT unnest(children) FROM origin)),

            -- get siblings of original query
            siblings AS(
            SELECT obj_id, object,vertices
            FROM city_object
            WHERE obj_id IN (SELECT unnest(children) FROM parents))

            SELECT obj_id, object,vertices FROM origin
            UNION
            SELECT obj_id, object,vertices FROM parents
            UNION SELECT * FROM children
            UNION SELECT * FROM siblings
            """.format(query_origin)
    cur.execute(query_cityobjects, [file_name, feature_id])

    object_cityobjects = cur.fetchall()
    threaded_postgreSQL_pool.putconn(conn)
    cityjson = CityJSON()
    cityjson.j['type'] = 'CityJSON'

    for queried_cityobject in object_cityobjects:
        # todo: add versions
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
        # query_items(file_name='3-20-DELFSHAVEN_filtered_10', schema_name='addcolumns', limit=10, offset=0, is_tmp=True)
        # filter_col_bbox(file_name='denhaag', bbox=[4.26787, 52.10574, 4.26954, 52.10669], epsg=4326)
        # Get String after substring occurrence



finally:
    if threaded_postgreSQL_pool:
        threaded_postgreSQL_pool.closeall
    print("Threaded PostgreSQL connection pool is closed")
