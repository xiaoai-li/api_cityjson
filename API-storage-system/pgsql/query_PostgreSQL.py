import sys
import time

import ujson
from cjio.cityjson import CityJSON
import pyproj
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

CHUNK_SIZE = 100


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

                SELECT referencesystem, transform,id
                FROM cityjson
                WHERE name=%s
                """.format(schema_name)
    cur.execute(query_cm_info, [file_name])
    cm_info = cur.fetchall()
    if cm_info:
        cityjson.j["metadata"] = {}
        cityjson.set_epsg(cm_info[0][0])
        cityjson.j["transform"] = cm_info[0][1]
        cj_info = {
            "type": "MetadataCityJSONFeature",
            "metadata": cityjson.j["metadata"],
            "transform": cityjson.j["transform"]
        }
        cityjson_id = cm_info[0][2]
    else:
        return None

    if epsg and bbox:
        if epsg != 4326:
            p_to = pyproj.CRS("epsg:4326")
            p_from = pyproj.CRS("epsg:" + str(epsg))
            transformer = pyproj.Transformer.from_crs(p_from, p_to, always_xy=True)
            min_xy = transformer.transform(bbox[0], bbox[1])
            max_xy = transformer.transform(bbox[2], bbox[3])


        query_bbox = """
        bbox&& box '(({}, {}),({}, {}))'
        """.format(min_xy[0], min_xy[1], max_xy[0], max_xy[1])
    else:
        query_bbox = "True "

    if attrs:
        query_attr = ""
        for attr in attrs:
            value = attrs[attr]
            if attr == "type":
                if len(value) == 1:
                    query_attr += "AND type IN ('{}') ".format(value[0])
                else:
                    query_attr += "AND type IN {} ".format(tuple(value))
            elif isinstance(value, list):
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
                    min = value["range"][0]
                    max = value["range"][1]
                    query_attr += "AND (attributes->> '{}')::float >= {} AND (attributes->> '{}')::float <= {} ".format(
                        attr, min, attr, max)

    else:
        query_attr = "AND True "

    query_sub = """
           SET search_path to {}, public;
    
            -- original query
            WITH origin AS (
            SELECT obj_id,object,vertices
            FROM cityobject
            WHERE cityjson_id=%s {} AND type In {}
            and {} ),
    
            -- get children of original query
            children AS(
            SELECT child_id as obj_id,object,vertices
            FROM  parent_children as pc join cityobject as c on pc.child_id=c.obj_id
            WHERE pc.cityjson_id=%s and parent_id IN (SELECT obj_id FROM origin))
    
            SELECT * from origin
            UNION ALL 
            SELECT * from children
                """.format(schema_name, query_attr, TOPLEVEL, query_bbox)
    cur.execute(query_sub, [cityjson_id, cityjson_id])
    while True:
        cityobjects = cur.fetchmany(2000)
        if not cityobjects:
            break

        for queried_cityobject in cityobjects:
            # todo: add versions
            id = queried_cityobject[0]
            object = queried_cityobject[1]
            vertices = queried_cityobject[2]
            add_cityobject(cityjson, id, object, vertices)
        cityjson.remove_duplicate_vertices()

    cityjson.remove_duplicate_vertices()
    threaded_postgreSQL_pool.putconn(conn)

    return ujson.dumps(cityjson.j, ensure_ascii=False,
                       escape_forward_slashes=True)


def query_item(file_name=None, schema_name=DEFAULT_SCHEMA, feature_id=None):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations

    cityjson = CityJSON()
    cityjson.j['type'] = 'CityJSON'
    query_cm_info = """
                SET search_path to {}, public;

                SELECT referencesystem, transform,id
                FROM cityjson
                WHERE name=%s
                """.format(schema_name)
    cur.execute(query_cm_info, [file_name])
    cm_info = cur.fetchall()
    if cm_info:
        cityjson.j["metadata"] = {}
        cityjson.set_epsg(cm_info[0][0])
        cityjson.j["transform"] = cm_info[0][1]
        cityjson_id = cm_info[0][2]
    else:
        return None

    query_cityfeature = """
        SET search_path to {}, public;

        -- original query
        WITH origin AS (
        SELECT obj_id, object,vertices
        FROM cityobject 
        WHERE cityjson_id=%s and obj_id=%s),

        children AS(
        SELECT obj_id, object,vertices
        FROM cityobject AS c JOIN parent_children AS m ON c.obj_id=m.child_id
        WHERE c.cityjson_id=%s and parent_id IN (SELECT obj_id FROM origin))

        SELECT * FROM origin
        UNION ALL SELECT * FROM children
        """.format(schema_name)
    cur.execute(query_cityfeature, [cityjson_id, feature_id, cityjson_id])

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
    return ujson.dumps(cityjson.j, ensure_ascii=False,
                       escape_forward_slashes=True)


def query_items(file_name=None, schema_name=DEFAULT_SCHEMA, limit=None, offset=0):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations

    cityjson = CityJSON()
    cityjson.j['type'] = 'CityJSON'
    query_cm_info = """
                SET search_path to {}, public;

                SELECT referencesystem, transform,id
                FROM cityjson
                WHERE name=%s
                """.format(schema_name)
    cur.execute(query_cm_info, [file_name])
    cm_info = cur.fetchall()

    cityjson.j["metadata"] = {}
    cityjson.set_epsg(cm_info[0][0])
    cityjson.j["transform"] = cm_info[0][1]
    cityjson_id = cm_info[0][2]
    if limit:
        query_cityobjects = """
            SET search_path to {}, public;
    
            -- original query
            WITH origin AS (
            SELECT obj_id, object,vertices
            FROM cityobject 
            WHERE cityjson_id={} and type In {}
            Limit {} offset {}),
    
            -- get children of original query
            children AS(
            SELECT obj_id, object,vertices
            FROM cityobject AS c JOIN parent_children AS m ON c.obj_id=m.child_id
            WHERE c.cityjson_id={} and parent_id IN (SELECT obj_id FROM origin))
    
            SELECT * from origin
            UNION SELECT * FROM children
            """.format(schema_name, cityjson_id, TOPLEVEL, limit, offset, cityjson_id)
    else:
        query_cityobjects = """
           SET search_path to {}, public;

           SELECT obj_id, object,vertices
           FROM cityobject 
           WHERE cityjson_id={}
           """.format(schema_name, cityjson_id, TOPLEVEL, limit, offset, cityjson_id)

    cur.execute(query_cityobjects)
    t1 = time.time()

    while True:
        cityobjects = cur.fetchmany(2000)
        print(2000)
        if not cityobjects:
            break

        for queried_cityobject in cityobjects:
            # todo: add versions
            id = queried_cityobject[0]
            object = queried_cityobject[1]
            vertices = queried_cityobject[2]
            add_cityobject(cityjson, id, object, vertices)
        cityjson.remove_duplicate_vertices()

    t2 = time.time()
    print("Time for constructing the file:   ", t2 - t1)

    cityjson.remove_duplicate_vertices()
    t3 = time.time()
    print("Time for removing duplicate vertices:   ", t3 - t2)
    # cityjson.update_bbox()
    print('start sending file')

    return ujson.dumps(cityjson.j, ensure_ascii=False,
                       escape_forward_slashes=True)


def query_col_info(file_name=None, schema_name=DEFAULT_SCHEMA):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations

    info = {'id': file_name}
    query_info = """
            SET search_path to {}, public;

            SELECT bbox, referencesystem, meta_attr, datasettitle
            FROM cityjson
            WHERE name=%s 
            """.format(schema_name)
    cur.execute(query_info, [file_name])
    results = cur.fetchall()
    info['metadata_attributes'] = results[0][2]
    info['description'] = results[0][3]
    info['crs'] = results[0][1]
    info['bbox'] = results[0][0]

    threaded_postgreSQL_pool.putconn(conn)
    return ujson.dumps(info, ensure_ascii=False,
                       escape_forward_slashes=True)


def query_cols_info(schema_name=DEFAULT_SCHEMA):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations

    query_info = """
            SET search_path to {}, public;

            SELECT bbox,name,referencesystem,datasettitle
            FROM cityjson
            """.format(schema_name)
    cur.execute(query_info)
    info = {}
    for result in cur.fetchall():
        info[result[1]] = {}
        info[result[1]]['bbox'] = result[0]
        info[result[1]]['crs'] = result[2]
        info[result[1]]['description'] = result[3]

    threaded_postgreSQL_pool.putconn(conn)
    return ujson.dumps(info, ensure_ascii=False,
                       escape_forward_slashes=True)


import psycopg2
from psycopg2 import pool

threaded_postgreSQL_pool = None
try:
    threaded_postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool \
        (5, 1000, **params_dic)
    # threaded_postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool(5, 100, os.environ['PSYCOPG2_POSTGRESQL_URI'])

    if (threaded_postgreSQL_pool):
        print("Connection pool created successfully using ThreadedConnectionPool")
except:
    print("Connection pool creation failed")

#
# def query_items(file_name=None, schema_name=DEFAULT_SCHEMA, limit=99999999, offset=0):
#     conn = connect()
#     cur = conn.cursor()
#     # Open a cursor to perform database operations
#
#     cityjson = CityJSON()
#     cityjson.j['type'] = 'CityJSON'
#     query_cm_info = """
#                 SET search_path to {}, public;
#
#                 SELECT referencesystem, transform,id
#                 FROM cityjson
#                 WHERE name=%s
#                 """.format(schema_name)
#     cur.execute(query_cm_info, [file_name])
#     cm_info = cur.fetchall()
#
#     cityjson.j["metadata"] = {}
#     cityjson.set_epsg(cm_info[0][0])
#     cityjson.j["transform"] = cm_info[0][1]
#
#     query_cityobjects = """
#     SET search_path to sixth, public;
#
#     SELECT obj_id,object,vertices
#     FROM cityobject
#     WHERE cityjson_id=1 and type in ('Building', 'Road', 'WaterBody', 'PlantCover', 'Bridge', 'GenericCityObject')
#     LIMIT {}
#     """.format(limit)
#
#     cur.execute(query_cityobjects)
#     while True:
#         cityobjects = cur.fetchmany(1000)
#         if not cityobjects:
#             break
#
#         for queried_cityobject in cityobjects:
#             # todo: add versions
#             id = queried_cityobject[0]
#             object = queried_cityobject[1]
#             vertices = queried_cityobject[2]
#             add_cityobject(cityjson, id, object, vertices)
#
#     cityjson.remove_duplicate_vertices()
#     cityjson.update_bbox()
#     save_path = PATHDATASETS + file_name + 'noLandUse.json'
#     with open(save_path, 'w') as fout:
#         json_str = ujson.dumps(cityjson.j)
#         fout.write(json_str)
#
#
# query_items(file_name='37en2_volledig', schema_name=DEFAULT_SCHEMA, limit=60595)