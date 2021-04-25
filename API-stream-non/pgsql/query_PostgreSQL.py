import sys

import ujson
from cjio.cityjson import CityJSON
from pyproj import Proj, transform

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

CHUNK_SIZE = 10000

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
        if epsg != cm_info[0][0]:
            inProj = Proj("+init=EPSG:" + str(epsg))
            outProj = Proj("+init=EPSG:" + str(cm_info[0][0]))
            min_xy = transform(inProj, outProj, bbox[0], bbox[1])
            max_xy = transform(inProj, outProj, bbox[2], bbox[3])
            bbox = [min_xy[0], min_xy[1], max_xy[0], max_xy[1]]

        query_bbox = """
        not (bbox[1]>{} or bbox[3]<{} or bbox[2]>{} or bbox[4]<{})
        """.format(bbox[2], bbox[0], bbox[3], bbox[1])
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

    def generator():
        try:
            yield ujson.dumps(cj_info) + '\n'
            query_cityobjects = """
                SET search_path to {}, public;

                -- original query
                WITH origin AS (
		        SELECT obj_id as main_id, obj_id, object,vertices
                FROM cityobject 
                WHERE cityjson_id=%s AND {} {}),

                -- get children of original query
                children AS(
                SELECT m.parent_id as main_id,obj_id, object,vertices
                FROM cityobject AS c JOIN parent_children AS m ON c.obj_id=m.child_id
                WHERE c.cityjson_id=%s and parent_id IN (SELECT obj_id FROM origin))

                SELECT * FROM origin
                WHERE obj_id not in (SELECT obj_id FROM children)
                UNION SELECT * FROM children
                ORDER BY main_id
                """.format(schema_name, query_bbox, query_attr)
            cur.execute(query_cityobjects, [cityjson_id, cityjson_id])
            yield from __dumps(cur)
        finally:
            threaded_postgreSQL_pool.putconn(conn)

    return generator()


def __dumps(cur):
    response=""
    while True:
        rows = cur.fetchall()
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
                response=response+ ujson.dumps(cj_feature, ensure_ascii=False,
                          escape_forward_slashes=True) + '\n'

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
        response = response + ujson.dumps(cj_feature, ensure_ascii=False,
                                          escape_forward_slashes=True) + '\n'
    return response


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
        UNION SELECT * FROM children
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

    cj_feature = {
        "type": "CityJSONFeature",
        "id": feature_id,
        "CityObjects": cityjson.j['CityObjects'],
        "vertices": cityjson.j['vertices'],
        "metadata": cityjson.j["metadata"],
        "transform": cityjson.j["transform"]
    }

    return ujson.dumps(cj_feature, ensure_ascii=False,
                       escape_forward_slashes=True)


def query_items(file_name=None, schema_name=DEFAULT_SCHEMA, limit=99999999, offset=0):
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

    cur.execute(query_cityobjects)
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
