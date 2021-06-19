import copy
import re
import sys

import numpy as np
import pyproj
from cjio.cityjson import CityJSON
import json
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

CHUNK_SIZE = 5
IF_FIRST = True
# IF_FIRST=True

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


def add_feature(cm, feature):
    offset = len(cm.j["vertices"])
    _test = feature['vertices']
    cm.j["vertices"] += feature['vertices']

    for id in feature['CityObjects']:
        cm.j["CityObjects"][id] = feature['CityObjects'][id]
        for g in cm.j['CityObjects'][id]['geometry']:
            update_geom_indices(g["boundaries"], offset)


def query_cols_info(schema_name=DEFAULT_SCHEMA):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations

    query_info = """
            SET search_path to {}, public;
            SELECT name,bbox::text,metadata
            FROM cityjson
            """.format(schema_name)
    cur.execute(query_info)
    cols_info = {"collections": []}
    col_info = {"id": None,
                "title": None,
                "description": None,
                "referenceSystem": None,
                "geographicalExtent": None,
                "extent_WGS84": []}
    for result in cur.fetchall():
        col_info["id"] = result[0]
        col_info["title"] = result[0]
        if "datasetTitle" in result[2]:
            col_info["description"] = result[2]["datasetTitle"]
        if "referenceSystem" in result[2]:
            col_info["referenceSystem"] = result[2]["referenceSystem"]
        if "geographicalExtent" in result[2]:
            col_info["geographicalExtent"] = result[2]["geographicalExtent"]

        extent = [float(x) for x in re.findall(r"[-+]?\d*\.\d+|\d+", result[1])]
        # ref_system = col_info["metadata"]['referenceSystem'].split("::")[-1]
        # p_to = pyproj.CRS("epsg:4326")
        # p_from = pyproj.CRS("epsg:" + ref_system)
        # transformer = pyproj.Transformer.from_crs(p_from, p_to, always_xy=True)
        # min_xy_4236 = transformer.transform(extent[0], extent[1])
        # max_xy_4236 = transformer.transform(extent[2], extent[3])
        col_info["extent_WGS84"] = extent

        dict = copy.deepcopy(col_info)
        cols_info["collections"].append(dict)

    threaded_postgreSQL_pool.putconn(conn)
    return cols_info


def query_col_info(dataset_name=None, schema_name=DEFAULT_SCHEMA):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations

    query_info = """
            SET search_path to {}, public;
            SELECT name,bbox::text,metadata,meta_attr
            FROM cityjson
            WHERE name=%s   
            """.format(schema_name)
    cur.execute(query_info, [dataset_name])
    col_info = {"id": None,
                "title": None,
                "description": None,
                "referenceSystem": None,
                "geographicalExtent": None,
                "extent_WGS84": []}

    result = cur.fetchall()[0]
    col_info["id"] = result[0]
    col_info["title"] = result[0]
    if "datasetTitle" in result[2]:
        col_info["description"] = result[2]["datasetTitle"]
    if "referenceSystem" in result[2]:
        col_info["referenceSystem"] = result[2]["referenceSystem"]
    if "geographicalExtent" in result[2]:
        col_info["geographicalExtent"] = result[2]["geographicalExtent"]

    extent = [float(x) for x in re.findall(r"[-+]?\d*\.\d+|\d+", result[1])]
    # ref_system = col_info["metadata"]['referenceSystem'].split("::")[-1]
    # p_to = pyproj.CRS("epsg:4326")
    # p_from = pyproj.CRS("epsg:" + ref_system)
    # transformer = pyproj.Transformer.from_crs(p_from, p_to, always_xy=True)
    # min_xy_4236 = transformer.transform(extent[0], extent[1])
    # max_xy_4236 = transformer.transform(extent[2], extent[3])
    col_info["extent_WGS84"] = extent

    col_info["attribute_information"] = result[3]

    if "datasetTitle" in result[2]:
        col_info["description"] = result[2]["datasetTitle"]
    threaded_postgreSQL_pool.putconn(conn)
    return col_info


def query_items(dataset_name=None, schema_name=DEFAULT_SCHEMA, limit=99999999, offset=0):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations

    cm = CityJSON()
    cm.j['type'] = 'CityJSON'
    query_cm_info = """
                SET search_path to {}, public;

                SELECT id, version, metadata, transform
                FROM cityjson
                WHERE name=%s
                """.format(schema_name)

    cur.execute(query_cm_info, [dataset_name])
    cm_info = cur.fetchall()
    cm_id = cm_info[0][0]
    version = cm_info[0][1]
    cm.j['version'] = version
    cm.j["metadata"] = cm_info[0][2]
    cm.j["transform"] = cm_info[0][3]
    query_features = """
        SET search_path to {}, public;

        SELECT feature
        FROM cityobject 
        WHERE cityjson_id=%s and type In {}
        limit {} offset {}
        """.format(schema_name, TOPLEVEL, limit, offset)

    cur.execute(query_features, [cm_id])
    features = cur.fetchall()

    for f in features:
        feature = f[0]
        add_feature(cm, feature)
    threaded_postgreSQL_pool.putconn(conn)
    return cm.j


def filter_col_bbox(dataset_name=None, schema_name=DEFAULT_SCHEMA, bbox=None, epsg=None, is_stream=True):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()

    cm = CityJSON()
    cm.j['type'] = 'CityJSON'
    query_cm_info = """
                SET search_path to {}, public;

                SELECT id, metadata, transform
                FROM cityjson
                WHERE name=%s
                """.format(schema_name)

    cur.execute(query_cm_info, [dataset_name])
    cm_info = cur.fetchall()

    if cm_info:
        cm_id = cm_info[0][0]
        version = cm_info[0][1]
        cm.j["metadata"] = cm_info[0][1]
        cm.j["transform"] = cm_info[0][2]
        cj_info = cm.j

    else:
        return None
    if epsg and bbox:
        min_xy = [bbox[0], bbox[1]]
        max_xy = [bbox[2], bbox[3]]

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
        return None

    query_features = """
                SET search_path to {}, public;

                -- original query
                WITH origin AS (
                SELECT obj_id as main_id, obj_id,object,vertices
                FROM sixth.cityobject
                WHERE cityjson_id=%s AND type In {} and {}),
                
                -- get children of original query
                children AS(
                SELECT parent_id as main_id, obj_id,object,vertices
                FROM  sixth.cityobject
                WHERE parent_id IN (SELECT obj_id FROM origin))
                
                (SELECT main_id, obj_id, object,vertices from origin ORDER by main_id)
                UNION ALL (SELECT * from children ORDER by main_id)
                ORDER by main_id
        """.format(schema_name, TOPLEVEL, query_bbox)

    def generator():
        try:
            del cj_info['CityObjects']
            del cj_info['vertices']

            yield json.dumps(cj_info) +'\n'
            cur.execute(query_features, [cm_id])
            yield from __dumps_col(cur)

        finally:
            threaded_postgreSQL_pool.putconn(conn)

    return generator()


def filter_col_attr(dataset_name=None, schema_name=DEFAULT_SCHEMA, attrs=None):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()

    cm = CityJSON()
    cm.j['type'] = 'CityJSON'
    query_cm_info = """
                SET search_path to {}, public;

                SELECT id, metadata, transform
                FROM cityjson
                WHERE name=%s
                """.format(schema_name)

    cur.execute(query_cm_info, [dataset_name])
    cm_info = cur.fetchall()

    if cm_info:
        cm_id = cm_info[0][0]
        cm.j["metadata"] = cm_info[0][1]
        cm.j["transform"] = cm_info[0][2]
        cj_info = cm.j
    else:
        return None

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
                val = value["operand"]
                operator = value["operator"]
                query_attr += "AND  (attributes->>'{}')::float {} {} ".format(attr, operator, val)
            if "range" in value:
                min = value["range"][0]
                max = value["range"][1]
                query_attr += "AND (attributes->> '{}')::float >= {} AND (attributes->> '{}')::float <= {} ".format(
                    attr, min, attr, max)

    query_features = """
            SET search_path to {}, public;
        
            -- original query
            WITH origin AS (
            SELECT obj_id as main_id, obj_id,object,vertices
            FROM sixth.cityobject
            WHERE cityjson_id=%s {}),
        
            -- get children of original query
            children AS(
            SELECT parent_id as main_id, obj_id,object,vertices
            FROM  sixth.cityobject
            WHERE parent_id IN (SELECT obj_id FROM origin))
        
            (SELECT main_id, obj_id, object,vertices from origin ORDER by main_id)
            UNION ALL (SELECT * from children ORDER by main_id)
            ORDER by main_id

    """.format(schema_name, query_attr)
    def generator():
        try:
            del cj_info['CityObjects']
            del cj_info['vertices']

            yield json.dumps(cj_info) +'\n'
            cur.execute(query_features, [cm_id])
            yield from __dumps_col(cur)

        finally:
            threaded_postgreSQL_pool.putconn(conn)

    return generator()



def __dumps_col(cur):
    while True:
        rows = cur.fetchmany()
        if not rows:
            break

        main_id = rows[0][0]
        cityjson = CityJSON()
        for row in rows:
            if row[0] != main_id:
                if IF_FIRST:
                    break
                cityjson.j['transform'] = []
                cityjson.remove_duplicate_vertices()
                cj_feature = {
                    "type": "CityJSONFeature",
                    "id": main_id,
                    "CityObjects": cityjson.j['CityObjects'],
                    "vertices": cityjson.j['vertices']
                }
                yield json.dumps(cj_feature) + '\n'

                cityjson = CityJSON()
                main_id = row[0]
            id = row[1]
            object = row[2]
            vertices = row[3]
            add_cityobject(cityjson, id, object, vertices)
        cityjson.j['transform'] = []
        cityjson.remove_duplicate_vertices()
        cj_feature = {
            "type": "CityJSONFeature",
            "id": main_id,
            "CityObjects": cityjson.j['CityObjects'],
            "vertices": cityjson.j['vertices']
        }
        yield json.dumps(cj_feature) + '\n'
        if IF_FIRST:
            break


def query_item(dataset_name=None, schema_name=DEFAULT_SCHEMA, feature_id=None):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()  # Open a cursor to perform database operations

    query_feature = """
        SET search_path to {}, public;

        SELECT feature
        FROM cityobject as a join cityjson as b on a.cityjson_id=b.id 
        WHERE name=%s and obj_id=%s
        """.format(schema_name)

    cur.execute(query_feature, [dataset_name, feature_id])
    feature = cur.fetchall()[0][0]

    return feature


def filter_cols_bbox(schema_name=DEFAULT_SCHEMA, bbox=None, epsg=None, is_stream=True):
    conn = threaded_postgreSQL_pool.getconn()
    cur = conn.cursor()

    if epsg and bbox:
        min_xy = [bbox[0], bbox[1]]
        max_xy = [bbox[2], bbox[3]]

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
        return None

    query_cms_info = """
                SET search_path to {}, public;

                SELECT id, version, metadata, transform
                FROM cityjson
                WHERE {}
                """.format(schema_name, query_bbox)

    cur.execute(query_cms_info)
    cms_info = cur.fetchall()
    cms_id = []
    cms_transform = {}

    version = cms_info[0][1]
    epsg = int(cms_info[0][2]['referenceSystem'].split("::")[-1])

    for cm_info in cms_info:
        cm_epsg = int(cm_info[2]['referenceSystem'].split("::")[-1])
        cm_version = cm_info[1]

        if (cm_epsg != epsg) or (cm_version != version):
            return
        cm_id = cm_info[0]
        cms_id.append(cm_id)
        cm_transform = cm_info[3]
        cms_transform[cm_id] = cm_transform

    if len(cms_id) == 1:
        cms_id = " ('{}') ".format(cms_id[0])
    else:
        cms_id = " {} ".format(tuple(cms_id))

    query_features = """
        SET search_path to {}, public;

        SELECT cityjson_id,feature
        FROM cityobject 
        WHERE cityjson_id in {} and type In {} and {}
        """.format(schema_name, cms_id, TOPLEVEL, query_bbox)

    cm = CityJSON()
    cm.j['type'] = 'CityJSON'
    cm.j['version'] = version
    cm.set_epsg(epsg)
    # -- put transform
    cm.j["transform"] = {}

    p_to = pyproj.CRS("epsg:" + str(epsg))
    p_from = pyproj.CRS("epsg:4326")
    transformer = pyproj.Transformer.from_crs(p_from, p_to, always_xy=True)
    min_xy = transformer.transform(bbox[0], bbox[1])
    max_xy = transformer.transform(bbox[2], bbox[3])

    ss = 0.001

    new_transform = {
        "scale": [ss, ss, ss],
        "translate": [*min_xy, 0]
    }
    cm.j["transform"] = new_transform

    cm.j["metadata"]["geographicalExtent"] = [*min_xy, 0, *max_xy, 10]
    cj_info = cm.j

    if not is_stream:
        # prepare new transform
        cur.execute(query_features)
        features = cur.fetchall()
        for f in features:
            cm_id = f[0]
            feature = f[1]
            transform = cms_transform[cm_id]
            vertices = np.array(feature["vertices"])
            feature["vertices"] = (vertices * transform["scale"] + transform["translate"]).tolist()

            add_feature(cm, feature)
        cm.remove_duplicate_vertices()
        cm.update_bbox()

        # compress
        _diff = (np.array(cm.j["vertices"]) - np.array(new_transform["translate"])).flatten()
        flat_vertices = np.array([int(("%.3f" % x).replace('.', '')) for x in _diff])
        cm.j['vertices'] = np.reshape(flat_vertices, (-1, 3)).tolist()

        threaded_postgreSQL_pool.putconn(conn)
        return cm.j

    def generator():
        try:

            del cj_info['CityObjects']
            del cj_info['vertices']

            yield cj_info
            cur.execute(query_features)
            yield from __dumps_cols(cur, cms_transform, new_transform)

        finally:
            threaded_postgreSQL_pool.putconn(conn)

    return generator()


def __dumps_cols(cur, cms_transform, new_transform):
    while True:
        rows = cur.fetchmany()
        if not rows:
            break

        for row in rows:
            cm_id = row[0]
            feature = row[1]
            transform = cms_transform[cm_id]
            vertices = np.array(feature["vertices"])
            feature["vertices"] = (vertices * transform["scale"] + transform["translate"])  # .tolist()

            _diff = (feature["vertices"] - np.array(new_transform["translate"])).flatten()
            flat_vertices = np.array([int(("%.3f" % x).replace('.', '')) for x in _diff])
            feature['vertices'] = np.reshape(flat_vertices, (-1, 3)).tolist()

            yield feature


import psycopg2
from psycopg2 import pool

threaded_postgreSQL_pool = None
# try:
threaded_postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool \
    (5, 1000, **params_dic)
# threaded_postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool(5, 100, os.environ['PSYCOPG2_POSTGRESQL_URI'])

if (threaded_postgreSQL_pool):
    # query_col_info(dataset_name='3-20-DELFSHAVEN')
    # query_items(dataset_name='3-20-DELFSHAVEN')
    # for i in filter_cols_bbox(bbox=[4.24625,51.98412,4.45377,52.12673], is_stream=True):
    #     print(i)
    print("Connection pool created successfully using ThreadedConnectionPool")
# except:
#     print("Connection pool creation failed")
