import json
import os
import sys

import numpy as np
import pyproj

sys.path.append('../')
from config import connect, PATHDATASETS, DEFAULT_SCHEMA


def from_index_to_vertex(boundaries, verticesList):
    new_boundries = boundaries[:]
    # needed for every geometry
    for i, temp_list in enumerate(boundaries):
        if not isinstance(temp_list, list):
            index = temp_list
            # always append the vertex to the verticesList
            x, y, z = verticesList[index]
            # the index is the length of the verticesList minus one.
            # In this way, the vertex does not have to be found
            new_boundries[i] = [x, y, z]
        else:
            new_boundries[i] = from_index_to_vertex(temp_list, verticesList)
    return new_boundries


def process_geometry(j, cityobject):
    # -- update vertex indices
    oldnewids = {}
    newvertices = []

    for geom in cityobject['geometry']:
        update_array_indices(geom["boundaries"], oldnewids, j["vertices"], newvertices, -1)
    return newvertices


def update_array_indices(a, dOldNewIDs, oldarray, newarray, slicearray):
    for i, each in enumerate(a):
        if isinstance(each, list):
            update_array_indices(each, dOldNewIDs, oldarray, newarray, slicearray)
        elif each is not None:
            if (slicearray == -1) or (slicearray == 0 and i == 0) or (slicearray == 1 and i > 0):
                if each in dOldNewIDs:
                    a[i] = dOldNewIDs[each]
                else:
                    a[i] = len(newarray)
                    dOldNewIDs[each] = len(newarray)
                    newarray.append(oldarray[each])


def insert_cityjson(file_name, schema_name):
    conn = connect()
    cur = conn.cursor()

    # open CityJSON file
    p = PATHDATASETS + file_name + '.json'
    if not os.path.isfile(p):
        print("There is no such file")
        return None
    data = json.load(open(p))

    count_cityobjects = len(data['CityObjects'])
    # store cityjson table
    metadata = {'type': data['type'], 'version': data['version']}
    if 'metadata' in data.keys():
        for key in data['metadata']:
            metadata[key] = data['metadata'][key]

    ref_system = None  # If no SRID is specified the unknown spatial reference system (SRID 0) is used.
    data_title = None

    if 'metadata' in data.keys():
        if 'referenceSystem' in data['metadata']:
            ref_system = int(data['metadata']['referenceSystem'].split("::")[-1])
        if 'datasetTitle' in data['metadata']:
            data_title = data['metadata']['datasetTitle']

    # transform back
    if 'transform' not in data.keys():
        pts_xyz = np.array(data['vertices']).T.min(axis=1)
        _diff = (data["vertices"] - pts_xyz).flatten()
        flat_vertices = np.array([int(("%.3f" % x).replace('.', '')) for x in _diff])
        data['vertices'] = np.reshape(flat_vertices, (-1, 3)).tolist()

        # -- put transform
        data["transform"] = {}
        ss = '0.'
        ss += '0' * 2
        ss += '1'
        ss = float(ss)
        data["transform"]["scale"] = [ss, ss, ss]
        data["transform"]["translate"] = [pts_xyz[0], pts_xyz[1], pts_xyz[2]]

    # get the norm mattrix
    scale = data["transform"]["scale"]
    translate = data["transform"]["translate"]

    # get real bbox
    separate_vertices = []
    bbox2ds = []
    parent_ids = []  # records which objs are multipart buildings
    obj_ids = list(data['CityObjects'].keys())
    for obj_id in data['CityObjects']:
        cityobject = data['CityObjects'][obj_id]
        vertices = process_geometry(data, cityobject)
        separate_vertices.append(vertices)
        if len(vertices) == 0:
            bbox2d = [0, 0, 0, 0]
            parent_ids.append(obj_id)
        else:
            vertices = (np.array(vertices) * scale + translate)
            x, y, z = zip(*vertices)
            bbox2d = [min(x), min(y), max(x), max(y)]
        bbox2ds.append(bbox2d)
    bbox2ds = np.array(bbox2ds)
    # prepare tiles
    pts_xy = np.array(np.array(data["vertices"]) * scale + translate).T[:2]
    min_xy = pts_xy.min(axis=1)
    max_xy = pts_xy.max(axis=1)


    # update parents of multi-part buildings
    for parent_id in parent_ids:
        cityobject = data['CityObjects'][parent_id]
        children_indices = [obj_ids.index(child_id) for child_id in cityobject['children']]
        children_bbox2d = bbox2ds[children_indices].T
        parent_bbox2d = [children_bbox2d[0, :].min(), children_bbox2d[1, :].min(), children_bbox2d[2, :].max(),
                         children_bbox2d[3, :].max()]
        parent_index = obj_ids.index(parent_id)
        bbox2ds[parent_index] = parent_bbox2d

    p_to = pyproj.CRS("epsg:4326")
    p_from = pyproj.CRS("epsg:" + str(ref_system))
    transformer = pyproj.Transformer.from_crs(p_from, p_to, always_xy=True)
    min_xy_4236 = transformer.transform(*min_xy)
    max_xy_4236 = transformer.transform(*max_xy)


    if ref_system:
        insert_metadata = """
        SET search_path TO {}, public; 
        INSERT INTO {}.cityjson 
        (name, referenceSystem, bbox, datasetTitle, metadata,transform) 
        VALUES ( %s, %s, '(({}, {}), ({}, {}))', %s, %s,%s)""" \
            .format(schema_name, schema_name, *min_xy_4236,*max_xy_4236)
        cur.execute(insert_metadata,
                    (file_name, ref_system, data_title, json.dumps(data['metadata']),
                     json.dumps(data['transform'])))

    conn.commit()

    # store city_object table
    step_index = range(int(count_cityobjects * 2 / 10), count_cityobjects, int(count_cityobjects / 10))
    insert_order = np.argsort(obj_ids)

    # prepare metadata for attribute filtering
    meta_attr = {"type": []}

    for step_id, obj_index in enumerate(insert_order):
        obj_id = obj_ids[obj_index]
        if step_id in step_index:
            finished_rate100 = int(step_id / count_cityobjects * 10) * 10
            print('The insertion has be finished ', finished_rate100, '%.')
        cityobject = data['CityObjects'][obj_id]
        vertices = separate_vertices[obj_index]

        parents = None
        if 'parents' in cityobject.keys():
            parents = cityobject['parents']
        children = None
        if 'children' in cityobject.keys():
            children = cityobject['children']

        attributes = {}
        if 'attributes' in cityobject.keys():
            attributes = cityobject['attributes']

        for key in attributes:
            if attributes[key] != "" and attributes[key] is not None:
                if key in meta_attr.keys():
                    meta_attr[key].append(attributes[key])
                else:
                    meta_attr[key] = [attributes[key]]

        meta_attr['type'].append(cityobject['type'])
        bbox2d = bbox2ds[obj_index]
        min_xy_4236 = transformer.transform(bbox2d[0], bbox2d[1])
        max_xy_4236 = transformer.transform(bbox2d[2], bbox2d[3])

        if ref_system:
            insert_cityobjects = """
            INSERT INTO cityobject (obj_id,type,bbox, parents, children, attributes, vertices, object, cityjson_id)
            VALUES (%s, %s,'(({}, {}), ({}, {}))',%s, %s, %s, %s,%s, currval('cityjson_id_seq'))
            """.format(*min_xy_4236,*max_xy_4236)
            cur.execute(insert_cityobjects,
                        (obj_id, cityobject['type'], json.dumps(parents), json.dumps(children), json.dumps(attributes),
                         json.dumps(vertices),
                         json.dumps(cityobject)))

        conn.commit()

    add_index_attr = ""

    for key in list(meta_attr):
        if key != "type":
            add_index_attr += "CREATE INDEX ON city_object((attributes->>'{}'));".format(key)
        if isinstance(meta_attr[key][0], (int, float)):
            try:
                meta_attr[key] = [min(meta_attr[key]), max(meta_attr[key])]
            except:
                meta_attr[key] = []
                print(key)
        else:
            value = list(set(meta_attr[key]))
            if len(value) > 25:
                meta_attr[key] = value[:25]
            else:
                meta_attr[key] = value
    update_meta_attr = """
        UPDATE cityjson SET meta_attr = %s
        WHERE id= currval('cityjson_id_seq') """  # .format(add_index_attr)
    cur.execute(update_meta_attr, [json.dumps(meta_attr)])
    conn.commit()

    print("""The insertion of "{}" in schema "{}" is done""".format(file_name, schema_name))


insert_cityjson('Zurich_Building_LoD2_V10', DEFAULT_SCHEMA)
# insert_cityjson('37en1', DEFAULT_SCHEMA)
insert_cityjson('37en2', DEFAULT_SCHEMA)
insert_cityjson('37ez1', DEFAULT_SCHEMA)
insert_cityjson('37ez2', DEFAULT_SCHEMA)
insert_cityjson('30dz2', DEFAULT_SCHEMA)
insert_cityjson('30gz1', DEFAULT_SCHEMA)
insert_cityjson('30gz2', DEFAULT_SCHEMA)
insert_cityjson('30hz1', DEFAULT_SCHEMA)
insert_cityjson('37fn1', DEFAULT_SCHEMA)
insert_cityjson('37fz1', DEFAULT_SCHEMA)
insert_cityjson('37hn1', DEFAULT_SCHEMA)
insert_cityjson('37gn2', DEFAULT_SCHEMA)
insert_cityjson('37gn1', DEFAULT_SCHEMA)
insert_cityjson('37dn2', DEFAULT_SCHEMA)
insert_cityjson('37bz2', DEFAULT_SCHEMA)
insert_cityjson('37bn2', DEFAULT_SCHEMA)
insert_cityjson('37cz2', DEFAULT_SCHEMA)
insert_cityjson('37gz1', DEFAULT_SCHEMA)
insert_cityjson('37gz2', DEFAULT_SCHEMA)
insert_cityjson('37hz1', DEFAULT_SCHEMA)
insert_cityjson('37hz2', DEFAULT_SCHEMA)
insert_cityjson('37hn2', DEFAULT_SCHEMA)
insert_cityjson('37fz2', DEFAULT_SCHEMA)
insert_cityjson('30hz2', DEFAULT_SCHEMA)
insert_cityjson('30hn1', DEFAULT_SCHEMA)
insert_cityjson('30gn2', DEFAULT_SCHEMA)
insert_cityjson('30gn1', DEFAULT_SCHEMA)
insert_cityjson('30dn2', DEFAULT_SCHEMA)
insert_cityjson('37bn1', DEFAULT_SCHEMA)
insert_cityjson('38cz1', DEFAULT_SCHEMA)
insert_cityjson('38cn1', DEFAULT_SCHEMA)
insert_cityjson('38az1', DEFAULT_SCHEMA)
insert_cityjson('31cz1', DEFAULT_SCHEMA)
insert_cityjson('31cn1', DEFAULT_SCHEMA)
insert_cityjson('31cn2', DEFAULT_SCHEMA)
insert_cityjson('31cz2', DEFAULT_SCHEMA)
insert_cityjson('38an2', DEFAULT_SCHEMA)
insert_cityjson('38cz2', DEFAULT_SCHEMA)
insert_cityjson('25an2', DEFAULT_SCHEMA)
insert_cityjson('25bn1', DEFAULT_SCHEMA)
insert_cityjson('25bn2', DEFAULT_SCHEMA)
insert_cityjson('25ez1', DEFAULT_SCHEMA)
insert_cityjson('25bz2', DEFAULT_SCHEMA)
insert_cityjson('25bz1', DEFAULT_SCHEMA)
insert_cityjson('25az2', DEFAULT_SCHEMA)
insert_cityjson('25az1', DEFAULT_SCHEMA)
insert_cityjson('25cn1', DEFAULT_SCHEMA)
insert_cityjson('25dn1', DEFAULT_SCHEMA)
insert_cityjson('25gn1', DEFAULT_SCHEMA)
insert_cityjson('25gn2', DEFAULT_SCHEMA)
insert_cityjson('25hz1', DEFAULT_SCHEMA)
insert_cityjson('25gz2', DEFAULT_SCHEMA)
insert_cityjson('25cz2', DEFAULT_SCHEMA)
insert_cityjson('25cz1', DEFAULT_SCHEMA)
insert_cityjson('24hz2', DEFAULT_SCHEMA)
insert_cityjson('30en2', DEFAULT_SCHEMA)
insert_cityjson('30fn2', DEFAULT_SCHEMA)
insert_cityjson('25an1', DEFAULT_SCHEMA)
# insert_cityjson('30gz1_01_2019_volledig', DEFAULT_SCHEMA)
# insert_cityjson('30gz1_02_2019_volledig', DEFAULT_SCHEMA)
# insert_cityjson('30gz1_03_2019_volledig', DEFAULT_SCHEMA)
# insert_cityjson('30gz1_04_2019_volledig', DEFAULT_SCHEMA)
# insert_cityjson('30gz2_01_2019_volledig', DEFAULT_SCHEMA)
# insert_cityjson('30gz2_02_2019_volledig', DEFAULT_SCHEMA)
# insert_cityjson('30gz2_03_2019_volledig', DEFAULT_SCHEMA)
# insert_cityjson('30gz2_04_2019_volledig', DEFAULT_SCHEMA)
insert_cityjson('37en1_01_2019_volledig', DEFAULT_SCHEMA)
insert_cityjson('37en1_02_2019_volledig', DEFAULT_SCHEMA)
insert_cityjson('37en1_03_2019_volledig', DEFAULT_SCHEMA)
insert_cityjson('37en1_04_2019_volledig', DEFAULT_SCHEMA)
# insert_cityjson('37fz2_01_2019_volledig', DEFAULT_SCHEMA)
# insert_cityjson('37fz2_02_2019_volledig', DEFAULT_SCHEMA)
# insert_cityjson('37fz2_03_2019_volledig', DEFAULT_SCHEMA)
# insert_cityjson('37fz2_04_2019_volledig', DEFAULT_SCHEMA)

