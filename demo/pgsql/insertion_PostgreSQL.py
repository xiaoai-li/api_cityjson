import copy
import json
import math
import os
import sys

import numpy as np
import psycopg2
from hilbertcurve.hilbertcurve import HilbertCurve
from scipy.spatial import cKDTree

DEFAULT_DB = 'cityjson'
DEFAULT_SCHEMA = 'addcolumns'

PATHDATASETS = '../datasets/'

param_dic = {
    "host": "localhost",
    "database": DEFAULT_DB,
    "user": "postgres",
    "password": "1234"
}


def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn


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


def convert_surface_to_polygon(surface, referenceSystem):
    linestring_index = 0
    linestring_list = []
    for linestring in surface:
        vertex_list = []
        first_vertex = "{0} {1} {2}".format(linestring[0][0], linestring[0][1], linestring[0][2])
        for vertex in linestring:
            vertex = "{0} {1} {2}".format(vertex[0], vertex[1], vertex[2])
            vertex_list.append(vertex)
        vertex_list.append(first_vertex)
        linestring_list.append(tuple(vertex_list))
        linestring_index = linestring_index + 1

    if len(linestring_list) == 1:
        polygon = '(' + str(tuple(vertex_list)) + ')'
    else:
        polygon = str(tuple(linestring_list))
    polygon = polygon.replace("'", "")
    polygon = polygon.replace('"', "")
    if referenceSystem is None:
        polygon = 'POLYGONZ {}'.format(polygon)
    else:
        polygon = 'SRID={}; POLYGONZ {}'.format(referenceSystem, polygon)
    return polygon


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


def create_hilbert(num_x, num_y):
    grid_x = range(0, num_x)
    grid_y = range(0, num_y)
    grid_xy = []
    for x in grid_x:
        for y in grid_y:
            grid_xy.append([x, y])

    hilbert_curve = HilbertCurve(20, 2)
    hilbert_index = hilbert_curve.distances_from_points(grid_xy)
    return np.array(hilbert_index)


def insert_cityjson(file_name, schema_name):
    conn = connect(param_dic)
    cur = conn.cursor()

    # open CityJSON file
    p = PATHDATASETS + file_name + '.json'
    if not os.path.isfile(p):
        print("There is no such file")
        return None
    data = json.load(open(p))

    count_cityobjects = len(data['CityObjects'])

    # store metadata table
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
    pts_xyz_max = np.array(data['vertices']).T.max(axis=1)
    center = pts_xyz_max / 2
    s = 1.0 / center.max()

    transform_norm = {"scale": s, "translate": center.tolist()}
    scale = data["transform"]["scale"]
    translate = data["transform"]["translate"]
    # get real bbox
    separate_vertices = []
    bbox2ds = []
    centroids = []
    parent_ids = []  # records which objs are multipart buildings
    obj_ids = list(data['CityObjects'].keys())
    for obj_id in data['CityObjects']:
        cityobject = data['CityObjects'][obj_id]
        vertices = process_geometry(data, cityobject)
        separate_vertices.append(vertices)
        if len(vertices) == 0:
            bbox2d = [0, 0, 0, 0]
            centroid = [0, 0]  # update later mainly for the tree query
            parent_ids.append(obj_id)
        else:
            vertices = (np.array(vertices) * scale + translate)
            x, y, z = zip(*vertices)
            bbox2d = [min(x), min(y), max(x), max(y)]
            centroid = [(bbox2d[0] + bbox2d[2]) / 2, (bbox2d[1] + bbox2d[3]) / 2]
        bbox2ds.append(bbox2d)
        centroids.append(centroid)
    bbox2ds = np.array(bbox2ds)
    centroids = np.array(centroids)
    # prepare tiles
    pts_xy = np.array(np.array(data["vertices"]) * scale + translate).T[:2]
    min_xy = pts_xy.min(axis=1)
    max_xy = pts_xy.max(axis=1)

    # make sure proportional
    num_cityobjects = len(data['CityObjects'])
    r = math.log10(num_cityobjects)  # todo: imprpve the proportion
    rate = 5 ** (r - 1)

    len_x = max_xy[0] - min_xy[0]
    len_y = max_xy[1] - min_xy[1]
    step_x = len_x / rate
    step_y = len_y / rate
    num_x = int(len_x / step_x) + 1
    num_y = int(len_y / step_y) + 1

    grid_x = np.arange(int(min_xy[0]), int(min_xy[0]) + rate * step_x, step_x)
    grid_y = np.arange(int(min_xy[1]), int(min_xy[1]) + rate * step_y, step_y)
    grid_xy = []
    for x in grid_x:
        for y in grid_y:
            grid_xy.append([x, y])
    grid_xy = np.array(grid_xy)
    print(len(grid_xy))
    tree = cKDTree(grid_xy, leafsize=20)
    hilbert_indices = create_hilbert(num_x, num_y)
    _, ids = tree.query(centroids, k=1)
    tile_ids = np.array(hilbert_indices[ids])

    # update parents of multi-part buildings
    for parent_id in parent_ids:
        cityobject = data['CityObjects'][parent_id]
        children_indices = [obj_ids.index(child_id) for child_id in cityobject['children']]
        children_bbox2d = bbox2ds[children_indices].T
        parent_bbox2d = [children_bbox2d[0, :].min(), children_bbox2d[1, :].min(), children_bbox2d[2, :].max(),
                         children_bbox2d[3, :].max()]
        parent_index = obj_ids.index(parent_id)
        bbox2ds[parent_index] = parent_bbox2d
        children_tile_ids = tile_ids[children_indices]
        parent_tile_id = int(np.median(children_tile_ids))
        tile_ids[parent_index] = parent_tile_id
        tile_ids[children_indices] = parent_tile_id

    print('tiles are indexed')

    if ref_system:
        insert_metadata = """
        SET search_path TO {}, public; 
        INSERT INTO {}.metadata 
        (name, version, referenceSystem, bbox, datasetTitle, object,transform_int,transform_norm) 
        VALUES ( %s, %s, %s, ST_MakeEnvelope({}, {}, {}, {}, {}), %s, %s,%s,%s)""" \
            .format(schema_name, schema_name, min_xy[0], min_xy[1], max_xy[0], max_xy[1], ref_system)
        cur.execute(insert_metadata,
                    (file_name, data['version'], ref_system, data_title, json.dumps(data['metadata']),
                     json.dumps(data['transform']), json.dumps(transform_norm)))
    else:
        insert_metadata = """
        SET search_path TO {}, public; 
        INSERT INTO {}.metadata 
        (name, version, datasetTitle, object,transform_int,transform_norm) 
        VALUES ( %s, %s, %s, %s,%s,%s)""" \
            .format(schema_name, schema_name, )
        cur.execute(insert_metadata,
                    (
                        file_name, data['version'], data_title, json.dumps(data['metadata']),
                        json.dumps(data['transform']),
                        json.dumps(transform_norm)))

    conn.commit()

    # store city_object table
    step_index = range(int(count_cityobjects * 2 / 10), count_cityobjects, int(count_cityobjects / 10))
    insert_order = np.argsort(tile_ids)

    # prepare metadata for attribute filtering
    # meta_attr={"name":{"type": 'int', "enum":[]}}
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

        if ref_system:
            insert_cityobjects = """
            INSERT INTO {}.city_object (obj_id, type, bbox, parents, children, attributes, vertices, object, metadata_id)
            VALUES (%s, %s, ST_MakeEnvelope({}, {}, {}, {}, {}),%s, %s, %s, %s,%s, currval('metadata_id_seq'))
            """.format(schema_name, bbox2d[0], bbox2d[1], bbox2d[2], bbox2d[3], ref_system)
            cur.execute(insert_cityobjects,
                        (obj_id, cityobject['type'], parents, children, json.dumps(attributes),
                         json.dumps(vertices),
                         json.dumps(cityobject)))
        else:
            insert_cityobjects = """
            INSERT INTO {}.city_object (obj_id, type, parents, children, attributes, vertices, object, metadata_id)
            VALUES (%s, %s ,%s, %s, %s, %s,%s, currval('metadata_id_seq'))
            """.format(schema_name)
            cur.execute(insert_cityobjects,
                        (obj_id, cityobject['type'], parents, children, json.dumps(attributes),
                         json.dumps(vertices),
                         json.dumps(cityobject)))

        # todo: or center?
        conn.commit()

        #  geometry
        for geometry in cityobject['geometry']:
            geom_type = geometry['type']
            geom_lod = geometry['lod']

            insert_geometry = """
            INSERT INTO {}.geometries (lod, type, city_object_id) VALUES (%s, %s, currval('city_object_id_seq'))
            """.format(schema_name)
            cur.execute(insert_geometry, (geom_lod, geom_type))
            conn.commit()

            boundaries = from_index_to_vertex(geometry['boundaries'], vertices)
            if 'semantics' in geometry.keys():
                semantics_surfaces = geometry['semantics']['surfaces']
                if geom_type == 'MultiSurface' or geom_type == 'CompositeSurface':
                    for surface_index, surface in enumerate(boundaries):
                        surface_type = None
                        surface_attributes = None
                        semantics_value = geometry['semantics']['values'][surface_index]
                        if semantics_value:
                            # print(semantics_surfaces)
                            semantics_surface = semantics_surfaces[semantics_value]
                            surface_type = semantics_surface['type']
                            surface_attributes = copy.deepcopy(semantics_surface)
                            del surface_attributes['type']
                        if ref_system:
                            polygon = convert_surface_to_polygon(surface, ref_system)
                            insert_geometry = """
                            INSERT INTO {}.surfaces (type, attributes, geometry, geometries_id)
                            VALUES (%s, %s, %s, currval('geometries_id_seq'))""".format(schema_name)
                            cur.execute(insert_geometry, (surface_type, json.dumps(surface_attributes), polygon))
                        else:
                            insert_geometry = """
                            INSERT INTO {}.surfaces (type, attributes, geometries_id)
                            VALUES (%s, %s, currval('geometries_id_seq'))""".format(schema_name)
                            cur.execute(insert_geometry, (surface_type, json.dumps(surface_attributes)))

                        conn.commit()

                elif geom_type == 'Solid':

                    for shell_index, shell in enumerate(boundaries):
                        for surface_index, surface in enumerate(shell):
                            surface_type = None
                            surface_attributes = None
                            semantics_value = geometry['semantics']['values'][shell_index][surface_index]
                            if semantics_value:
                                semantics_surface = semantics_surfaces[semantics_value]
                                surface_type = semantics_surface['type']
                                del semantics_surface['type']
                                surface_attributes = semantics_surface
                            if ref_system:
                                polygon = convert_surface_to_polygon(surface, ref_system)
                                insert_geometry = """
                                INSERT INTO {}.surfaces (type, attributes, geometry, geometries_id)
                                VALUES (%s, %s, %s, currval('geometries_id_seq'))""".format(schema_name)
                                cur.execute(insert_geometry, (surface_type, json.dumps(surface_attributes), polygon))
                            else:
                                insert_geometry = """
                                INSERT INTO {}.surfaces (type, attributes, geometries_id)
                                VALUES (%s, %s, currval('geometries_id_seq'))""".format(schema_name)
                                cur.execute(insert_geometry, (surface_type, json.dumps(surface_attributes)))
                            conn.commit()

                elif geom_type == 'MultiSolid' or geom_type == 'CompositeSolid':
                    for solid_index, solid in enumerate(boundaries):
                        for shell_index, shell in enumerate(solid):
                            for surface_index, surface in enumerate(shell):
                                surface_type = None
                                surface_attributes = None
                                semantics_value = geometry['semantics']['values'][solid_index][shell_index][
                                    surface_index]
                                if semantics_value:
                                    semantics_surface = semantics_surfaces[semantics_value]
                                    surface_type = semantics_surface['type']
                                    del semantics_surface['type']
                                    surface_attributes = semantics_surface
                                if ref_system:
                                    polygon = convert_surface_to_polygon(surface, ref_system)
                                    insert_geometry = """
                                    INSERT INTO {}.surfaces (type, attributes, geometry, geometries_id)
                                    VALUES (%s, %s, %s, currval('geometries_id_seq'))""".format(schema_name)
                                    cur.execute(insert_geometry,
                                                (surface_type, json.dumps(surface_attributes), polygon))
                                else:
                                    insert_geometry = """
                                    INSERT INTO {}.surfaces (type, attributes, geometries_id)
                                    VALUES (%s, %s, currval('geometries_id_seq'))""".format(schema_name)
                                    cur.execute(insert_geometry, (surface_type, json.dumps(surface_attributes)))
                                conn.commit()
                else:
                    print('unknown geometry type')
            else:
                if geom_type == 'MultiSurface' or geom_type == 'CompositeSurface':
                    for surface_index, surface in enumerate(boundaries):
                        if ref_system:
                            polygon = convert_surface_to_polygon(surface, ref_system)
                            insert_geometry = """
                            INSERT INTO {}.surfaces (geometry, geometries_id)
                            VALUES (%s,currval('geometries_id_seq'))""".format(schema_name)
                            cur.execute(insert_geometry, (polygon,))
                        else:
                            insert_geometry = """
                            INSERT INTO {}.surfaces (geometries_id)
                            VALUES (currval('geometries_id_seq'))""".format(schema_name)
                            cur.execute(insert_geometry)
                        conn.commit()

                elif geom_type == 'Solid':
                    for shell_index, shell in enumerate(boundaries):
                        for surface_index, surface in enumerate(shell):
                            if ref_system:
                                polygon = convert_surface_to_polygon(surface, ref_system)
                                insert_geometry = """
                                INSERT INTO {}.surfaces (geometry, geometries_id)
                                VALUES (%s,currval('geometries_id_seq'))""".format(schema_name)
                                cur.execute(insert_geometry, (polygon,))
                            else:
                                insert_geometry = """
                                INSERT INTO {}.surfaces (geometries_id)
                                VALUES (currval('geometries_id_seq'))""".format(schema_name)
                                cur.execute(insert_geometry)
                            conn.commit()

                elif geom_type == 'MultiSolid' or geom_type == 'CompositeSolid':
                    for solid_index, solid in enumerate(boundaries):
                        for shell_index, shell in enumerate(solid):
                            for surface_index, surface in enumerate(shell):
                                if ref_system:
                                    polygon = convert_surface_to_polygon(surface, ref_system)
                                    insert_geometry = """
                                    INSERT INTO {}.surfaces (geometry, geometries_id)
                                    VALUES (%s,currval('geometries_id_seq'))""".format(schema_name)
                                    cur.execute(insert_geometry, (polygon,))
                                else:
                                    insert_geometry = """
                                    INSERT INTO {}.surfaces (geometries_id)
                                    VALUES (currval('geometries_id_seq'))""".format(schema_name)
                                    cur.execute(insert_geometry)
                                conn.commit()

                else:
                    print('unknown geometry type')
    add_index_attr = ""

    for key in meta_attr:
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
                meta_attr[key] = ["", value[0]]
            else:
                meta_attr[key] = value
    update_meta_attr = """
        {}
        UPDATE metadata SET meta_attr = %s 
        WHERE id= currval('metadata_id_seq') """.format(add_index_attr)
    cur.execute(update_meta_attr, [json.dumps(meta_attr)])
    conn.commit()

    print("""The insertion of "{}" in schema "{}" is done""".format(file_name, schema_name))


#
#
# insert_cityjson('3-20-DELFSHAVEN', DEFAULT_SCHEMA)
# insert_cityjson('denhaag', DEFAULT_SCHEMA)
# insert_cityjson('delft', DEFAULT_SCHEMA)
# insert_cityjson('vienna', DEFAULT_SCHEMA)
# insert_cityjson('montreal', DEFAULT_SCHEMA)
# insert_cityjson('DA13_3D_Buildings_Merged', DEFAULT_SCHEMA)
# insert_cityjson('Zurich_Building_LoD2_V10', DEFAULT_SCHEMA)

# insert_cityjson('37en1', DEFAULT_SCHEMA)
# insert_cityjson('37en2', DEFAULT_SCHEMA)
# insert_cityjson('37ez1', DEFAULT_SCHEMA)
# insert_cityjson('37ez2', DEFAULT_SCHEMA)

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