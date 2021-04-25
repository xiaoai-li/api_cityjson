import copy
import os
import zlib

import ujson
from cjio import cityjson, subset
from cjio.cityjson import CityJSON
from pyproj import Proj, transform

from config import PATHDATASETS

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

MAX_LIMIT = 99999999


def getcm(filename):
    p = PATHDATASETS + filename + '.json'
    if not os.path.isfile(p):
        return None
    f = open(p)
    return cityjson.reader(file=f, ignore_duplicate_keys=True)


def is_attrs_cityobject(co, attrs):
    for attr in attrs:
        value = attrs[attr]
        if attr == "type":
            co_type = co['type']
            if co_type not in value:
                return False
        elif attr not in co["attributes"]:
            return False

        elif isinstance(value, list):
            co_value = co["attributes"][attr]
            if co_value not in value:
                return False
        else:
            co_value = co["attributes"][attr]

            if "value" in value:
                val = value["value"]
                operator = value["operator"]

                if operator == '=':
                    if val != co_value:
                        return False
                if operator == '>':
                    if val <= co_value:
                        return False
                if operator == '>=':
                    if val < co_value:
                        return False
                if operator == '<':
                    if val >= co_value:
                        return False
                if operator == '<=':
                    if val > co_value:
                        return False
            if "range" in value:
                min = value["range"][0]
                max = value["range"][1]
                if co_value < min or co_value > max:
                    return False
    return True


def get_subset_attr(cm, attrs=None, exclude=False):
    # print ('get_subset_bbox')
    # -- new sliced CityJSON object
    cm2 = CityJSON()
    cm2.j["version"] = cm.j["version"]
    cm2.path = cm.path
    if "transform" in cm.j:
        cm2.j["transform"] = cm.j["transform"]
    re = set()
    for coid in cm.j["CityObjects"]:
        if is_attrs_cityobject(cm.j["CityObjects"][coid], attrs):
            re.add(coid)
    re2 = copy.deepcopy(re)
    if exclude == True:
        allkeys = set(cm.j["CityObjects"].keys())
        re = allkeys ^ re
    # -- also add the parent-children
    for theid in re2:
        if "children" in cm.j['CityObjects'][theid]:
            for child in cm.j['CityObjects'][theid]['children']:
                re.add(child)
        if "parents" in cm.j['CityObjects'][theid]:
            for each in cm.j['CityObjects'][theid]['parents']:
                re.add(each)

    for each in re:
        cm2.j["CityObjects"][each] = cm.j["CityObjects"][each]
    # -- geometry
    subset.process_geometry(cm.j, cm2.j)
    # -- templates
    subset.process_templates(cm.j, cm2.j)
    # -- appearance
    if ("appearance" in cm.j):
        cm2.j["appearance"] = {}
        subset.process_appearance(cm.j, cm2.j)
    # -- metadata
    try:
        cm2.j["metadata"] = copy.deepcopy(cm.j["metadata"])
        cm2.update_metadata(overwrite=True, new_uuid=True)
    except:
        pass

    return cm2


def query_items(file_name=None, limit=MAX_LIMIT, offset=0):
    cm = getcm(file_name)
    if "appearcance" in cm.j:
        del cm.j["appearcance"]
    if limit == MAX_LIMIT:
        return ujson.dumps(cm.j, ensure_ascii=False,
                           escape_forward_slashes=True)
    else:
        theids = cm.get_ordered_ids_top_co(limit=limit, offset=offset)
        cm = cm.get_subset_ids(theids, exclude=False)
        return ujson.dumps(cm.j, ensure_ascii=False,
                           escape_forward_slashes=True)


def query_item(file_name=None, featureID=None):
    cm = getcm(file_name)
    f = cm.get_subset_ids([featureID], exclude=False).j
    if 'metadata' in f:
        del f['metadata']
    if 'version' in f:
        del f['version']
    if 'extensions' in f:
        del f['extensions']
    return ujson.dumps(f, ensure_ascii=False,
                       escape_forward_slashes=True)


def filter_col(file_name=None, attrs=None, bbox=None, epsg=None):
    cm = getcm(file_name)
    if "appearcance" in cm.j:
        del cm.j["appearcance"]
    cm_epsg = cm.get_epsg()

    if epsg and bbox:
        if epsg != cm_epsg:
            inProj = Proj("+init=EPSG:" + str(epsg))
            outProj = Proj("+init=EPSG:" + str(cm_epsg))
            min_xy = transform(inProj, outProj, bbox[0], bbox[1])
            max_xy = transform(inProj, outProj, bbox[2], bbox[3])
            bbox = [min_xy[0], min_xy[1], max_xy[0], max_xy[1]]
        cm = cm.get_subset_bbox(bbox=bbox, exclude=False)

    if attrs:
        cm = get_subset_attr(cm, attrs=attrs, exclude=False)
    return ujson.dumps(cm.j, ensure_ascii=False,
                       escape_forward_slashes=True)


# filename = '37en2_volledig'
# p = PATHDATASETS + filename + '.json'
# f = open(p)
# cm = cityjson.reader(file=f, ignore_duplicate_keys=True)
# attrs = {"type": ["Building", "Road", "WaterBody", "PlantCover", "Bridge", "GenericCityObject"]}
# cm2 = get_subset_attr(cm, attrs=attrs, exclude=False)

# save_path = PATHDATASETS + filename + 'noLandUse.json'
# cityjson.save(cm2, path=save_path)

# def compress_originalzlib(cmpath, comprout, cm=None):
#     if cm == None:
#         cm_file = open(cmpath)
#         cm = cityjson.reader(file=cm_file, ignore_duplicate_keys=True)
#
#     compressed = zlib.compress(ujson.dumps(cm.j).encode())
#
#     cout = open(comprout, 'wb')
#     cout.write(compressed)
#     cout.close()
#
#     print("Compression finished")
#
#
# filenames = ['Zurich_Building_LoD2_V10']
#
# for filename in filenames:
#     p_in = PATHDATASETS + filename + '.json'
#     p_out = PATHDATASETS + '/compressed/' + filename
#     compress_originalzlib(cmpath=p_in, comprout=p_out)
