import json
import os

from cjio import cityjson
from flask import Flask, request, Response

from pgsql.query_PostgreSQL import query_items, query_item, query_col_info, query_cols_info, \
    filter_col

app = Flask(__name__)
PATHDATASETS = './datasets/'

# -- errors
JINVALIDFORMAT = {"code": "InvalidParameterValue", "description": "Invalid format"}
JINVALIDCOLLECTION = {"code": "InvalidParameterValue", "description": "Invalid feature collection"}
JINVALIDIDENTIFIER = {"code": "NotFound", "description": "identifier not found"}


@app.route('/', methods=['GET'])
def root():
    return "Hello CityJSON REST API"


@app.route('/collections/', methods=['GET'])  # -- html/json
def collections():
    collections_info = query_cols_info()
    return Response(collections_info, mimetype='application/json')


@app.route('/collections/<dataset>/', methods=['GET'])  # -- html/json
def collection(dataset):
    info = query_col_info(dataset)
    return Response(info, mimetype='application/json')


@app.route('/collections/<dataset>/items/', methods=['GET'])  # -- html/json/bbox/limit/offset
def items(dataset):
    re_limit = request.args.get('limit', None)
    re_offset = request.args.get('offset', None)
    re_bbox = request.args.get('bbox', None)
    re_epsg = request.args.get('epsg', None)
    re_attrs = request.args.get('attrs', None)

    if re_bbox is not None:
        re_bbox = re_bbox.split(',')
        if len(re_bbox) != 4:
            return JINVALIDFORMAT

    if re_attrs is not None:
        re_attrs = json.loads(re_attrs)

    if re_limit is not None:
        re_limit = int(re_limit)

    if re_offset is not None:
        re_offset = int(re_offset)

    if re_limit and re_offset:  # pagination
        items_queried = query_items(file_name=dataset, limit=re_limit, offset=re_offset)
        return Response(items_queried, mimetype='application/json')
    elif re_limit:
        items_queried = query_items(file_name=dataset, limit=re_limit)
        return Response(items_queried, mimetype='application/json')
    elif re_bbox or re_attrs:  # stream
        gen = filter_col(file_name=dataset, bbox=re_bbox, epsg=re_epsg, attrs=re_attrs)
        if gen:
            return Response(gen, mimetype='application/json')
        else:
            return JINVALIDIDENTIFIER, 404
    else:  # send all items
        all_items = query_items(file_name=dataset)
        return Response(all_items, mimetype='application/json')


@app.route('/collections/<dataset>/items/<featureID>/', methods=['GET'])  # -- html/json
def item(dataset, featureID):
    feature = query_item(file_name=dataset, feature_id=featureID)
    return Response(feature, mimetype='application/json')


@app.errorhandler(404)
def not_found(error):
    return JINVALIDIDENTIFIER, 404


def getcm(filename):
    p = PATHDATASETS + filename + '.json'
    if os.path.isfile(p) == False:
        return None
    f = open(p)
    return cityjson.reader(file=f, ignore_duplicate_keys=True)


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, threaded=True)
