import json

from flask import Flask, request, Response
import zlib
from query_FileSystem import query_items, filter_col, query_item

app = Flask(__name__)

# -- errors
JINVALIDFORMAT = {"code": "InvalidParameterValue", "description": "Invalid format"}
JINVALIDCOLLECTION = {"code": "InvalidParameterValue", "description": "Invalid feature collection"}
JINVALIDIDENTIFIER = {"code": "NotFound", "description": "identifier not found"}


@app.route('/', methods=['GET'])
def root():
    return "Hello CityJSON REST API"


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
        import time
        # t1 = time.time()
        # data = query_items(file_name=dataset)
        # t2 =time.time()
        # print("Time for reading the file:   ",t2-t1)
        #
        # compressed = zlib.compress(data.encode())
        # t3 =time.time()
        # print("Time for compressing the file:   ",t3-t2)
        p = '../data_pdok' + '/compressed/' + dataset
        file = open(p, "rb")
        byte = file.read()
        return byte

        # return Response(all_items, mimetype='application/json')


@app.route('/collections/<dataset>/items/<featureID>/', methods=['GET'])  # -- html/json
def item(dataset, featureID):
    item_queried = query_item(file_name=dataset, featureID=featureID)
    return Response(item_queried, mimetype='application/json')


@app.errorhandler(404)
def not_found(error):
    return JINVALIDIDENTIFIER, 404


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, threaded=True)
