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
    re_limit = request.args.get('limit', 9999999)
    re_offset = request.args.get('offset', 0)
    re_bbox = request.args.get('bbox', None)
    re_epsg = request.args.get('epsg', None)
    re_attrs = request.args.get('attrs', None)

    if re_bbox is not None:
        re_bbox = re_bbox.split(',')
        if len(re_bbox) != 4:
            return JINVALIDFORMAT

    if re_attrs is not None:
        re_attrs = json.loads(re_attrs)

    if re_bbox and re_epsg:
        items_filtered = filter_col(file_name=dataset, bbox=re_bbox, epsg=re_epsg)
        return Response(items_filtered, mimetype='application/json')
    elif re_attrs:
        items_filtered = filter_col(file_name=dataset, attrs=re_attrs)
        return Response(items_filtered, mimetype='application/json')
    elif re_limit != 9999999:
        items_queried = query_items(file_name=dataset, limit=re_limit, offset=re_offset)
        return Response(json.dumps(items_queried), mimetype='application/json')
    else:
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

        # return Response(compressed, mimetype='application/json')



@app.route('/collections/<dataset>/items/<featureID>/', methods=['GET'])  # -- html/json
def item(dataset, featureID):
    item_queried = query_item(file_name=dataset, featureID=featureID)
    return Response(item_queried, mimetype='application/json')


@app.errorhandler(404)
def not_found(error):
    return JINVALIDIDENTIFIER, 404


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, threaded=True)
