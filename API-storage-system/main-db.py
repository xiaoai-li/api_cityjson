import json
import os
import zlib
from cjio import cityjson
from flask import Flask, render_template, request, Response, stream_with_context

from pgsql.query_PostgreSQL import query_items, query_item, query_col_info, query_cols_info, filter_col_bbox, \
    filter_col_attr, filter_cols_bbox

app = Flask(__name__)
PATHDATASETS = './datasets/'

# -- errors
JINVALIDFORMAT = {"code": "InvalidParameterValue", "description": "Invalid format"}
JINVALIDCOLLECTION = {"code": "InvalidParameterValue", "description": "Invalid feature collection"}
JINVALIDIDENTIFIER = {"code": "NotFound", "description": "identifier not found"}


def stream_template(template_name, **context):
    app.update_template_context(context)
    t = app.jinja_env.get_template(template_name)
    rv = t.stream(context)
    rv.enable_buffering(5)
    return rv


@app.route('/', methods=['GET'])
def root():
    re = request.args.get('f', None)
    if re == 'html' or re is None:
        return render_template("root.html")
    elif re == 'json':
        content = {
            "links": [
                {
                    "rel": "self",
                    "type": "application/json",
                    "title": "This document as JSON",
                    "href": "http://127.0.0.1:5000/?f=json"
                },
                {
                    "rel": "data",
                    "type": "application/json",
                    "title": "Collections",
                    "href": "http://127.0.0.1:5000/collections"
                }
            ],
            "title": "CityJSON RESTful access demo",
            "description": "It's still work-in-progress! The structure of this demo is roughly copied from pygeoapi demo"
        }
        return Response(json.dumps(content), mimetype='application/json')
    else:
        return JINVALIDFORMAT


@app.route('/collections/', methods=['GET'])  # -- html/json
def collections():
    collections_info = query_cols_info()
    re_bbox = request.args.get('bbox', None)
    re_epsg = request.args.get('epsg', None)
    re = request.args.get('f', None)
    if re == 'html' or re is None:
        if re_bbox and re_epsg:
            re_bbox = re_bbox.split(',')
            if len(re_bbox) != 4:
                return JINVALIDFORMAT
            generator = stream_with_context(filter_cols_bbox(bbox=re_bbox, epsg=re_epsg,is_stream=True))
            return Response(stream_template('filtered_results.html', rows=generator, datasetname='filtered results'))
        else:
            return render_template("collections.html", datasets=collections_info)
    elif re == 'json':
        if re_bbox:
            re_bbox = re_bbox.split(',')
            if len(re_bbox) != 4:
                return JINVALIDFORMAT
            items_filtered = filter_cols_bbox(bbox=re_bbox, epsg=re_epsg,is_stream=False)
            return Response(json.dumps(items_filtered), mimetype='application/json')
        else:
            return Response(json.dumps(collections_info), mimetype='application/json')
    else:
        return JINVALIDFORMAT


@app.route('/collections/<dataset>/', methods=['GET'])  # -- html/json
def collection(dataset):
    collection_info = query_col_info(dataset)
    re = request.args.get('f', None)
    if re == 'html' or re is None:
        return render_template("collection.html", dataset=collection_info)
    elif re == 'json':
        return Response(json.dumps(collection_info), mimetype='application/json')
    else:
        return JINVALIDFORMAT


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
        items_filtered = filter_col_bbox(dataset_name=dataset, bbox=re_bbox, epsg=re_epsg, is_stream=False)
        return Response(items_filtered, mimetype='application/json')
    elif re_attrs:
        items_filtered = filter_col_attr(dataset_name=dataset, attrs=re_attrs, is_stream=False)
        return Response(items_filtered, mimetype='application/json')
    elif re_limit!=9999999:
        items_queried = query_items(dataset_name=dataset, limit=re_limit, offset=re_offset)
        return Response(json.dumps(items_queried), mimetype='application/json')
    else:
        all_items = json.dumps(query_items(dataset_name=dataset))  # get the whole dataset

        # compressed = zlib.compress(all_items.encode())

        return Response(all_items, mimetype='application/json')
        # return Response(json.dumps(all_items), mimetype='application/json')



@app.route('/collections/<dataset>/items/<featureID>/', methods=['GET'])  # -- html/json
def item(dataset, featureID):
    feature = query_item(dataset_name=dataset, feature_id=featureID)

    return Response(json.dumps(feature), mimetype='application/json')



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
