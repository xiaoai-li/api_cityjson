import json
import os
from time import sleep

from cjio import cityjson
from flask import Flask, render_template, request, Response, stream_with_context, render_template_string
from pgsql.query_PostgreSQL import query_collections, query_items, query_feature, query_col_bbox, query_cols_bbox, \
    filter_col_bbox, filter_cols_bbox

app = Flask(__name__)

# jindex = json.loads(open('./datasets/index.json').read())
collections_info = query_collections('addcolumns')
jindex = {'collections': collections_info}
PATHDATASETS = './datasets/'

# -- errors
JINVALIDFORMAT = {"code": "InvalidParameterValue", "description": "Invalid format"}
JINVALIDCOLLECTION = {"code": "InvalidParameterValue", "description": "Invalid feature collection"}
JINVALIDIDENTIFIER = {"code": "NotFound", "description": "identifier not found"}


# @pytest.yield_fixture
# def client(app):
#     """
#     Overriding the `client` fixture from pytest_flask to fix this bug:
#     https://github.com/pytest-dev/pytest-flask/issues/42
#     """
#     with app.test_client() as client:
#         yield client
#
#     while True:
#         top = flask._request_ctx_stack.top
#         if top is not None and top.preserved:
#             top.pop()
#         else:
#             break


# @app.route("/stream")
# def stream():
#     @stream_with_context
#     def generate():
#         yield render_template_string('<link rel=stylesheet href="{{ url_for("static", filename="stream.css") }}">')
#
#         for i in range(500):
#             yield render_template_string("<p>{{ i }}: {{ s }}</p>\n", i=i, s=i*2)
#             sleep(1)
#
#     return app.response_class(generate())


@app.route('/', methods=['GET'])
def root():
    re = request.args.get('f', None)
    if re == 'html' or re is None:
        return render_template("root.html")
    elif re == 'json':
        return render_template("todo.html")
    else:
        return JINVALIDFORMAT


@app.route('/collections/', methods=['GET'])  # -- html/json
def collections():
    # -- bbox
    # re_bbox = request.args.get('bbox', None)  # TODO : only 2D bbox? I'd say yes, but should be discussed...
    #
    # if re_bbox is not None:
    #     r = re_bbox.split(',')
    #     if len(r) != 4:
    #         return JINVALIDFORMAT
    #     try:
    #         re_bbox = list(map(float, r))
    #         generator = stream_with_context(filter_cols_bbox(bbox=re_bbox))
    #         dataset = "global_filtered"
    #         return Response(stream_template('cols_filtered.html', rows=generator, datasetname=dataset))
    #     except:
    #         return JINVALIDFORMAT
    # else:
    bboxes = query_cols_bbox()
    re = request.args.get('f', None)
    if re == 'html' or re is None:
        return render_template("collections.html", datasets=jindex['collections'], bounds=bboxes, type=0)
    elif re == 'json':
        return json.dumps(jindex)  # todo?
    else:
        return JINVALIDFORMAT


# @app.route('/collections/<dataset>/', methods=['GET'])  # -- html/json
# def collection(dataset):
#     # -- bbox
#     re_bbox = request.args.get('bbox', None)  # TODO : only 2D bbox? I'd say yes, but should be discussed...
#     re_epsg = request.args.get('epsg', None)
#
#     if re_bbox is not None:
#         r = re_bbox.split(',')
#         if len(r) != 4:
#             return JINVALIDFORMAT
#         try:
#             re_bbox = list(map(float, r))
#             filter_dataset = filter_col_bbox(file_name=dataset, bbox=re_bbox, epsg=re_epsg)
#             bbox_wgs84, bbox_original, epsg = query_col_bbox(filter_dataset)
#             ds = {"name": filter_dataset, "title": ''}
#             return render_template("col_filtered.html", dataset=ds, bounds=json.dumps(bbox_wgs84),
#                                    crs=epsg, bounds_original=bbox_original, type=1)
#         except:
#             return JINVALIDFORMAT
#     else:
#         bbox_wgs84, bbox_original, epsg = query_col_bbox(dataset)
#         re = request.args.get('f', None)
#         if re == 'html' or re is None:
#             collections = query_collections()
#             for each in collections:
#                 if each['name'] == dataset:
#                     return render_template("collection.html", dataset=each, bounds=json.dumps(bbox_wgs84),
#                                            crs=epsg, bounds_original=bbox_original, type=1)
#             return JINVALIDFORMAT
#         elif re == 'json':
#             p = PATHDATASETS + dataset + '.json'
#             if not os.path.isfile(p):
#                 return None
#             f = open(p)
#             cm = cityjson.reader(file=f, ignore_duplicate_keys=True)
#             return cm.j
#         else:
#             return JINVALIDFORMAT


def stream_template(template_name, **context):
    app.update_template_context(context)
    t = app.jinja_env.get_template(template_name)
    rv = t.stream(context)
    rv.enable_buffering(5)
    return rv


@app.route('/collections/<dataset>/', methods=['GET'])  # -- html/json
def collection(dataset):
    # -- bbox
    re_bbox = request.args.get('bbox', None)  # TODO : only 2D bbox? I'd say yes, but should be discussed...
    re_epsg = request.args.get('epsg', None)

    if re_bbox is not None:
        r = re_bbox.split(',')
        if len(r) != 4:
            return JINVALIDFORMAT
        try:
            re_bbox = list(map(float, r))
            if dataset == '_global':
                generator = stream_with_context(filter_cols_bbox(bbox=re_bbox))
                dataset = "global_filtered"
                return Response(stream_template('cols_filtered.html', rows=generator, datasetname=dataset))
            else:
                generator = stream_with_context(filter_col_bbox(file_name=dataset, bbox=re_bbox, epsg=re_epsg))
                return Response(stream_template('col_filtered.html', rows=generator, datasetname=dataset))

        except:
            return JINVALIDFORMAT
    else:
        bbox_wgs84, bbox_original, epsg, meta_attr = query_col_bbox(dataset)
        re = request.args.get('f', None)
        if re == 'html' or re is None:
            collections = query_collections()
            for each in collections:
                if each['name'] == dataset:
                    return render_template("collection.html", dataset=each, bounds=json.dumps(bbox_wgs84),
                                           crs=epsg, bounds_original=bbox_original, type=1, meta_attr=meta_attr)
            return JINVALIDFORMAT
        elif re == 'json':
            p = PATHDATASETS + dataset + '.json'
            if not os.path.isfile(p):
                return None
            f = open(p)
            cm = cityjson.reader(file=f, ignore_duplicate_keys=True)
            return cm.j
        else:
            return JINVALIDFORMAT


@app.route('/collections/<dataset>/items/', methods=['GET'])  # -- html/json/bbox/limit/offset
def items(dataset):
    re_limit = int(request.args.get('limit', default=10))
    re_offset = int(request.args.get('offset', default=0))
    cm = query_items(file_name=dataset, limit=re_limit, offset=re_offset)

    # -- html/json
    re_f = request.args.get('f', None)
    if re_f == 'html' or re_f is None:
        return render_template("items.html", datasetname=dataset, jcm=cm.j, limit=re_limit, offset=re_offset)
    elif re_f == 'json':
        return json.dumps(cm.j)
    else:
        return JINVALIDFORMAT


@app.route('/collections/<dataset>/items/<featureID>/', methods=['GET'])  # -- html/json
def item(dataset, featureID):
    re = request.args.get('f', None)

    if re == 'html' or re is None:
        f = query_feature(file_name=dataset, feature_id=featureID)
        return render_template("item.html", jitem=f, datasetname=dataset)
    elif re == 'json':
        f = query_feature(file_name=dataset, feature_id=featureID).j
        if 'metadata' in f:
            del f['metadata']
        if 'version' in f:
            del f['version']
        if 'extensions' in f:
            del f['extensions']
        f['type'] = 'CityJSONFeature'
        f['id'] = featureID
        return json.dumps(f)
    else:
        return JINVALIDFORMAT


@app.errorhandler(404)
def not_found(error):
    return render_template('404.html'), 404


def getcm(filename):
    p = PATHDATASETS + filename + '.json'
    if os.path.isfile(p) == False:
        return None
    f = open(p)
    return cityjson.reader(file=f, ignore_duplicate_keys=True)


@app.route('/collections/<dataset>/visualise/', methods=['GET'])
def visualise(dataset):
    for each in jindex['collections']:
        if each['id'] == dataset:
            return render_template("visualise.html", stream=dataset)
    return JINVALIDFORMAT

# @app.route('/stream/', methods=['GET'])
# def stream():
#     dataset = request.args.get('dataset', None)
#     f = open(PATHDATASETS + dataset + ".json", "r")
#     cj = json.loads(f.read())

#     # line-delimited JSON generator
#     def generate():
#         if cj['type'] == "CityJSONCollection":
#             for k, v in cj.items():
#                 if k == "features":
#                     for feature in cj[k]:
#                         feature = str(feature)
#                         yield '{}\n'.format(feature)
#                 else:
#                     yield '{}\n'.format({k: v})
#         elif cj['type'] == "CityJSON":
#             cm = str(cj)
#             yield '{}\n'.format(cm)
#     f.close()
#     return app.response_class(generate(), mimetype='application/json')

# @app.route('/collections/<dataset>/stream/')
# def collection_stream(dataset):
#     # -- fetch the dataset, invalid if not found
#     cm = getcm(dataset)
#     if cm == None:
#         return JINVALIDCOLLECTION
#
#     # line-delimited JSON generator
#     def generate():
#         for featureID in cm.j["CityObjects"]:
#             f = cm.get_subset_ids([featureID], exclude=False).j
#             if 'metadata' in f:
#                 del f['metadata']
#             if 'version' in f:
#                 del f['version']
#             if 'extensions' in f:
#                 del f['extensions']
#             f["type"] = "CityJSONFeature"
#             f["id"] = featureID
#             s = json.dumps(f)
#             s += "\n"
#             yield s
#
#     return Response(generate(), mimetype='application/json-seq')

# return Response(generate(), mimetype='text/plain')

# @app.route('/<filename>/download/')
# def cmd_download(filename):
#     cm = getcm(filename)
#     if cm == None:
#         return render_template("wrongdataset.html")
#     else:
#         return cm.j


# @app.route('/<filename>/')
# def cmd_info(filename):
#     cm = getcm(filename)
#     if cm == None:
#         return render_template("wrongdataset.html")
#     else:
#         i = cm.get_info()
#         return json.loads(i)


# @app.route('/<filename>/subset/random/<number>/')
# def cmd_subset_random(filename, number):
#     cm = getcm(filename)
#     if cm == None:
#         return render_template("wrongdataset.html")
#     else:
#         cm2 = cm.get_subset_random(number=int(number), exclude=False)
#         return cm2.j


# @app.route('/<filename>/subset/bbox/<minx>/<miny>/<maxx>/<maxy>/')
# def cmd_subset_bbox(filename, minx, miny, maxx, maxy):
#     cm = getcm(filename)
#     if cm == None:
#         return render_template("wrongdataset.html")
#     else:
#         cm2 = cm.get_subset_bbox(bbox=mybbox, exclude=False)
#         return cm2.j


# @app.route('/<filename>/subset/cotype/<thecotype>/')
# def cmd_subset_cotype(filename, thecotype):
#     cm = getcm(filename)
#     if cm == None:
#         return render_template("wrongdataset.html")
#     else:
#         cm2 = cm.get_subset_cotype(cotype=thecotype, exclude=False)
#         return cm2.j

# #
if __name__ == '__main__':
    app.debug = True
    app.run()
