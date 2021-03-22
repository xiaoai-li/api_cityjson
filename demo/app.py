import json
import os

from cjio import cityjson
from flask import Flask, render_template, request, Response, stream_with_context

from pgsql.query_PostgreSQL import query_collections, query_items, query_feature, query_col, query_cols_bbox, \
    filter_col, query_col_transform

app = Flask(__name__)

# jindex = json.loads(open('./datasets/index.json').read())
collections_info = query_collections('addcolumns')
jindex = {'collections': collections_info}
PATHDATASETS = './datasets/'

# -- errors
JINVALIDFORMAT = {"code": "InvalidParameterValue", "description": "Invalid format"}
JINVALIDCOLLECTION = {"code": "InvalidParameterValue", "description": "Invalid feature collection"}
JINVALIDIDENTIFIER = {"code": "NotFound", "description": "identifier not found"}


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
    bboxes = query_cols_bbox()
    re = request.args.get('f', None)
    if re == 'html' or re is None:
        return render_template("collections.html", datasets=jindex['collections'], bounds=bboxes, type=0)
    elif re == 'json':
        return json.dumps(jindex)  # todo?
    else:
        return JINVALIDFORMAT


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

    # -- attr
    re_attrs = request.args.get('attrs', None)
    transform_int, transform_norm = query_col_transform(file_name=dataset)
    print(re_attrs)
    if re_bbox is not None:
        r = re_bbox.split(',')

        if len(r) != 4:
            return JINVALIDFORMAT
        try:
            re_bbox = list(map(float, r))
            transform_int, transform_norm = query_col_transform(file_name=dataset)
            generator = stream_with_context(filter_col(file_name=dataset, bbox=re_bbox, epsg=re_epsg))
            return Response(
                stream_template('col_filtered.html', rows=generator, datasetname=dataset,
                                transform_int=transform_int, transform_norm=transform_norm))

        except:
            return JINVALIDFORMAT
    elif re_attrs is not None:
        re_attrs = json.loads(re_attrs)
        generator = stream_with_context(filter_col(file_name=dataset, attrs=re_attrs))
        return Response(
            stream_template('col_filtered.html', rows=generator, datasetname=dataset, transform_int=transform_int,
                            transform_norm=transform_norm))

    else:
        bbox_wgs84, bbox_original, epsg, meta_attr = query_col(dataset)
        re = request.args.get('f', None)
        if re == 'html' or re is None:
            collections = query_collections()
            print(meta_attr)
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


#
# @app.route('/collections/<dataset>/visualise/', methods=['GET'])
# def visualise(dataset):
#     for each in jindex['collections']:
#         if each['id'] == dataset:
#             return render_template("visualise.html", stream=dataset)
#     return JINVALIDFORMAT

#
# @app.route('/stream/', methods=['GET'])
# def stream():
#     dataset = request.args.get('dataset', None)
#     f = open(PATHDATASETS + dataset + ".json", "r")
#     cj = json.loads(f.read())
#
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
#
#     f.close()
#     return app.response_class(generate(), mimetype='application/json')


#


if __name__ == '__main__':
    app.debug = True
    app.run()
