import json
import os
from timeit import default_timer as timer

from cjio import cityjson
from flask import Flask, render_template, request, Response
from pgsql.query_PostgreSQL import query_collections, query_items, query_feature

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
    re = request.args.get('f', None)
    if re == 'html' or re is None:
        return render_template("collections.html", datasets=jindex['collections'])
    elif re == 'json':
        return json.dumps(jindex)  # todo?
    else:
        return JINVALIDFORMAT


@app.route('/collections/<dataset>/', methods=['GET'])  # -- html/json
def collection(dataset):
    re = request.args.get('f', None)
    if re == 'html' or re is None:
        for each in jindex['collections']:
            if each['name'] == dataset:
                return render_template("collection.html", dataset=each)
        return JINVALIDFORMAT
    elif re == 'json':
        return open('./datasets/' + dataset + '.json').read()
    else:
        return JINVALIDFORMAT


@app.route('/collections/<dataset>/items/', methods=['GET'])  # -- html/json/bbox/limit/offset
def items(dataset):
    s=timer()
    # # -- bbox
    # re_bbox = request.args.get('bbox', None)  # TODO : only 2D bbox? I'd say yes, but should be discussed...
    # if re_bbox is not None:
    #     r = re_bbox.split(',')
    #     if len(r) != 4:
    #         return JINVALIDFORMAT
    #     try:
    #         b = list(map(float, r))
    #     except:
    #         return JINVALIDFORMAT
    #     cm = cm.get_subset_bbox(bbox=b, exclude=False)

    re_limit = int(request.args.get('limit', default=10))
    re_offset = int(request.args.get('offset', default=0))
    cm = query_items(file_name=dataset, limit=re_limit, offset=re_offset)

    # -- html/json
    re_f = request.args.get('f', None)
    if re_f == 'html' or re_f is None:
        e=timer()
        print('query 10 items from '+ dataset + ': '+ str(e-s))

        return render_template("items.html", datasetname=dataset, jcm=cm.j, limit=re_limit, offset=re_offset)
    elif re_f == 'json':
        return json.dumps(cm.j)
    else:
        return JINVALIDFORMAT


@app.route('/collections/<dataset>/items/<featureID>/', methods=['GET'])  # -- html/json
def item(dataset, featureID):
    re = request.args.get('f', None)
    if re == 'html' or re is None:
        f = query_feature(file_name=dataset, feature_id=featureID).j
        if 'metadata' in f:
            del f['metadata']
        if 'version' in f:
            del f['version']
        if 'extensions' in f:
            del f['extensions']
        f['type'] = 'CityJSONFeature'
        f['id'] = featureID
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

@app.route('/collections/<dataset>/stream/')
def collection_stream(dataset):
    # -- fetch the dataset, invalid if not found
    cm = getcm(dataset)
    if cm == None:
        return JINVALIDCOLLECTION

    # line-delimited JSON generator
    def generate():
        for featureID in cm.j["CityObjects"]:
            f = cm.get_subset_ids([featureID], exclude=False).j
            if 'metadata' in f:
                del f['metadata']
            if 'version' in f:
                del f['version']
            if 'extensions' in f:
                del f['extensions']
            f["type"] = "CityJSONFeature"
            f["id"] = featureID
            s = json.dumps(f)
            s += "\n"
            yield s

    return Response(generate(), mimetype='application/json-seq')
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
