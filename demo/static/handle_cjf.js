//=== Functions for visualisation
// Adapted from https://github.com/tudelft3d/CityJSON-viewer/blob/master/js/viewer.js

var boolDrag = false
//Camera variables
var scene
var camera
var renderer
var raycaster
var mouse
var controls
var spot_light

// JSON variables
var featureDict = {} //contains the features
var meshes = [] //contains the meshes of the objects
var geoms = {} //contains the geometries of the objects

// If streaming CityJSONFeatures:
// Bbox necessary for vertex normalisation (to place them around the origin, threejs coordinate system)
// Used for normalising coordinates, function is called after bbox has been streamed
// https://stackoverflow.com/questions/3862096/2d-coordinate-normalization
var bbox = [];
var diag = 0;

function calculateDiag() {
    diag = Math.sqrt(
        (bbox[3] - bbox[0]) * (bbox[3] - bbox[0]) +
        (bbox[4] - bbox[1]) * (bbox[4] - bbox[1]) +
        (bbox[5] - bbox[2]) * (bbox[5] - bbox[2]));
}

//normal
var norm = null;


function initDocument() {
    $("#dragger").mousedown(function () {
        boolDrag = true;
    });

    $(document).mouseup(function () {
        boolDrag = false
    });

    $(document).mousemove(function (event) {
        if (boolDrag == false) {
            return
        }
    });

    $(window).resize(function () {
        v = document.getElementById("viewer")
        camera.aspect = $("#viewer").width() / $("#viewer").height();
        camera.updateProjectionMatrix();
        renderer.setSize($("#viewer").width(), $("#viewer").height());
    });
}

//called at document load and create the viewer functions
function initViewer(norm_input) {
    scene = new THREE.Scene();
    camera = new THREE.PerspectiveCamera(
        60, // Field of view
        window.innerWidth / window.innerHeight, // Aspect ratio
        0.001, // Near clipping pane
        10000 // Far clipping pane
    );
    norm = norm_input
    console.log(norm)

    // Focus camera on middle of dataset
    camera.position.set(0, 0,2);
    //camera.lookAt(0,0,0);

    //renderer for three.js
    renderer = new THREE.WebGLRenderer({
        antialias: true
    });
    console.log(renderer, renderer.domElement)
    document.getElementById("viewer").appendChild(renderer.domElement);
    renderer.setSize($("#viewer").width(), $("#viewer").height());
    renderer.setClearColor(0xFFFFFF);
    renderer.shadowMap.enabled = true;
    renderer.shadowMap.type = THREE.PCFSoftShadowMap;

    // add raycaster and mouse (for clickable objects)
    raycaster = new THREE.Raycaster()
    mouse = new THREE.Vector2();

    //add AmbientLight (light that is only there that there's a minimum of light and you can see color)
    //kind of the natural daylight
    var am_light = new THREE.AmbientLight(0xFFFFFF, 0.7); // soft white light
    scene.add(am_light);

    // Add directional light
    var spot_light = new THREE.SpotLight(0xDDDDDD);
    spot_light.position.set(84616, -1, 447422);
    spot_light.target = scene;
    spot_light.castShadow = true;
    spot_light.intensity = 0.4
    spot_light.position.normalize()
    scene.add(spot_light);

    //var axesHelper = new THREE.AxesHelper(5);
    //scene.add(axesHelper);

    // render & orbit controls
    controls = new THREE.OrbitControls(camera, renderer.domElement);
    controls.addEventListener('change', function () {
        renderer.render(scene, camera);
    });

    controls.target.set(0, 0, 0);
    //enable movement parallel to ground
    controls.screenSpacePanning = true;

    //render before loading so that window is not black
    renderer.render(scene, camera);
}

async function handleNewFeature(feature) {
    var featureName = feature["id"];
    //add json to the dict
    featureDict[featureName] = feature;

    //load the CityObjects into the viewer
    await loadCityObjects(featureName)

    renderer.render(scene, camera);
}

//convert CityObjects to mesh and add them to the viewer
async function loadCityObjects(featureName) {

    var json = featureDict[featureName]

    // Normalise coordinates
    var normGeom = new THREE.Geometry()
    if (norm) {
        var scale=norm.scale;
        var translate=norm.translate
         for (var i = 0; i < json.vertices.length; i++) {
            json.vertices[i][0] =(json.vertices[i][0]-translate[0])*scale;
            json.vertices[i][1] =(json.vertices[i][1]-translate[1])*scale;
            json.vertices[i][2] =(json.vertices[i][2]-translate[2])*scale;
        }

    } else {
        for (var i = 0; i < json.vertices.length; i++) {
            var point = new THREE.Vector3(json.vertices[i][0], json.vertices[i][1], json.vertices[i][2]
            );
            normGeom.vertices.push(point)
        }
        normGeom.normalize()

        for (var i = 0; i < json.vertices.length; i++) {
            json.vertices[i][0] = normGeom.vertices[i].x;
            json.vertices[i][1] = normGeom.vertices[i].y;
            json.vertices[i][2] = normGeom.vertices[i].z;
        }

    }

    //count number of objects
    var totalco = Object.keys(json.CityObjects).length;
    console.log("Total # City Objects: ", totalco);

    //create dictionary
    var children = {}

    //iterate through all cityObjects
    for (var cityObj in json.CityObjects) {
        //console.log(cityObj);

        try {
            //parse cityObj that it can be displayed in three js
            await parseObject(cityObj, featureName)

        } catch (e) {
            console.log("ERROR at creating: " + cityObj);
            continue
        }

        //set color of object
        var coType = json.CityObjects[cityObj].type;
        var material = new THREE.MeshLambertMaterial();
        material.color.setHex(ALLCOLOURS[coType]);

        //create mesh
        //geoms[cityObj].normalize()
        var _id = featureName + "_" + cityObj
        var coMesh = new THREE.Mesh(geoms[_id], material)
        coMesh.name = cityObj;
        coMesh.featureName = featureName
        coMesh.castShadow = true;
        coMesh.receiveShadow = true;
        scene.add(coMesh);
        meshes.push(coMesh);
    }
}

//convert json file to viewer-object
async function parseObject(cityObj, featureName) {

    var json = featureDict[featureName]

    if (json.CityObjects[cityObj].children != undefined) {

        return (json.CityObjects[cityObj].children)
    }
    ;

    //create geometry and empty list for the vertices
    var geom = new THREE.Geometry()

    //each geometrytype must be handled different
    var geomType = json.CityObjects[cityObj].geometry[0].type
    if (geomType == "Solid") {
        boundaries = json.CityObjects[cityObj].geometry[0].boundaries[0];
    } else if (geomType == "MultiSurface" || geomType == "CompositeSurface") {
        boundaries = json.CityObjects[cityObj].geometry[0].boundaries;
    } else if (geomType == "MultiSolid" || geomType == "CompositeSolid") {
        boundaries = json.CityObjects[cityObj].geometry[0].boundaries;
    }


    //needed for assocation of global and local vertices
    var verticeId = 0

    var vertices = [] //local vertices
    var indices = [] //global vertices
    var boundary = [];

    //contains the boundary but with the right verticeId
    for (var i = 0; i < boundaries.length; i++) {

        for (var j = 0; j < boundaries[i][0].length; j++) {

            //the original index from the json file
            var index = boundaries[i][0][j];

            //if this index is already there
            if (vertices.includes(index)) {

                var vertPos = vertices.indexOf(index)
                indices.push(vertPos)
                boundary.push(vertPos)

            } else {

                //add vertice to geometry
                var point = new THREE.Vector3(
                    json.vertices[index][0],
                    json.vertices[index][1],
                    json.vertices[index][2]
                );
                geom.vertices.push(point)

                vertices.push(index)
                indices.push(verticeId)
                boundary.push(verticeId)

                verticeId = verticeId + 1
            }

        }

        //create face
        //triangulated faces
        if (boundary.length == 3) {
            geom.faces.push(
                new THREE.Face3(boundary[0], boundary[1], boundary[2])
            )

            //non triangulated faces
        } else if (boundary.length > 3) {

            //create list of points
            var pList = []
            for (var j = 0; j < boundary.length; j++) {
                pList.push({
                    x: json.vertices[vertices[boundary[j]]][0],
                    y: json.vertices[vertices[boundary[j]]][1],
                    z: json.vertices[vertices[boundary[j]]][2]
                })
            }
            //get normal of these points
            var normal = await get_normal_newell(pList)

            //convert to 2d (for triangulation)
            var pv = []
            for (var j = 0; j < pList.length; j++) {
                var re = await to_2d(pList[j], normal)
                pv.push(re.x)
                pv.push(re.y)
            }

            //triangulate
            var tr = await earcut(pv, null, 2);

            //create faces based on triangulation
            for (var j = 0; j < tr.length; j += 3) {
                geom.faces.push(
                    new THREE.Face3(
                        boundary[tr[j]],
                        boundary[tr[j + 1]],
                        boundary[tr[j + 2]]
                    )
                )
            }
        }
        //reset boundaries
        boundary = []
    }

    //needed for shadow
    geom.computeFaceNormals();

    //add geom to the list
    var _id = featureName + "_" + cityObj
    geoms[_id] = geom

    return ("")
}