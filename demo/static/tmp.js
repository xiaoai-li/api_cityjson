var bounds = {{ bounds }}

var map = L.map('map', {
    center: [0,0],
    zoom: 13
});

if (bounds.length==1){
    map.fitBounds(bounds);
    var rect = L.rectangle(bounds[i], {color: 'blue', weight: 3}).on('click', function (e) {
    console.info(e);
}).addTo(map);
}
else
{for (i = 0; i < bounds.length; i++) {
  var rect = L.rectangle(bounds[i], {color: 'blue', weight: 3}).on('click', function (e) {
    console.info(e);
}).addTo(map);
}
}



// Set up the OSM layer
L.tileLayer(
  'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18
  }).addTo(map);


// Initialise the FeatureGroup to store editable layers
var editableLayers = new L.FeatureGroup();
map.addLayer(editableLayers);

// define custom marker
var MyCustomMarker = L.Icon.extend({
  options: {
    shadowUrl: null,
    iconAnchor: new L.Point(12, 12),
    iconSize: new L.Point(24, 24),
    iconUrl: 'https://upload.wikimedia.org/wikipedia/commons/6/6b/Information_icon4_orange.svg'
  }
});

var drawPluginOptions = {
  position: 'topright',
  draw: {
    polyline:false,
    polygon: false,
    circle: false, // Turns off this drawing tool
    rectangle: {
      shapeOptions: {
        clickable: false
      }
    },
    marker: false
  },
  edit: {
    featureGroup: editableLayers, //REQUIRED!!
    remove: false
  }
};

// Initialise the draw control and pass it the FeatureGroup of editable layers
var drawControl = new L.Control.Draw(drawPluginOptions);
map.addControl(drawControl);

map.on('draw:created', function(e) {
  var type = e.layerType,
    layer = e.layer;

  if (type === 'marker') {
    layer.bindPopup('A popup!');
  }

  editableLayers.addLayer(layer);
});