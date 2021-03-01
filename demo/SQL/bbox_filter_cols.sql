SET search_path to addcolumns, public;
WITH cityjson AS (
SELECT id
FROM metadata
WHERE (st_transform(bbox, 4326)&&ST_Envelope('SRID=4326;LINESTRING(0.52734 49.38237 , 6.85547 54.36776 )'::geometry))),

origin_top AS (
SELECT obj_id as main_id, obj_id, c.object, vertices
FROM city_object AS c JOIN cityjson AS m ON c.metadata_id=m.id
WHERE metadata_id in (SELECT id FROM cityjson) AND type IN ('Building', 'Bridge', 'CityObjectGroup', 'CityFurniture', 'GenericCityObject', 'LandUse', 'PlantCover', 'Railway', 'Road', 'SolitaryVeget
ationObject', 'TINRelief', 'TransportSquare', 'Tunnel', 'WaterBody') AND
(st_transform(bbox, 4326)&&ST_Envelope('SRID=4326;LINESTRING(0.52734 49.38237 , 6.85547 54.36776 )'::geometry))),

origin_part AS (
SELECT obj_id, c.object, vertices,parents
FROM city_object AS c JOIN cityjson AS m ON c.metadata_id=m.id
WHERE metadata_id in (SELECT id FROM cityjson) AND type NOT IN ('Building', 'Bridge', 'CityObjectGroup', 'CityFurniture', 'GenericCityObject', 'LandUse', 'PlantCover', 'Railway', 'Road', 'SolitaryV
egetationObject', 'TINRelief', 'TransportSquare', 'Tunnel', 'WaterBody') AND
(st_transform(bbox, 4326)&&ST_Envelope('SRID=4326;LINESTRING(0.52734 49.38237 , 6.85547 54.36776 )'::geometry))),

-- get parents of original query_part
parents AS(
SELECT obj_id as main_id, obj_id, object,vertices,children
FROM city_object
WHERE obj_id IN (SELECT unnest(parents) FROM origin_part)),

-- get siblings of original query
siblings AS(
SELECT unnest(parents) as main_id, obj_id, object,vertices
FROM city_object
WHERE obj_id IN (SELECT unnest(children) FROM parents))


SELECT main_id, obj_id, object,vertices FROM origin_top
UNION SELECT main_id, obj_id, object,vertices FROM parents
UNION SELECT * FROM siblings
ORDER BY main_id
