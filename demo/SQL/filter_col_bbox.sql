SELECT city_object.id, city_object.object, city_object.attributes
FROM {}.city_object
WHERE city_object.metadata_id=LOWER(%s) AND
ST_Intersects(convexhull,
ST_Envelope('SRID={};LINESTRING({} {},{} {})'::geometry))
LIMIT {} OFFSET {}
SET search_path to addcolumns, public;

(SELECT c.id
FROM city_object AS c JOIN metadata AS m ON c.metadata_id=m.id
WHERE name='denhaag'AND ST_Intersects(c.bbox,
ST_Envelope('SRID=4326;LINESTRING(4.2 4.22,52 52.2)'::geometry)))
UNION
(SELECT id
FROM city_object, (SELECT cparent_flattened, version
FROM (city_object AS c JOIN metadata AS m ON c.metadata_id=m.id), unnest(children) AS children_flattened
WHERE name='denhaag' AND ST_Intersects(c.bbox,
ST_Envelope('SRID=4326;LINESTRING(4.2 4.22,52 52.2)'::geometry))) ) AS children
WHERE obj_id = children_flattened