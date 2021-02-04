select srid from 


SELECT ST_AsText(ST_Envelope('LINESTRING(0 0, 1 3)'::geometry));
 SELECT Find_SRID('test', 'surfaces', 'geometry');


SELECT id,object ->>'type'
FROM test.city_object
WHERE city_object.metadata_id='metadata_delft' AND
ST_Intersects(convexhull,
ST_Envelope('SRID=7415;LINESTRING(84616.468 447422.999,85000.839 447750.636)'::geometry));

select *
from test.city_object 
where object ->>'type'='BuildingPart'

select parents_id
from (SELECT id
FROM test.city_object
WHERE city_object.metadata_id='metadata_delft' AND
ST_Intersects(convexhull,
ST_Envelope('SRID=7415;LINESTRING(84616.468 447422.999,85000.839 447750.636)'::geometry))) as A,test.parents_children
where A.id=children_id
					

SELECT city_object.id, city_object.object, city_object.attributes
FROM {}.city_object
WHERE object  ->> 'type' in {} 
AND city_object.metadata_id = LOWER(%s)
LIMIT {} OFFSET {}""" 


SELECT city_object.id, city_object.object, city_object.attributes
FROM test.city_object
WHERE city_object.metadata_id='metadata_delft' AND
ST_Intersects(convexhull,
ST_Envelope('SRID=7415;LINESTRING(84616.468 447422.999,85000.839 447750.636)'::geometry))
LIMIT 20 OFFSET 0
