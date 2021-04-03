SET search_path to addcolumns, public;

-- original query
WITH origin AS (
SELECT obj_id, c.metadata_id, c.object, vertices,version
FROM city_object AS c JOIN metadata AS m ON c.metadata_id=m.id
WHERE name='denhaag'
ORDER BY tile_id
LIMIT 20 OFFSET 1),

-- get parents of original query
parents AS(
SELECT obj.obj_id,obj.object, obj.vertices,obj.metadata_id
FROM city_object as obj, origin, parents_children as pc
WHERE origin.obj_id=child_id AND origin.metadata_id=pc.metadata_id
AND obj.metadata_id=pc.metadata_id AND obj.obj_id=parent_id),

-- get children of original query
children AS(
SELECT obj.obj_id,obj.object, obj.vertices
FROM city_object as obj, origin, parents_children as pc
WHERE origin.obj_id=parent_id AND origin.metadata_id=pc.metadata_id
AND obj.metadata_id=pc.metadata_id AND obj.obj_id=child_id),

-- get siblings of original query
siblings AS(
SELECT obj.obj_id,obj.object, obj.vertices
FROM city_object as obj, parents, parents_children as pc
WHERE parents.obj_id=parent_id AND parents.metadata_id=pc.metadata_id
AND obj.metadata_id=pc.metadata_id AND obj.obj_id=child_id)

SELECT obj_id, object,vertices FROM origin
UNION
SELECT obj_id, object,vertices FROM parents
UNION SELECT * FROM children
UNION SELECT * FROM siblings