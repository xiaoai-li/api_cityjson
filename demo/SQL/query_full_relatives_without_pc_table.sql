SET search_path to addcolumns, public;

-- original query
WITH origin AS (
SELECT obj_id, c.metadata_id, c.object, vertices,version,parents,children
FROM city_object AS c JOIN metadata AS m ON c.metadata_id=m.id
WHERE name='denhaag'
ORDER BY tile_id
LIMIT 20 OFFSET 1),

-- get parents of original query
parents AS(
SELECT obj_id, object,vertices,children
FROM city_object
WHERE obj_id IN (SELECT unnest(parents) FROM origin)),

-- get children of original query
children AS(
SELECT obj_id, object,vertices
FROM city_object
WHERE obj_id IN (SELECT unnest(children) FROM origin)),

-- get siblings of original query
siblings AS(
SELECT obj_id, object,vertices
FROM city_object
WHERE obj_id IN (SELECT unnest(children) FROM parents))

SELECT obj_id, object,vertices FROM origin
UNION
SELECT obj_id, object,vertices FROM parents
UNION SELECT * FROM children
UNION SELECT * FROM siblings