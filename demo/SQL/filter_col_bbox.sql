SET search_path to addcolumns, public;
SELECT st_transform(c.bbox, 4326)
FROM city_object AS c JOIN metadata AS m ON c.metadata_id=m.id
WHERE name='denhaag' 