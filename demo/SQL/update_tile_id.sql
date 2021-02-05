SET search_path TO addcolumns;

UPDATE city_object SET tile_id = tile_value.value
FROM (SELECT parent_id,avg(tile_id) ::numeric::integer as value
    FROM city_object, (SELECT children_id,obj_id as parent_id
    FROM (city_object AS c JOIN metadata AS m
    ON c.metadata_id=m.id), unnest(children) AS children_id
    WHERE name='denhaag'
    ORDER BY tile_id) AS children
    WHERE obj_id = children_id
    GROUP BY parent_id) AS tile_value
WHERE city_object.obj_id = tile_value.parent_id;


