SELECT meta.id,parent_id,avg(tile_id) ::numeric::integer as value
FROM (parents_children as pc JOIN metadata as meta on pc.metadata_id=meta.id), city_object as obj
WHERE pc.child_id=obj.obj_id
GROUP BY meta.id,pc.parent_id