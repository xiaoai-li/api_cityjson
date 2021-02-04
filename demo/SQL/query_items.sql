
(select obj_id
from addcolumns.city_object as c join addcolumns.metadata as m on c.metadata_id=m.id
where name='Zurich_Building_LoD2_V10' and type='Building'
limit 30 offset 0)
UNION
select obj_id
from addcolumns.city_object
where obj_id in (select children_flattened
from (addcolumns.city_object as c join addcolumns.metadata as m on c.metadata_id=m.id),unnest(children) as children_flattened
where name='Zurich_Building_LoD2_V10' and type='Building'
limit 30 offset 0);