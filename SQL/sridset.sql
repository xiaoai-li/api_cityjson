Set search_path to addcolumns, public;
Select st_asgeojson(st_transform(ST_SetSRID(bbox, 21781 ), 4326))
from metadata
where name='DA13_3D_Buildings_Merged'

SELECT st_transform(bbox, 4326)
FROM metadata
WHERE referencesystem IS NOT null