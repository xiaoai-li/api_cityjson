# The schema 1 
The goal is to test the jsonb performance
## Storage system --> Database
## Data in database --> the 5 from the CityJSON

## DB schema
- **The 'type' stored as one member in jsonb and not indexed**
- parents/ children stored as an array 
- bounding box stored as geometry
```
CREATE TABLE cityjson (
            id serial  PRIMARY KEY,
            name text,
            referenceSystem int,
            bbox geometry(POLYGON),
            datasetTitle text,
            metadata jsonb,
            meta_attr jsonb,
            transform jsonb
        )

        CREATE TABLE cityobject (
            id serial PRIMARY KEY,
            obj_id text,
            parents text[],
            children text[],
            bbox geometry(POLYGON),
            attributes jsonb,
            vertices jsonb,
            object jsonb,
            cityjson_id int REFERENCES cityjson (id) on delete cascade on update cascade
        )
```
## Use case
Query 10 items from the 2 different cityjson files  (denhaag, zurich) (Having building parts)
## Performance indicator
