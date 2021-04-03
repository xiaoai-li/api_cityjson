
set search_path to addcolumns;
select pg_size_pretty(pg_relation_size('city_object'));

-- pg_total_relation_size pg_table_size
SELECT
    nspname,
    pg_size_pretty ( sum(
        pg_relation_size (C .oid))
    ) AS "total_size"
FROM
    pg_class C
LEFT JOIN pg_namespace N ON (N.oid = C .relnamespace)
WHERE
    nspname NOT IN (
        'pg_catalog',
        'information_schema','public'
    )
AND C .relkind <> 'i'
AND nspname !~ '^pg_toast'
GROUP BY nspname
ORDER BY nspname
