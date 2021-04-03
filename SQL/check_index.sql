SELECT
    tablename,
    indexname,
    indexdef
FROM
    pg_indexes
WHERE
    schemaname = 'addcolumns'
ORDER BY
    tablename,
    indexname;