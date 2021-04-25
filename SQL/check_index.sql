SELECT
    tablename,
    indexname,
    indexdef
FROM
    pg_indexes
WHERE
    schemaname = 'first'
ORDER BY
    tablename,
    indexname;


	drop index first.cityobject_expr_idx
	        CREATE INDEX ON first.cityobject((attributes->>'type'));
