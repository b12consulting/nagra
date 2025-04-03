SELECT
 m.name as table_name,
 ti.name as column_name,
 ti.type as column_type
FROM
  sqlite_master AS m,
  pragma_table_info(m.name) AS ti
WHERE
  m.type in ('table', 'view')
ORDER BY
 table_name,
  ti.cid
