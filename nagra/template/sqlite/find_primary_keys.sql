SELECT
  m.tbl_name AS table_name,
  ti.name AS pk_col
FROM
  sqlite_master AS m,
  pragma_table_info(m.name) as ti
WHERE
  m.type = 'table'
  AND ti.pk = 1
;
