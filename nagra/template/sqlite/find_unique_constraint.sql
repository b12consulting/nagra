SELECT
  m.tbl_name AS table_name,
  il.name AS idx_name,
  ii.name AS column_name
FROM
  sqlite_master AS m,
  pragma_index_list(m.name) AS il,
  pragma_index_info(il.name) AS ii
WHERE
  m.type = 'table'
  AND il."unique"
ORDER BY table_name, idx_name
;
