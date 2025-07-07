SELECT
  'fk_' || fk."from" AS fk_name,
  m.tbl_name AS table_name,
  fk."from",
  fk."table",
  fk."to"
FROM
  sqlite_master AS m,
  pragma_foreign_key_list(m.name) AS fk
;
