SELECT tc.table_name, kc.column_name
FROM information_schema.table_constraints tc
 JOIN information_schema.key_column_usage kc ON (
  kc.table_name = tc.table_name
  AND kc.table_schema = tc.table_schema
  AND kc.constraint_name = tc.constraint_name
)
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND kc.ordinal_position is not null
  AND tc.table_schema = '{{pg_schema}}'
