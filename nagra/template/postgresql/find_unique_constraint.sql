
select
 tbl.relname as table_name,
 idx.relname as index_name
from pg_index pgi
  join pg_class idx on idx.oid = pgi.indexrelid
  join pg_namespace insp on insp.oid = idx.relnamespace
  join pg_class tbl on tbl.oid = pgi.indrelid
  join pg_namespace tnsp on tnsp.oid = tbl.relnamespace
where pgi.indisunique
  and not pgi.indisprimary
  and tnsp.nspname = '{{pg_schema}}'
