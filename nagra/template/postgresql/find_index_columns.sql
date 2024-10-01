SELECT pg_get_indexdef(attrelid, attnum, true)
 FROM   pg_attribute
 WHERE  attrelid = '{{name}}'::regclass
