SELECT table_name, column_name
FROM information_schema.columns
WHERE table_schema = '{{pg_schema}}'
ORDER BY table_name
