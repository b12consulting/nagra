SELECT table_name, column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_schema = '{{pg_schema}}'
ORDER BY
 table_name,
 ordinal_position;
