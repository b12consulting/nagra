SELECT table_name, column_name, data_type, character_maximum_length
FROM information_schema.columns
ORDER BY
 table_name,
 ordinal_position;
