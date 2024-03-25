WITH all_tables AS (SELECT name FROM sqlite_master WHERE type = 'table') 
SELECT at.name as table_name, pti.name as column_name
FROM all_tables at INNER JOIN pragma_table_info(at.name) pti
