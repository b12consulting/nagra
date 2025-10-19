SELECT
 t.name AS table_name,
 i.name AS index_name
FROM sys.indexes AS i
JOIN sys.tables AS t
  ON t.object_id = i.object_id
JOIN sys.schemas AS s
  ON s.schema_id = t.schema_id
WHERE i.is_unique = 1
  AND i.is_primary_key = 0
  AND s.name = '{{pg_schema}}';
