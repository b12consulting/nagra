SELECT i.name
FROM sys.indexes AS i
JOIN sys.tables AS t
  ON t.object_id = i.object_id
JOIN sys.schemas AS s
  ON s.schema_id = t.schema_id
WHERE i.name IS NOT NULL
;
