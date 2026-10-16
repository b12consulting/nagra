SELECT c.name
FROM sys.index_columns AS ic
JOIN sys.columns AS c
  ON c.object_id = ic.object_id AND c.column_id = ic.column_id
JOIN sys.indexes AS i
  ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.tables AS t
  ON t.object_id = i.object_id
JOIN sys.schemas AS s
  ON s.schema_id = t.schema_id
WHERE i.name = '{{name}}'
ORDER BY ic.key_ordinal;
