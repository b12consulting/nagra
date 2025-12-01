SELECT
    fk.name AS [ForeignKey_Name],
    t_origin.name AS [Origin_Table],
    c_origin.name AS [Origin_Column],
    t_dest.name AS [Destination_Table],
    c_dest.name AS [Destination_Column]
FROM
    sys.foreign_keys AS fk
INNER JOIN
    sys.foreign_key_columns AS fkc
    ON fk.object_id = fkc.constraint_object_id
INNER JOIN
    sys.tables AS t_origin
    ON fkc.parent_object_id = t_origin.object_id
INNER JOIN
    sys.columns AS c_origin
    ON fkc.parent_object_id = c_origin.object_id
    AND fkc.parent_column_id = c_origin.column_id
INNER JOIN
    sys.tables AS t_dest
    ON fkc.referenced_object_id = t_dest.object_id
INNER JOIN
    sys.columns AS c_dest
    ON fkc.referenced_object_id = c_dest.object_id
    AND fkc.referenced_column_id = c_dest.column_id
WHERE SCHEMA_NAME(t_origin.schema_id) = '{{mssql_schema}}'
