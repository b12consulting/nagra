ALTER TABLE {{table}}
 ADD COLUMN {{column}} {{col_def}} {{- " NOT NULL" if not_null else "" }}
;

