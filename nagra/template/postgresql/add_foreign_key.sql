ALTER TABLE {{table}}
 ADD COLUMN {{column}} {{col_def}} {{- " NOT NULL" if required else "" }}
 CONSTRAINT fk_{{column}} REFERENCES "{{other_table}}"(id);
