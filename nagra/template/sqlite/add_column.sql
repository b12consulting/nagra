ALTER TABLE {{table}}
 ADD COLUMN {{column}} {{col_def}} {{- " NOT NULL" if not_null else "" }}
{%- if fk_table %}
 CONSTRAINT fk_{{column}} REFERENCES "{{fk_table}}"(id);
{%- endif %}

