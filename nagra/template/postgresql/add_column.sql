ALTER TABLE "{{table}}"
 ADD COLUMN "{{column}}" {{col_def}} {{- " NOT NULL" if not_null else "" }}
{%- if default %}
 DEFAULT {{default}}
{%- endif %}
{%- if fk_table %}
 CONSTRAINT fk_{{column}} REFERENCES "{{fk_table.name}}"("{{fk_table.primary_key}}") {{- " ON DELETE CASCADE" if not_null else "" }};
{%- endif %}
