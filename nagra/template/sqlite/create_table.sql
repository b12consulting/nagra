CREATE TABLE  "{{table.name}}" (
  "{{table.primary_key}}"  {{pk_type or "INTEGER"}} PRIMARY KEY
  {%- if fk_table %}
   CONSTRAINT fk_{{fk_table.name}} REFERENCES "{{fk_table.name}}"("{{fk_table.primary_key}}") {{- " ON DELETE CASCADE" if not_null else "" }}
  {%- endif %}
);
