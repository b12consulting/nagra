CREATE TABLE  "{{table.name}}" (
  {% for name, typedef, fk_table, default in natural_key  -%}
  "{{name}}"  {{typedef}} NOT NULL

    {%- if default %}
     DEFAULT {{default}}
    {%- endif %}

    {%- if name in fk_tables %}
    {% set fk_table = fk_tables[name] %}
     CONSTRAINT fk_{{name}} REFERENCES "{{fk_table.name}}"("{{fk_table.primary_key}}") {{- " ON DELETE CASCADE" if not_null else "" }}
    {%- endif %}

   {{", " if not loop.last}}
  {% endfor %}
);
