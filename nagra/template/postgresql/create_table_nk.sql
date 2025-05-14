CREATE TABLE  "{{table.name}}" (
  {% for name, typedef, fk_table, default in natural_key  -%}
  "{{name}}"  {{typedef}} NOT NULL

    {%- if default %}
     DEFAULT {{default}}
    {%- endif %}

    {%- if fk_table %}
     CONSTRAINT fk_{{name}} REFERENCES "{{fk_table}}"(id) {{- " ON DELETE CASCADE" if not_null else "" }}
    {%- endif %}

   {{", " if not loop.last}}
  {% endfor %}
);
