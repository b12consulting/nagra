CREATE TABLE [{{ table.name }}] (
  {% for name, typedef, fk_table, default in natural_key -%}
  [{{ name }}] {{ typedef }} NOT NULL
  {%- if default %}
   DEFAULT {{ default }}
  {%- endif %}
  {%- if fk_tables.get(name) %}
   CONSTRAINT fk_{{ name }} FOREIGN KEY ([{{ name }}])
     REFERENCES [{{ fk_tables[name].name }}] ([{{ fk_tables[name].primary_key }}])
  {%- endif %}
  {{ "," if not loop.last }}
  {% endfor %}
);
