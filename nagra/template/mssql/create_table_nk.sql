{% macro q(name) -%}[{{ name }}]{%- endmacro %}
CREATE TABLE {{ q(table.name) }} (
  {% for name, typedef, fk_table, default in natural_key -%}
  {{ q(name) }} {{ typedef }} NOT NULL
  {%- if default %}
   DEFAULT {{ default }}
  {%- endif %}
  {%- if fk_tables.get(name) %}
   CONSTRAINT fk_{{ name }} FOREIGN KEY ({{ q(name) }})
     REFERENCES {{ q(fk_tables[name].name) }} ({{ q(fk_tables[name].primary_key) }})
  {%- endif %}
  {{ "," if not loop.last }}
  {% endfor %}
);
