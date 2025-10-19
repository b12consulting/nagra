{% macro q(name) -%}[{{ name }}]{%- endmacro %}
CREATE TABLE {{ q(table.name) }} (
  {{ q(table.primary_key) }} {{ pk_type or "BIGINT IDENTITY(1,1)" }} PRIMARY KEY
  {%- if fk_table %}
  , CONSTRAINT fk_{{ fk_table.name }} FOREIGN KEY ({{ q(table.primary_key) }})
    REFERENCES {{ q(fk_table.name) }} ({{ q(fk_table.primary_key) }})
  {%- endif %}
);
