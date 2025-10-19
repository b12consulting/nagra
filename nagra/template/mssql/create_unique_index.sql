{% macro q(name) -%}[{{ name }}]{%- endmacro %}
{%- if natural_key is defined -%}
CREATE UNIQUE INDEX {{ q(table ~ "_idx") }} ON {{ q(table) }} (
  {%- for col in natural_key -%}
  {{ q(col) }}{{ ", " if not loop.last }}
  {%- endfor %}
);
{%- endif %}
