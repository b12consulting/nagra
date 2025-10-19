{% macro q(name) -%}[{{ name }}]{%- endmacro %}
DROP TABLE IF EXISTS {{ q(name) }};
