{% macro q(name) -%}[{{ name }}]{%- endmacro %}
DROP VIEW IF EXISTS {{ q(name) }};
