{% macro q(name) -%}[{{ name }}]{%- endmacro %}
ALTER TABLE {{ q(table) }} DROP CONSTRAINT {{ q(name) }};
