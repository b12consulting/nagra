{% macro q(name) -%}[{{ name }}]{%- endmacro %}
CREATE OR ALTER VIEW {{ q(name) }} AS {{ view_def }};
