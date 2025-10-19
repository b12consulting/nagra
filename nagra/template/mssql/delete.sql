{% macro q(name) -%}[{{ name }}]{%- endmacro %}
DELETE FROM {{ q(table) }}
{% if conditions -%}
 WHERE
 {{ conditions | join(' AND ') }}
{%- endif %}
;
