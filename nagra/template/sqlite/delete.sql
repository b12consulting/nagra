DELETE FROM "{{table}}"
{% if conditions -%}
 WHERE
 {{ conditions | join(' AND ') }}
{%- endif %}
