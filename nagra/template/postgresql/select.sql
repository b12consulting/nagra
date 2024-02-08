SELECT
  {{ columns | join(', ') }}
FROM "{{table}}"
{%- if joins is defined -%}
{%- for next_table, alias, prev_table, col in joins %}
LEFT JOIN "{{next_table}}" as {{alias}} ON ({{alias}}.id = "{{prev_table}}"."{{col}}")
{%- endfor -%}

{%- endif %}
{% if conditions -%}
 WHERE
 {{ conditions | join(' AND ') }}
{%- endif %}
{% if groupby -%}
 GROUP BY
 {{ groupby | join(', ') }}
{%- endif %}
{% if orderby -%}
 ORDER BY
 {{ orderby | join(', ') }}
{%- endif %}
{% if limit -%}
 LIMIT {{ limit }}
{%- endif %}
{% if offset -%}
 OFFSET {{ offset }}
{%- endif %}
;
