SELECT
  {{ columns | join(', ') }}
FROM "{{table}}"

{%- for next_table, alias, prev_table, alias_col, prev_col in joins %}
 LEFT JOIN "{{next_table}}" as {{alias}} ON (
    {{alias}}."{{alias_col}}" = "{{prev_table}}"."{{prev_col}}"
 )
{%- endfor -%}

{% if conditions -%}
 WHERE
 {{ conditions | join(' AND ') }}
{%- endif %}
{% if groupby -%}
 GROUP BY
 {{ groupby | join(', ') }}
{%- endif -%}
{% if orderby -%}
 ORDER BY
 {{ orderby | join(', ') }}
{%- endif %}
{% if limit -%}
 LIMIT {{ limit }}
{%- endif %}
{% if offset -%}
 OFFSET {{ offset }}
{%- endif -%}
;
