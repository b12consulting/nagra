{% set use_offset = offset is not none %}
SELECT{% if limit and not use_offset %} TOP {{ limit }}{% endif %}
  {{ columns | join(', ') }}
FROM [{{ table }}]

{%- for next_table, alias, prev_table, alias_col, prev_col in joins %}
 LEFT JOIN [{{ next_table }}] AS [{{ alias }}] ON (
    [{{ alias }}].[{{ alias_col }}] = [{{ prev_table }}].[{{ prev_col }}]
 )
{%- endfor -%}

{% if conditions %}
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
{% elif use_offset -%}
 ORDER BY (SELECT NULL)
{%- endif %}
{% if use_offset -%}
 OFFSET {{ offset or 0 }} ROWS
 {% if limit %}
 FETCH NEXT {{ limit }} ROWS ONLY
 {% endif %}
{%- endif %}
;
