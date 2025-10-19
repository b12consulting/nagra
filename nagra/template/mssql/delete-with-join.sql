{% macro q(name) -%}[{{ name }}]{%- endmacro %}
DELETE FROM {{ q(table) }}
WHERE {{ q(table) }}.[id] IN (
  SELECT {{ q(table) }}.[id] FROM {{ q(table) }}
  {%- for next_table, alias, prev_table, alias_col, prev_col in joins %}
   LEFT JOIN {{ q(next_table) }} AS {{ q(alias) }} ON (
     {{ q(alias) }}.{{ q(alias_col) }} = {{ q(prev_table) }}.{{ q(prev_col) }}
   )
  {%- endfor -%}
  WHERE
  {{ conditions | join(' AND ') }}
);
