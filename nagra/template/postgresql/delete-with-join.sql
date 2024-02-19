DELETE FROM "{{table}}"
WHERE "{{table}}".id IN (
  SELECT "{{table}}".id from "{{table}}"
  {%- for next_table, alias, prev_table, alias_col, prev_col in joins %}
   LEFT JOIN "{{next_table}}" as {{alias}} ON (
     {{alias}}."{{alias_col}}" = "{{prev_table}}"."{{prev_col}}"
   )
  {%- endfor -%}
  WHERE
  {{ conditions | join(' AND ') }}
)
