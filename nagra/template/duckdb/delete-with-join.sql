DELETE FROM "{{table}}"
WHERE "{{table}}".id IN (
  SELECT "{{table}}".id from "{{table}}"
  {%- for next_table, alias, prev_table, col in joins %}
  JOIN "{{next_table}}" as {{alias}} ON ({{alias}}.id = "{{prev_table}}"."{{col}}")
  {%- endfor %}
  WHERE
  {{where}}
)

