UPDATE "{{table}}"
SET
  {% for col in columns if col not in condition_key-%}
  "{{col}}" = {{ "?," if not loop.last else "?" }}
  {%- endfor %}

WHERE
  {% for col in condition_key-%}
  "{{col}}" = {{ "? AND " if not loop.last else "?" }}
  {%- endfor %}

RETURNING {{ returning | map('autoquote') |join(', ') }}

