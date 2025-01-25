UPDATE "{{table}}"
SET
  {% for col in columns if col not in condition_key-%}
  "{{col}}" = {{ "%s," if not loop.last else "%s" }}
  {%- endfor %}

WHERE
  {% for col in condition_key-%}
  "{{col}}" = {{ "%s AND " if not loop.last else "%s" }}
  {%- endfor %}

{% if returning %}
RETURNING {{ returning | map('autoquote') |join(', ') }}
{% endif %}

