INSERT INTO "{{table}}" ({{columns | map('autoquote') | join(', ') }})
VALUES (
  {% for col in columns -%}
  {{ "?," if not loop.last else "?" }}
  {%- endfor %}
)

ON CONFLICT (
 {{conflict_key | map('autoquote') | join(', ') }}
)
{% if do_update %}
DO UPDATE SET
  {% for col in columns if col not in conflict_key-%}
  "{{col}}" = EXCLUDED."{{col}}" {{", " if not loop.last}}
  {%- endfor %}

{% else %}
DO NOTHING
{% endif %}

{% if returning %}
RETURNING {{ returning | map('autoquote') |join(', ') }}
{% endif %}
