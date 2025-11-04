INSERT INTO "{{table}}" ({{columns | map('autoquote') | join(', ') }})
VALUES (
  {% for col in columns -%}
  {{ "%s," if not loop.last else "%s" }}
  {%- endfor %}
)

{% if do_update %}
ON CONFLICT (
 {{conflict_key | map('autoquote') | join(', ') }}
)
DO UPDATE SET
  {% for col in columns if col not in conflict_key-%}
  "{{col}}" = EXCLUDED."{{col}}" {{", " if not loop.last}}
  {%- endfor %}
{% endif %}

{% if returning %}
RETURNING {{ returning | map('autoquote') |join(', ') }}
{% endif %}


