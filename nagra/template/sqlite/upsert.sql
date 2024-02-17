INSERT INTO "{{table}}" ({{columns | join(', ') }})
VALUES (
  {% for col in columns -%}
  {{ "?," if not loop.last else "?" }}
  {%- endfor %}
)

ON CONFLICT (
 {{conflict_key | join(', ') }}
)
{% if do_update %}
DO UPDATE SET
  {% for col in columns if col not in conflict_key-%}
  {{col}} = EXCLUDED.{{col}} {{", " if not loop.last}}
  {%- endfor %}
RETURNING id
{% else %}
DO NOTHING
{% endif %}
