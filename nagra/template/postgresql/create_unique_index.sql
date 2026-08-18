{%- if natural_key is defined -%}
CREATE UNIQUE INDEX {{table}}_idx ON "{{table}}" (
  {{ natural_key | map('autoquote') |join(', ') }}
)
  {%- if nulls_not_distinct is defined and nulls_not_distinct %}
    NULLS NOT DISTINCT
  {%- endif %}
;
{%- endif %}
