{%- if natural_key is defined -%}
CREATE UNIQUE INDEX IF NOT EXISTS {{table}}_idx ON "{{table}}" (
  {{ natural_key | map('autoquote') |join(', ') }}
);
{%- endif %}
