{%- if natural_key is defined -%}
CREATE UNIQUE INDEX {{table}}_idx ON "{{table}}" (
  {{ natural_key | map('autoquote') |join(', ') }}
);
{%- endif %}
