{%- if natural_key is defined -%}
CREATE UNIQUE INDEX [{{ table ~ "_idx" }}] ON [{{ table }}] (
  {%- for col in natural_key -%}
  [{{ col }}]{{ ", " if not loop.last }}
  {%- endfor %}
);
{%- endif %}
