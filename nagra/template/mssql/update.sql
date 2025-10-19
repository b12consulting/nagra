{% macro q(name) -%}[{{ name }}]{%- endmacro %}
UPDATE {{ q(table) }}
SET
  {% for col in columns if col not in condition_key -%}
  {{ q(col) }} = ?{{ ", " if not loop.last else "" }}
  {%- endfor %}
{% if returning %}
OUTPUT {% for col in returning -%}
 inserted.{{ q(col) }}{{ ", " if not loop.last }}
{%- endfor %}
{% endif %}
WHERE
  {% for col in condition_key -%}
  {{ q(col) }} = ?{{ " AND " if not loop.last else "" }}
  {%- endfor %}
;
