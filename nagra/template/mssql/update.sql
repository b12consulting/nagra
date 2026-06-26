UPDATE [{{ table }}]
SET
  {% for col in columns if col not in condition_key -%}
  [{{ col }}] = ?{{ ", " if not loop.last else "" }}
  {%- endfor %}
{% if returning %}
OUTPUT {% for col in returning -%}
 inserted.[{{ col }}]{{ ", " if not loop.last }}
{%- endfor %}
{% endif %}
WHERE
  {% for col in condition_key -%}
  [{{ col }}] = ?{{ " AND " if not loop.last else "" }}
  {%- endfor %}
;
