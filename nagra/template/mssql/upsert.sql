{% macro q(name) -%}[{{ name }}]{%- endmacro %}
{% if with_pk %}
SET IDENTITY_INSERT {{ q(table) }} ON;
{% endif %}

MERGE INTO {{ q(table) }} AS target
USING
  (SELECT
  {% for col in columns -%}
    ? AS {{ q(col) }}{{ ", " if not loop.last else "" }}
  {%- endfor %}
) AS source
ON (
  {% for col in conflict_key -%}
  target.{{ q(col) }} = source.{{ q(col) }}{{ " AND " if not loop.last else "" }}
  {%- endfor %}
)
{% if do_update %}
WHEN MATCHED THEN
  UPDATE SET
  {% for col in columns if col not in conflict_key -%}
  {{ q(col) }} = source.{{ q(col) }}{{ ", " if not loop.last else "" }}
  {%- endfor %}
{% endif %}
WHEN NOT MATCHED THEN INSERT (
  {%- for col in columns -%}
    {{ q(col) }}{{ ", " if not loop.last else "" }}
  {%- endfor -%}
  ) VALUES (
  {%- for col in columns -%}
    source.{{ q(col) }}{{ ", " if not loop.last else "" }}
  {%- endfor -%}
  )
{% if returning %}
OUTPUT {% for col in returning -%}
 inserted.{{ q(col) }}{{ ", " if not loop.last }}
{%- endfor %}
{% endif %}
;

{% if with_pk %}
SET IDENTITY_INSERT {{ q(table) }} OFF;
{% endif %}
