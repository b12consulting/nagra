{% if set_identity %}
SET IDENTITY_INSERT [{{ table }}] ON;
{% endif %}

MERGE INTO [{{ table }}] AS target
USING
  (SELECT
  {% for col in columns -%}
    ? AS [{{ col }}]{{ ", " if not loop.last else "" }}
  {%- endfor %}
) AS source
ON (
  {% for col in conflict_key -%}
  target.[{{ col }}] = source.[{{ col }}]{{ " AND " if not loop.last else "" }}
  {%- endfor %}
)
{% if do_update %}
WHEN MATCHED THEN
  UPDATE SET
  {% for col in columns if col not in conflict_key -%}
  [{{ col }}] = source.[{{ col }}]{{ ", " if not loop.last else "" }}
  {%- endfor %}
{% endif %}
WHEN NOT MATCHED THEN INSERT (
  {%- for col in columns -%}
    [{{ col }}]{{ ", " if not loop.last else "" }}
  {%- endfor -%}
  ) VALUES (
  {%- for col in columns -%}
    source.[{{ col }}]{{ ", " if not loop.last else "" }}
  {%- endfor -%}
  )
{% if returning %}
OUTPUT {% for col in returning -%}
 inserted.[{{ col }}]{{ ", " if not loop.last }}
{%- endfor %}
{% endif %}
;

{% if set_identity %}
SET IDENTITY_INSERT [{{ table }}] OFF;
{% endif %}
