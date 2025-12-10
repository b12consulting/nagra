CREATE TABLE  "{{table.name}}" (
  {%- if table.primary_key %}
   "{{table.primary_key}}" {{ctypes.get(table.primary_key, "BIGSERIAL")}} PRIMARY KEY
  {%- endif %}

  {%- for column in columns %}
   {{" , " if (not loop.first) or table.primary_key}}

   "{{column}}" {{ctypes[column]}}

   {%- if column in not_null %}
    NOT NULL
   {%- endif %}

   {%- if column in table.default %}
    DEFAULT {{table.default[column]}}
   {%- endif %}
  {%- endfor %}
);
