CREATE TABLE  "{{table.name}}" (
  {%- if table.primary_key %}
   "{{table.primary_key}}" {{ctypes.get(table.primary_key, "BIGSERIAL")}} PRIMARY KEY
  {%- endif %}

  {%- for column in table.columns.values() if column.name != table.primary_key %}
   {{" , " if (not loop.first) or table.primary_key}}

   "{{column.name}}" {{ctypes[column.name]}}

   {%- if column.name in not_null %}
    NOT NULL
   {%- endif %}

   {%- if column.name in table.default %}
    DEFAULT {{table.default[column.name]}}
   {%- endif %}
  {%- endfor %}
);
