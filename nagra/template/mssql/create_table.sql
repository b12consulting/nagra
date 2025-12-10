CREATE TABLE [{{ table.name }}] (
  [{{ table.primary_key }}] {{ pk_type or "BIGINT IDENTITY(1,1)" }} PRIMARY KEY

  {%- for column in columns %}
   {{" , " if (not loop.first) or table.primary_key}}

   [{{ column }}] {{ ctypes[column] }}

   {%- if column in not_null %}
    NOT NULL
   {%- endif %}

   {%- if column in table.default %}
    DEFAULT {{ table.default[column] }}
   {%- endif %}
  {%- endfor %}
);
