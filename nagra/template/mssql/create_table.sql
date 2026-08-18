CREATE TABLE [{{ table.name }}] (
  {%- if table.primary_key %}
   [{{ table.primary_key }}] {{ctypes.get(table.primary_key, "BIGINT IDENTITY(1,1)")}} PRIMARY KEY
   {%- if pk_fk_table %}
    CONSTRAINT fk_{{table.primary_key}} REFERENCES [{{pk_fk_table.name}}]([{{pk_fk_table.primary_key}}]) ON DELETE CASCADE
   {%- endif %}
  {%- endif %}

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
