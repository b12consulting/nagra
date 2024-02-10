CREATE TABLE IF NOT EXISTS "{{table}}" (
  id INTEGER PRIMARY KEY,
 {%- for name, col_def in columns.items() %}
  "{{name}}" {{col_def}}
  {{- " NOT NULL" if name in required_columns else "" }}
  ,
 {%- endfor %}
STRICT);
