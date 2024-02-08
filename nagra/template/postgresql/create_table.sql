CREATE TABLE IF NOT EXISTS "{{table}}" (
  id BIGSERIAL PRIMARY KEY,
 {%- for name, col_def in columns.items() %}
  "{{name}}" {{col_def}}
  {{- " NOT NULL" if name in not_null else "" }}
  {{- ", " if not loop.last else "" }}
 {%- endfor -%}
);
