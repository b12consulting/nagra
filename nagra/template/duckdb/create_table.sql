CREATE SEQUENCE IF NOT EXISTS seq_{{table}}_id START 1;
CREATE TABLE IF NOT EXISTS "{{table}}" (
  id BIGINT DEFAULT nextval('seq_{{table}}_id'),
 {%- for name, col_def in columns.items() %}
  "{{name}}" {{col_def}}
  {{- " NOT NULL" if name in not_null else "" }}
  {{- ", " if not loop.last else "" }}
 {%- endfor -%}
);
