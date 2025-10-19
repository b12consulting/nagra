{% macro q(name) -%}[{{ name }}]{%- endmacro %}
ALTER TABLE {{ q(table) }}
 ADD {{ q(column) }} {{ col_def }}{{ " NOT NULL" if not_null else "" }}
{%- if default %}
 DEFAULT {{ default }}
{%- endif %}
;
{% if fk_table %}
ALTER TABLE {{ q(table) }}
 ADD CONSTRAINT fk_{{ column }} FOREIGN KEY ({{ q(column) }})
 REFERENCES {{ q(fk_table.name) }} ({{ q(fk_table.primary_key) }});
{% endif %}
