{% macro q(name) -%}[{{ name }}]{%- endmacro %}
ALTER TABLE {{ q(table) }}
 ADD CONSTRAINT FOREIGN KEY ({{ q(column) }})
 REFERENCES {{ q(foreign_table) }} ({{ q(foreign_column) }});
