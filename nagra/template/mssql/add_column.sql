ALTER TABLE [{{ table }}]
 ADD [{{ column }}] {{ col_def }}{{ " NOT NULL" if not_null else "" }}
{%- if default %}
 DEFAULT {{ default }}
{%- endif %}
;
{% if fk_table %}
ALTER TABLE [{{ table }}]
 ADD CONSTRAINT fk_{{table}}_{{ column }} FOREIGN KEY ([{{ column }}])
 REFERENCES [{{ fk_table.name }}] ([{{ fk_table.primary_key }}])
 {{- " ON DELETE CASCADE" if not_null else "" }}
;
{% endif %}
