class {{class_name}}Stub({{base_class}}):
   {% for name in table.natural_key -%}
   {%- set col = table.columns[name] -%}
   {%- if name in table.foreign_keys -%}
         {{name}}: {{snake_to_pascal(name)}}Stub {% if not table.required(name) %} | None {% endif %}
   {%- else -%}
         {{name}}: {{col.dtype}} {% if not table.required(name) -%} | None {% endif %}
   {%- endif %}
   {% endfor %}

class {{class_name}}({{base_class}}):
    {% for name, col in table.columns.items() -%}
    {% if name in table.foreign_keys -%}
         {{name}}: {{snake_to_pascal(name)}}Stub {% if not table.required(name) -%} | None {% endif %}
    {% else -%}
         {{name}}: {{col.dtype}} {% if not table.required(name) -%} | None {% endif %}
    {% endif -%}
    {%- endfor -%}
