CREATE TABLE  "{{table.name}}" (
  {% for name, typedef in natural_key  -%}
  "{{name}}"  {{typedef}} NOT NULL {{", " if not loop.last}}
  {% endfor %}
);
