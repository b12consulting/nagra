SELECT table_name, view_definition
FROM information_schema.views
WHERE table_schema = '{{pg_schema}}';
