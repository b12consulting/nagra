select table_name, view_definition from  information_schema.views where table_schema = '{{pg_schema}}';
