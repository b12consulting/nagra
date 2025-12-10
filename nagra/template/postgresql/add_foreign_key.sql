ALTER TABLE "{{table}}"  ADD CONSTRAINT "{{name}}"
 FOREIGN KEY ("{{column}}")
 REFERENCES "{{foreign_table}}" ("{{foreign_column}}");
