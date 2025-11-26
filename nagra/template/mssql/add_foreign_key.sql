ALTER TABLE [{{ table }}]
 ADD CONSTRAINT FOREIGN KEY ([{{ column }}])
 REFERENCES [{{ foreign_table }}] ([{{ foreign_column }}]);
