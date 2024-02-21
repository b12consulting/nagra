
# Changelog

## 0.0.3


**Breaking change:** `load_schema()` now accept a io object or a path object or a toml
payload. A simple file name is not accepted anymore

**New feature:** Add one to many support in select queries: Table constructor now
accepts a `many2one` parameters that can be used like this:

``` python
person_table = Table(
    "person",
    columns={
        "name": "varchar",
        "parent": "bigint",
    },
    foreign_keys={
        "parent": "person",
    },
    natural_key=["name"],
    one2many = {
        "skills": "skill.person",
    }
)
```

or via toml:

``` toml
[person]
natural_key = ["name"]
[person.columns]
name = "varchar"
parent = "bigint"
birthdate = "date"
[person.one2many]
skills =  "skill.person"
```

This also means that there must be a table "skill" with a foreign key
column "person" that references the person table.

With such definitions, a select can use it like:

``` python
person.select(
    "name",
    "skills.name"
).stm()
```

Which gives:

``` sql
SELECT
  "person"."name", "skills_0"."name"
FROM "person"
 LEFT JOIN "skill" as skills_0 ON (
    skills_0."person" = "person"."id"
 );
```

**New feature:** `Table.upsert` now returns ids of inserted or updated
rows (no id is returned when a row is left untouched).

``` python
>>> person.delete()  # start from empty table
>>> upsert = person.upsert("name")
>>> records = [("Doe",)]
>>> upsert.executemany(records)
[1]
>>> upsert.executemany(records)
[]
```


## 0.0.2

**Breaking change:** `Table.upsert` now supports a `lenient` parameter
(default to `None`). If set to `True` (or to a list of column names),
allows a foreign key to not be resoled and simply put a null value in
database. If not set raises `UnresolvedFK` exception when a foreign
key can not be resolved (the corresponding value is not found in the
database)

**New feature:** `--where` argument in the command line now accept
multiple expressions. `--where` can also be repeated multiple times.

**New feature:** Proper support for `not_null` parameter. All column
names given in a list will be made required in the table definition in
the database.

**New feature:** Add `Schema.drop` to drop all tables in schema.
