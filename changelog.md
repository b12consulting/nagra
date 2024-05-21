
# Changelog

### 0.1.0 (released: 2024-05-21)

**Breaking change:** `Select.to_dict` now returns an iterable

**New feature:** `Select.to_pandas` supports a `chunked` parameter: if
set to a non-zero value the method will return an iterable yielding
dataframes instead returning of a unique (possibly large) dataframe

**New feature:** Add `table.drop()`

**New feature:** Check for inconsistencies on hill-defined tables

**Breaking change:** Foreign keys are know defined with an `ON DELETE
CASCADE` pragma if the supporting column is required

**New feature:** Add support for UUID type

**New feature:** Support for concat `||` operator

**New feature:** Add support for default values on table columns


### 0.0.4 (released: 2024-04-03)

**Breaking change:** `load_schema()` has been replaced by Schema.load
(and Schema.from_toml).

**New feature:** Transaction can now be used without a context
manager, this provide more flexibility in multi-threaded app.

**New feature:** CockroachDB support

**New feature:** Add new column on existing tables on schema
creation. Before any existing table was kept as-is.

**New feature:** Auto validation of rows when a condition is given to
an upsert

**New feature:** New method `to_pandas` on Select.

**New feature:** Add support for one-to-many through table aliases

**New feature:** Upsert now returns the id of the rows created or
updated


## 0.0.3 (released: 2024-02-29)

**Breaking change:** `load_schema()` now accept a io object or a path object or a toml
payload. A simple file name is not accepted anymore

**New feature:** Add one to many support in select queries: Table constructor now
accepts a `one2many` parameters that can be used like this:

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

**New feature:** `Table.upsert` can now be used without giving the
columns of the statement. It defaults to the table columns (like
`Table.select`).

**New feature:** New method `from_pandas` on Upsert, this allows to
directly pass a dataframe to be written:

``` python
>>> df = DataFrame({"city": [...], "timestamp": [...], "value": [...]})
>>> upsert = temperature.upsert("city", "timestamp", "value")
>>> upsert.from_pandas(df)
```


## 0.0.2 (released: 2024-02-15)

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
