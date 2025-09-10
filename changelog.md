
# Changelog


### 0.7 (to be released)

**Operators support** S-expression module now infer operator signature
for all infix operators, only infix ones must be explicitly
declared. This change make a large range of operator automatically
available

**Nested models** Add support for nested models on both
`Select.to_dataclass` and `Select.to_dict`.

**PG Vector** Add partial support for `vector` datatype: Nagra can now
properly detect `vector` column (treated has float arrays) when
introspecing existing tables.

**Misc**
- Fixed bug when accessing Transaction in multi-threaded context.
- Fix pydantic schema generation (cli usag)
- ` Upsert.from_dict` now support dotted notation when receiving
  records.

**Breaking change** 
- `Table.get` raise an exception when the table is
not found (instead of returning `None`)

### 0.6 (released 2025-07-18)

**New feature:** Add `Schema.generate_pydantic_models` utility
method. This allows for the automatic generation of Pydantic models
based on database schema. It can also be used from the cli with:

    nagra schema  --pydantic

**New feature:** Support for PostgreSQL's `COPY FROM` for
high-performance data ingestion. This can be used via the new
`Table.copy_from` method. It behaves like an insert except the choice
of column to write to is not customisable, it is always on all the
columns.

**New feature:** The `suspend_fk` context manager now supports
SQLite. Beware that contary to Postgresql, any foreign key violation
made while foreign keys are suspended wont be spotted by Sqlite.


**Fixes:**
- Fixed an issue where foreign key constraints were not correctly
  applied to tables using only natural keys.
- Corrected a bug that occurred during table creation when a primary
  key was also a foreign key.
- Improve database introspection to correctly differentiate between
  primary and natural keys.
- Ensure the correct PostgreSQL schema is targeted when retrieving
  constraints (removed hard-coded `public` schema).
- Improve typing issues in the schema definition.


### 0.5 (released 2025-04-11)

**New feature:** Add support for views, both creation and
introspection are supported.

Views can be defined with two different fashion in toml:

``` toml
# With explicit definition ..
[max_temp]
as_select = """
SELECT city.name as city, max(temperature.value) as max_temp
FROM city
JOIN temperature on (temperature.city = city.id)
GROUP BY city.name
"""
[max_temp.columns]
city = "str"
max_temp = "float"

# .. or with nagra s-expressions
[min_temp]
view_select = "city"
[min_temp.view_columns]
min_temp = "(min temperatures.value)"
city = "name"
```

The `View` object behaves like the `Table` object (for select
statements) and a view can be accessed by name with
`schema.get("my_view_nane")` of with `View.get("name")`

**New feature:** Add `Upsert.from_dict`, the mirror method of
`Select.to_dict`, see example usage in
[db-sync.py](examples/db-sync.py).

**New feature:** Add `Upsert.resolve`, this allows to re-use foreign
key resolution before db insertion.

**Fixes:**
- Support for blob type: `Select.dtype` failed to resolve data
  type. Now nagra will ignore blob column in select by default.
- `Select.to_pandas` now returns correct dataframe when resultset is empty
- Fix parenthesization for OR expressions
- Raise error on malformed natural key

**Various :**
- Add `length`, `upper` and `lower` operators
- Python >= 3.11 compat (end of support of class properties):
  `Schema.default` is now a class member
- Timescaledb compat: Do not rely on `IF NOT EXISTS` stanza when creating indexes


### 0.4 (released 2024-01-28)

**New feature:** Add support for tables without primarey key. No `id`
column is created and the table can not be referenced by other tables.
Usefull for large table like timeseries.

Toml example:

``` toml
[blog]
natural_key = ["title"]
primary_key = ""
[blog.columns]
title = "text"
length = "int"
user = "bigint"
[blog.foreign_keys]
user = "user"
```

Python example:

``` python
Table(
    "blog",
    columns={
        "tile": "text",
        "length": "int",
        "user": "bigint",
    },
    natural_key=["title"],
    primary_key=None,
    foreign_keys={"user": "user"}
)
```

**New feature:** Add support for tables and columns using reserved
words like `null`, `max` or `select`.

**Fixes:**
- Add support for tables using reserved word like `transaction`, `limit`,
etc.
- Fix returning in with SQLite for tables with custom primary key
  (thanks @JonathanSamelsonB12).
- Python 3.13, `Transaction.current` is not a property anymore, it is
  now a function.

**New feature:** Add `LRUGenerator` for `WriterMixin._resolve`. This will
cache foreign keys resolution within the duration of a transaction. It
has to be enabled through the `fk_cache` parameter when the
transaction is created:

``` python
with Transaction(dsn, fk_cache=True): # default is False
    ...

```

It will speed-up upserts (and inserts and updates) when a foreign key
is involved.  It shouldn't impact application logic in most
situation. The main corner case is when the `id` value for a given
natural key changes, but this kind of operation is discouraged.


**Various:**
- New `empty` property on `Schema`, return true if no table are present.
- New `sync.py` in `examples/`, demonstrate how to synchonise two
  databases with the help of Nagra.
- New `substr`, `isnot` and `match` operators
- Default to `TEXT` instead of `VARCHAR` for string-like column in
  Postgresql (improve compatibitly with Timescaledb)
- New `empty` property on `Schema`
- `timestamptz` support in SQLite (thanks @cecilehautecoeur)

### 0.3 (released 2024-11-04)

**New feature:** DB introspection: Nagra is now able to introspect
existing databases and infer schema:

``` python
from nagra import Transaction, Schema

with Transaction('sqlite://examples/weather/weather.db'):
    schema = Schema.from_db()
    print(list(schema.tables))
    # -> ['city', 'weather']

    city = schema.get('city')
    print(list(city.columns))
    # -> ['id', 'name']
```


**New feature:** Temporary suppression of foreign keys constraints. The
`Schema.suspend_fk` context manager is able to drop foreign keys
constraints and re-add them at the end of the block. This allows to
load more complex datasets with cross-references.

**New feature:** The method `Schema.setup_statement` can be used to
generate the simple migration statements without executing them

**Fix** delete with SQLite when a parameter is passed (issue #15)

**Breaking change:** CockroachDB support removed

**Fix** support for '-' operator when only one operand is given

**Fix** `Select.orderby` when multiple expressions are given

**Fix** `Upsert.executemany` when no data is given (empty list)


### 0.2.0 (released 2024-08-23)

**Breaking change:** Rename `Schema.load` into `Schema.load_toml`

**New feature:** Cli: Add csv export on select, with `--csv` flag

**Fix:**  Add proper quotes around column names on postgresql upsert

**New feature:** Add array support: So one can now declare a table like:

``` python
parameter_table = Table(
    "parameter",
    columns={
        "name": "str",
        "timestamps": "timestamp []",
        "values": "float []",
    },
    natural_key=["name"],
)
upsert = parameter.upsert()
records = [
    ("one", ["2024-08-03T10:42", "2024-08-03T10:43"], [2, 3]),
    ("two", ["2024-08-03T10:44", "2024-08-03T10:45"], [4, 5]),
    ("three", ["2024-08-03T10:46", "2024-08-03T10:47"], [6, 7]),
]
upsert.executemany(records)
```

**Fix:** Cli: Fix where condition on delete when used from the cli.

**New feature:** Add query arguments support in to_dict:

``` python
temperature = Schema.get("temperature")
records = temperature.select().where("(= value {})").to_dict(12)
```

**New feature:** Type `timestamptz` is now accepted for SQLite.


###  0.1.2 (released: 2024-06-30)

**New feature:** Create new cursor for each execute, it allows for
example to iterate on select and update the db record by record

**New feature:** Add `Update` class and basic tests, this is mainly
useful when updating by id when the full natural key is not known (or
too long)

**Fix:** `limit` and `offset` values where lost on Select.clone

**Fix** Auto-convert pandas columns to string for non-basic dtypes

**Fix:** Ensure schema loading is correct

**New feature:** Add support for custom primary key

**New:** Add db introspection:
- Discovery of columns and types
- Discovery of primary keys and unique indexes



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
