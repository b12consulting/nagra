
Nagra is a Python database toolkit targeting Postgresql and Sqlite.

# Install

Nagra is available on [PyPI](https://pypi.org/project/nagra/) and can be installed using `pip`, `uv`, ...e.g.:

    pip install nagra

Optional dependency targets:

- pandas support: `pandas`
- polars support: `polars`
- PostgreSQL: `pg`
- to install all optional dependencies: `all`

For example:

    pip install nagra[polars,pg]

# Crash course

## Define tables

The simplest way to define a database schema is to define the tables
in a toml string

``` python
from nagra import Schema

schema_toml = """
[city]
natural_key = ["name"]
[city.columns]
name = "str"
lat = "str"
long = "date"
[city.one2many]
temperatures = "temperature.city"

[temperature]
natural_key = ["city", "timestamp"]
[temperature.columns]
city = "bigint"
timestamp = "timestamp"
value = "float"
"""

schema = Schema.from_toml(schema_toml)
```

You can then use `schema.get(table_name)` to get a table by name:

``` python
city = schema.get('city')
```

It's often handy to maintain the schema in its own file (in the
example here in `schema.toml`), and use `from_toml` with a file handle
of a `Path`instance:

```python
schema = Schema.from_toml(open('schema.toml'))
# or
schema = Schema.from_toml(Path('schema.toml'))
```

## Generate SQL Statements

Let's first create a select statement

``` python
stm = city.select("name").stm()
print(stm)
# ->
# SELECT
#   "city"."name"
# FROM "city"
```

If no fields are given, `select` will query all fields and resolve foreign keys
``` python
stm = temperature.select().stm()
print(stm)
# ->
# SELECT
#   "temperature"."timestamp", "city_0"."name", "temperature"."value"
# FROM "temperature"
# LEFT JOIN "city" as city_0 ON (city_0.id = "temperature"."city")
```

One can explicitly ask for foreign key, with a dotted expression

``` python
stm = temperature.select("city.lat", "timestamp").stm()
print(stm)
# ->
# SELECT
#   "city_0"."lat", "temperature"."timestamp"
# FROM "temperature"
# LEFT JOIN "city" as city_0 ON (city_0.id = "temperature"."city")
```

So in the above example, `city`, a column of the `temperature` table, is
a foreign key to the `city` table. By prefixing the expression with
the column name we can easily access the linked table, without having
to express a join condition.

This works implicitly because `nagra` enforce both a primary key
(unsurprisingly named `id`) on every table and will base every foreign
key on those primary keys.

### Python definition

Similarly one table can be defined directly in Python, like this:

``` python
from nagra import Table

city = Table(
    "city",
    columns={
        "name": "str",
        "lat": "str",
        "long": "str",
    },
    natural_key=["name"],
    one2many={
        "temperatures": "temperature.city",
    }
)
```


## Add Data and Query Database

A `with Transaction ...` statement defines a transaction block, with
an atomic semantic (either all statement are successful and the
changes are commited or the transaction is rollbacked).

Example of other values possible for transaction parameters:
`sqlite://some-file.db`, `postgresql://user:pwd@host/dbname`.

We first add cities:

``` python
with Transaction("sqlite://"):
    schema.create_tables()  # Emit all the "CREATE TABLE ..." in the database

    cities = [
        ("Brussels","50.8476° N", "4.3572° E"),
        ("Louvain-la-Neuve", "50.6681° N", "4.6118° E"),
    ]
    upsert = city.upsert("name", "lat", "long")
    print(upsert.stm())
    # ->
    #
    # INSERT INTO "city" (name, lat, long)
    # VALUES (?,?,?)
    # ON CONFLICT (name)
    # DO UPDATE SET
    #   lat = EXCLUDED.lat , long = EXCLUDED.long

    upsert.executemany(cities) # Execute upsert
```

We see that the upsert statement will automatically rely on the
natural key in order to express the `ON CONFLICT` pragma. Here again,
this implicit behaviour comes from a design choice of the table
class. In `nagra` it's not possible to define a table without a
natural key. When the table is created, a unique index is defined on
the table based on the natural key definition.

The enforcement of natural keys greatly simplifies the `upsert`
operations, but it's also a good practice to have at least one unicity
constraint on each table.

We can then add temperatures

``` python
    upsert = temperature.upsert("city.name", "timestamp", "value")
    upsert.execute("Louvain-la-Neuve", "2023-11-27T16:00", 6)
    upsert.executemany([
        ("Brussels", "2023-11-27T17:00", 7),
        ("Brussels", "2023-11-27T20:00", 8),
        ("Brussels", "2023-11-27T23:00", 5),
        ("Brussels", "2023-11-28T02:00", 3),
    ])
```


Read data back:

``` python
    records = list(city.select())
    print(records)
    # ->
    # [('Brussels', '50.8476° N', '4.3572° E'), ('Louvain-la-Neuve', '50.6681° N', '4.6118° E')]
```

Aggregation example: average temperature per latitude:

``` python
    # Aggregation
    select = temperature.select("city.lat", "(avg value)").groupby("city.lat")
    print(list(select))
    # ->
    # [('50.6681° N', 6.0), ('50.8476° N', 5.75)]

    print(select.stm())
    # ->
    # SELECT
    #   "city_0"."lat", avg("temperature"."value")
    # FROM "temperature"
    #  LEFT JOIN "city" as city_0 ON (
    #     city_0."id" = "temperature"."city"
    #  )
    # GROUP BY
    #  "city_0"."lat"
    #
    # ;
```


Similarly we can start from the `city` table and use the
`temperatures` alias defined in the one2many dict:


``` python
    select = city.select(
        "name",
        "(avg temperatures.value)"
    ).orderby("name")
    assert dict(select) == {'Brussels': 5.75, 'Louvain-la-Neuve': 6.0}
```

The complete code for this crashcourse is in
[crashcourse.py](https://github.com/b12consulting/nagra/tree/master/examples/crashcourse.py)


## Dataframe support

If pandas or polars is installed you can use `Select.to_pandas` (or `to_polars`) and
`Upsert.from_pandas` (`from_polars`), like this:

``` python
    # Generate df from select
    df = temperature.select().to_pandas()
    print(df)
    # ->
    #           city.name         timestamp  value
    # 0  Louvain-la-Neuve  2023-11-27T16:00    6.0
    # 1          Brussels  2023-11-27T17:00    7.0
    # 2          Brussels  2023-11-27T20:00    8.0
    # 3          Brussels  2023-11-27T23:00    5.0
    # 4          Brussels  2023-11-28T02:00    3.0

    # Update df and pass it to upsert
    df["value"] += 10
    temperature.upsert().from_pandas(df)
    # Let's test one value
    row, = temperature.select("value").where("(= timestamp '2023-11-28T02:00')")
    assert row == (13,)
```


# Development

To install the project in editable mode along with all the optional dependencies
as well as the dependencies needed for development (testing, linting, ...),
clone the project and run:

    [uv ] pip install --group dev -e '.[all]'

Or, to use stock uv functionalities:

    uv sync --extra all


# Miscellaneous

## Changelog and roadmap

The project changelog is available here:
[changelog.md](https://github.com/b12consulting/nagra/blob/master/changelog.md)

Future ideas:
- Support for other DBMS (SQL Server)
- CTE support
- Database migrations


## Similar solutions / inspirations

https://github.com/malloydata/malloy/tree/main
:  Malloy is an experimental language for describing data
   relationships and transformations.

https://github.com/jeremyevans/sequel
:  Sequel: The Database Toolkit for Ruby

https://orm.drizzle.team/
: Headless TypeScript ORM with a head.
