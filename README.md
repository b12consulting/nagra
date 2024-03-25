
# Install

    pip install nagra

Optionally, to work with Postgresql:

    pip install psycopg


# Crash course

## Define tables

Tables can be defined with classes like this:

``` python
from nagra import Table

city = Table(
    "city",
    columns={
        "name": "varchar",
        "lat": "varchar",
        "long": "varchar",
    },
    natural_key=["name"],
    one2many={
        "temperatures": "temperature.city",
    }
)

temperature = Table(
    "temperature",
    columns={
        "timestamp": "timestamp",
        "city": "int",
        "value": "float",
    },
    natural_key=["city", "timestamp"],
    foreign_keys={
        "city": "city",
    },

)
```


Or based on a toml string:

```
from nagra import load_schema

schema_toml = """
[city]
natural_key = ["name"]
[city.columns]
name = "varchar"
lat = "varchar"
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

load_schema(schema_toml)
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

One can explicitly ask for foreign key, with a dotted field

``` python
stm = temperature.select("city.lat", "timestamp").stm()
print(stm)
# ->
# SELECT
#   "city_0"."lat", "temperature"."timestamp"
# FROM "temperature"
# LEFT JOIN "city" as city_0 ON (city_0.id = "temperature"."city")
```


## Add Data and Query Database

A `with Transaction ...` statemant defines a transaction block, with
an atomic semantic (either all statement are successful and the
changes are commited or the transaction is rollbacked).

Example of other values possible for transaction parameters:
`sqlite://some-file.db`, `postgresql://user:pws@host/dbname`.

We first add cities:

``` python
with Transaction("sqlite://"):
    Schema.default.setup()  # Create tables

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
<https://github.com/b12consulting/nagra/tree/master/examples/crashcourse.py>


# Miscellaneous

The project changelog is available here:
https://github.com/b12consulting/nagra/blob/master/changelog.md

Future ideas:
- Integration with FastAPI
- Support for CockroachDB
