
# Install

    pip install nagra

Optionally, to work with Postgresql:

    pip install psycopg


# Crash course

## Table Definition

Tables are defined like this

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

Create transaction & execute. Example of other values possible for
 transation paramaters: `sqlite://some-file.db`,
 `postgresql://user:pws@host/dbname`:

We first add cities

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


Read data back

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
```
