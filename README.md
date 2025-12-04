
# Install

Nagra is available on [PyPI](https://pypi.org/project/nagra/) and can be installed using `pip`, `uv`, ...e.g.:

    pip install nagra

Optional dependency targets:

- pandas support: `pandas`
- polars support: `polars`
- PostgreSQL: `pg`
- MSSQL Server: `mssql`
- to install all optional dependencies: `all`

For example:

    pip install nagra[polars,pg,mssql]

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

``` python
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
`sqlite://some-file.db`, `postgresql://user:pwd@host/dbname`, `mssql://user:pwd@host:1433/dbname`.

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
[crashcourse.py](https://github.com/b12consulting/nagra/tree/master/examples/crashcourse.py)


## Pandas support

If pandas is installed you can use `Select.to_pandas` and
`Upsert.from_pandas`, like this:

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

    [uv] pip install --group dev -e .

Or, to use stock uv functionalities:

    uv sync

To run the tests, you will need a local PostgreSQL cluster running (install it e.g. with `brew install postgresql`),
containing a database `nagra`. You can create it using the command `createdb nagra`. 
Then, simply run

    [uv run] pytest

## Testing setup

In order to run the test suite you will need a local Postgresql
instance, with an empty nagra db:

    createdb nagra

You will also need a Sql Server, run it with docker:

    docker run --platform linux/amd64 --cap-add SYS_PTRACE  -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=p4ssw0rD" -p 1433:1433  -d mcr.microsoft.com/mssql/server
    sqlcmd -S 127.0.0.1:1433 -d master -C -P p4ssw0rD -U sa

And in the sqlcmd shell, run:

    create database nagra
    go

Note: when using Podman Desktop on Apple Silicon, make sure the podman machine uses "Apple Hypervisor" as its provider (as opposed to `libkrun`), and make sure Rosetta emulation is activated in the settings of the "Podman" extension (needs to be changed before creating the VM).

You might also need to install the ODBC drivers for MSSQL using:

    brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
    brew update
    HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18

To skip some database systems when running the tests, run e.g.:

    pytest --skip-dsns mssql

# Miscellaneous

## Changelog and roadmap

The project changelog is available here:
[changelog.md](https://github.com/b12consulting/nagra/blob/master/changelog.md)

Future ideas:
- Support for other DBMS (SQL Server)


## Similar solutions / inspirations

https://github.com/malloydata/malloy/tree/main
:  Malloy is an experimental language for describing data
   relationships and transformations.

https://github.com/jeremyevans/sequel
:  Sequel: The Database Toolkit for Ruby


https://orm.drizzle.team/
: Headless TypeScript ORM with a head.
