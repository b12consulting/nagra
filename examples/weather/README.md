
# Weather Example

This folder contains a miniature version of a data project with an
ETL, a reporting dashboard and an api. It serves as a showcase on how
Nagra simplify data modeling and data access and how it can be composed
with the usual tooling of a data pipeline.

We use sqlite for all the examples, but you should be able to switch
`sqlite://weather.db` to `postgresql:///weather` and run the exact same
code.

## Data sources

Our two sources in this example are the files `city.csv` and
`weather.csv`. By chance, the cities given in the wearther file are
exactly the same as the ones in the city file, with the same spelling
or capitalization.

``` shell
$ cat city.csv
name
Bruxelles
Louvain-la-Neuve
Leuven

$ cat weather.csv
city,timestamp,temperature,wind_speed
Bruxelles, "2024-06-01 12:00", 20, 30
Louvain-la-Neuve, "2024-06-01 12:00", 21, 20
Leuven, "2024-06-01 12:00", 19, 10
```



## Data Model

The first problem to solve is to define a data model. With Nagra one
can define a table like this:

``` python
city = Table(
    name="city",
    columns={
        "name": "varchar",
    },
    natural_key=["name"],
)
```

This definition gives the name "city" to the table, and defines one
column "name" of type varchar (aka an `str` in Python parlance). This
also express the fact that the natural key is composed of the column
name, so we can have no two cities with the same name and conversely a
city is uniquely identified by its name.

Similarly we can define another table like this

``` python
weather = Table(
    "weather",
    columns={
        "city": "int",
        "timestamp": "timestamp",
        "temperature": "float",
        "wind_speed": "float",
    },
    natural_key=["city", "timestamp"],
    foreign_keys={
        "city": "city",
    }
)
```


The `foreign_key` parameter tells that the "city" column (the dict
key) is a reference to the "city" table (the dict value).

As a the number of table growth, it can become quickly burdersome to
define the tables definitions like this, that's why Nagra comes with a
function helper that can load all of those from a toml file.


## Schema

The `weather_schema.toml` contains our table definitions and can be
loaded with `Schema.load`, like this:


``` python
from nagra import Transaction, Table, Schema

db = "sqlite://weather.db"
schema = Schema.default
with Transaction(db):
    schema.load("weather_schema.toml")
    schema.create_tables()
```


The example here above also introduces the `Transaction` object. This
context manager wraps the block of code under it in a database
transaction, making it fully atomic. It is usually used in a program
entry point, guaranteeing that the database is kept in a consistent
state even in case of a crash.

What we also see is that `schema` also provides a `create_tables`
method, so it is not only able to instanciate our `Table` objects but
also to create actual tables in the database.

If your run the above code, you should see a new file `weather.db` and
this sqlite database should contain two tables:

``` shell
$ sqlite3 weather.db ".tables"
city     weather
```


## ETL - City

Now that our data model is in place, we can finally load some
data. Thanks to pandas, its easy to create a dataframe out of the csv
files:

``` python
df = read_csv(HERE / "city.csv")
```

Whe can then use `Table.get` to get back our city table and ask it for
an upsert statement:

``` python
city_upsert = Table.get("city").upsert("name")
```

We passed "name" as argument, simply to tell that we will update (or
insert) values in the "name" column. If no argument is given, upsert
will default to all. We can ask to get the sql statement:

``` python
>>> print(city_upsert.stm())
INSERT INTO "city" (name)
VALUES (
  ?
)
ON CONFLICT (
 name
)
DO NOTHING
```

Or we can execute it like this:

``` python
city_upsert.execute("Bruxelles")
```

This will add a line in the table. Since we already have a dataframe,
we can use `executemany` to loads all the lines:

``` python
city_upsert.executemany(df.values)
```


## Interlude

Lets pause our work here and prepare a stripped-down version of what
we did until now.

``` python
from nagra import Transaction, Table, Schema

db = "sqlite://weather.db"
schema = Schema.default
with Transaction(db):
    schema.load("weather_schema.toml")
    schema.create_tables()
    city_upsert = Table.get("city").upsert("name")
    records = [
        ("Bruxelles",),
        ("Louvain-la-Neuve",),
    ]
    city_upsert.executemany(records)
```


If you run the above code, you will be able to inspect the result with
the sqlite3 cli:

``` shell
$ sqlite3 weather.db
SQLite version 3.43.2 2023-10-10 13:08:14
Enter ".help" for usage hints.
sqlite> .mode col
sqlite> SELECT * FROM city;
id  name
--  ----------------
1   Bruxelles
2   Louvain-la-Neuve
```

As you can see, the table also contains and `id` column, this column
is added automatically on every table created by Nagra, and is used as
a reference to every foreign that needs to model a link to the table.

If you define the `NAGRA_DB` and `NAGRA_SCHEMA` environment variables,
you can also use the `nagra` cli:


``` shell
export NAGRA_DB=sqlite://weather.db
export NAGRA_SCHEMA="weather_schema.toml"
```

and then for example:

``` shell
$ nagra select city
name
----------------
Bruxelles
Louvain-la-Neuve
```

Here `id` is not shown by default, but you can specify the column names
you want to see:

``` shell
$ nagra select city name id
name                id
----------------  ----
Bruxelles            1
Louvain-la-Neuve     2
```


## ETL - Weather Data

Just like we loaded `city.csv` we can load `weather.csv`:

``` python
df = read_csv(HERE / "weather.csv")
weather_upsert = Table.get("weather").upsert(
    "city.name",
    "timestamp",
    "temperature",
    "wind_speed",
)
weather_upsert.executemany(df.values)
```


Here again the arguments to `Table.upsert` are the columns names we
want to update.

There is a little difference though, we used `city.name` instead of
simply `city`. This means that we won't resolve the city names we got
from `weather.csv`.  Worded differently: We don't have to read the
existing city names and their corresponding `id` from the records in
the database in order give the proper values to the `city` column. We
let the library deal with it based on the fact that `city.name`
identifies the `name` column of the `city` table (or more precisely
the table referred by the foreign key column `city`).


## ETL - Full Run

All the steps explained here above are combined in
`weather-etl.py`. You have to invoque it like this:

``` shell
$ python weather-etl.py city
$ python weather-etl.py weather
```

Feel free to to inspect the content of the database after every step,
to remove the database file and run those in a reverse order.


## Streamlit Report

The Streamlit example in `streamlit-weather.py`, shows how to create a
select box based on the list of cities and then display the
corresponding lines in the `weather` table.

The select box is created like this:

``` python
cities = Table.get("city").select("name").orderby("name")
name = st.selectbox("City", [c for c, in cities])
```

The first line creates a select object:
- We get the table object with `Table.get("city")`
- Then `.select(..)` returns a select object, we pass `"name"` to
  specify which column we want to read
- Methods on select are chainable, so `orderby(...)` also returns the
  select object

Here we don't call `.execute` on it, we simply loop on it:
`[c for c, in cities]`. This is because a select is also iterable, it's only
possible when no parameter have to passed to the execute.

The DataFrame with weather data is created like this:

``` python
select =  Table.get("weather").select(
    "timestamp",
    "temperature",
    "wind_speed",
).where(
    '(= city.name {})'
).orderby("timestamp")
rows = select.execute(name)
df = DataFrame(rows, columns=select.columns)
```

Here again we use `Table.get` to get a table object and `.select(...)`
to choose the column to read. We then filter the results with
`.where`: it takes one or more boolean expression. Those expressions
use a polish notation and are composable, eg:

    '(or (= city.name "Bruxelles") (> timestamp "2024-01-01") )'

If more than one expression is given, a conjunction is implied:
`.where(A, B)` becomes `.where("(and A B)")`.


## API with FastAPI

TODO
