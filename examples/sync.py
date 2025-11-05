"""
This example is based on the crashcourse and shows how to synchronise
two different databases.

To run this example, you should first create a postgresql database, on
a Linux or MacOS you can simply do:

    createdb nagra-sync-demo

The sqlite one is implicitly created.

You can then run:

    python examples/sync.py

This should simply print "Done". You should then be able to query the
sqlite db like this:

    $ sqlite3 nagra-sync-demo.db "SELECT c.name, t.value  FROM temperature t JOIN city c ON (t.city = c.id)"
    Louvain-la-Neuve|6.0
    Brussels|7.0
    Brussels|8.0
    Brussels|5.0
    Brussels|3.0

"""

from nagra import Schema, Transaction


schema_toml = """
[city]
natural_key = ["name"]
not_null = ["lat", "long"]
[city.columns]
country = "varchar"
name = "varchar"
lat = "varchar"
long = "varchar"
[city.one2many]
temperatures = "temperature.city"


[temperature]
natural_key = ["city", "timestamp"]
[temperature.columns]
city = "bigint"
timestamp = "timestamp"
value = "float"
[temperature.foreign_keys]
city = "city"
"""


def pg_init(pg_dsn):
    with Transaction(pg_dsn):
        schema = Schema.from_toml(schema_toml)
        schema.create_tables()

        # Add cities
        cities = [
            ("Brussels", "50.8476° N", "4.3572° E"),
            ("Louvain-la-Neuve", "50.6681° N", "4.6118° E"),
        ]
        city = schema.get("city")
        upsert = city.upsert("name", "lat", "long")
        upsert.executemany(cities)  # Execute upsert

        # Add temperatures
        temperature = schema.get("temperature")
        upsert = temperature.upsert("city.name", "timestamp", "value")

        upsert.executemany(
            [
                ("Louvain-la-Neuve", "2023-11-27 16:00:00", 6),
                ("Brussels", "2023-11-27 17:00:00", 7),
                ("Brussels", "2023-11-27 20:00:00", 8),
                ("Brussels", "2023-11-27 23:00:00", 5),
                ("Brussels", "2023-11-28 02:00:00", 3),
            ]
        )


def sync_to_sqlite(pg_dsn, sqlite_dsn):
    # Get Schema and data
    with Transaction(pg_dsn):
        schema = Schema.from_db()
        cities = list(schema.get("city").select())
        temperatures = list(schema.get("temperature").select())

    # Sync to SQLite
    with Transaction(sqlite_dsn):
        schema.create_tables()
        # We make sure to insert cities first in order to properly
        # resolve the foreign key
        schema.get("city").upsert().executemany(cities)
        schema.get("temperature").upsert().executemany(temperatures)


if __name__ == "__main__":
    pg_dsn = "postgresql:///nagra-sync-demo"
    sqlite_dsn = "sqlite://nagra-sync-demo.db"

    pg_init(pg_dsn)
    sync_to_sqlite(pg_dsn, sqlite_dsn)
    print("Done")
