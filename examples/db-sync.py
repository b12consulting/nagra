"""
This example is based on the crashcourse and shows how to synchronise two different databases
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
            ("Brussels","50.8476° N", "4.3572° E"),
            ("Louvain-la-Neuve", "50.6681° N", "4.6118° E"),
        ]
        city = schema.get("city")
        upsert = city.upsert("name", "lat", "long")
        upsert.executemany(cities) # Execute upsert

        # Add temperatures
        temperature = schema.get("temperature")
        upsert = temperature.upsert("city.name", "timestamp", "value")

        upsert.execute("Louvain-la-Neuve", "2023-11-27 16:00:00", 6)
        upsert.executemany([
            ("Brussels", "2023-11-27 17:00:00", 7),
            ("Brussels", "2023-11-27 20:00:00", 8),
            ("Brussels", "2023-11-27 23:00:00", 5),
            ("Brussels", "2023-11-28 02:00:00", 3),
        ])


def sync_to_sqlite(pg_dsn, sqlite_dsn):
    # Get Schema and data
    with Transaction(pg_dsn):
        schema = Schema.from_db()
        cities = schema.get("city").select().to_dict()

    # Sync to SQLite
    with Transaction(sqlite_dsn):
        schema = Schema.from_toml(schema_toml)
        schema.create_tables()
        schema.get("city").upsert().from_dict(cities)


if __name__ == "__main__":
    pg_dsn = "postgresql:///nagra-sync-demo"
    sqlite_dsn = "sqlite://nagra-sync-demo.db"

    pg_init(pg_dsn)
    sync_to_sqlite(pg_dsn, sqlite_dsn)
    print("Synced")
