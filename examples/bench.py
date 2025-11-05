from random import choices
from string import ascii_letters

from nagra import Transaction, Table, Schema
from nagra.utils import timeit


schema_toml = """
[city]
natural_key = ["name"]
[city.columns]
name = "varchar"
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


def random_name(k=10):
    return "".join(choices(ascii_letters, k=k))


def setup():
    schema = Schema.default
    if schema.empty:
        schema.load_toml(schema_toml)
    schema.create_tables()
    city = schema.get("city")
    temperature = schema.get("temperature")

    # Clear tables
    city.delete()
    temperature.delete()

    # Add cities
    cities = [
        ("Brussels",),
        ("Louvain-la-Neuve",),
    ]
    cities.extend(set(random_name() for _ in range(100_000)))

    upsert = city.upsert("name")
    upsert(cities)  # Execute upsert


def add_temp(with_cache):
    # Add temperatures
    temperature = Table.get("temperature")
    upsert = temperature.upsert("city.name", "timestamp", "value")

    for _ in range(2000):
        upsert.executemany(
            [
                ("Brussels", "2023-11-27 17:00:00", 7),
                ("Louvain-la-Neuve", "2023-11-27 20:00:00", 8),
                ("Brussels", "2023-11-27 23:00:00", 5),
                ("Louvain-la-Neuve", "2023-11-28 02:00:00", 3),
                ("Brussels", "2023-11-27 17:00:00", 7),
                ("Louvain-la-Neuve", "2023-11-27 20:00:00", 8),
                ("Brussels", "2023-11-27 23:00:00", 5),
                ("Louvain-la-Neuve", "2023-11-28 02:00:00", 3),
            ]
        )


def bench_fk_cache(dsn: str, fk_cache: bool):
    # Setup db
    with Transaction(dsn):
        setup()
    # Generate timing if temperature insertion
    with Transaction(dsn, fk_cache=fk_cache):
        with timeit(f"Insert temps, with cache = {fk_cache}"):
            add_temp(fk_cache)


if __name__ == "__main__":
    import cProfile

    dsn = "postgresql:///nagra-bench"

    for fk_cache in (True, False):
        profiler = cProfile.Profile()
        profiler.enable()
        bench_fk_cache(dsn, fk_cache)
        profiler.disable()
        profiler.dump_stats("fk_cache_{fk_cache}.prof")
        print(f"Run `snakeviz fk_cache_{fk_cache}.prof` to see profile")

    # Example output
    # Insert temps, with cache = True 1.73s
    # Run `snakeviz fk_cache_True.prof` to see profile
    # Insert temps, with cache = False 2.86s
    # Run `snakeviz fk_cache_False.prof` to see profile
