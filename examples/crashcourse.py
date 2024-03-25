from nagra import load_schema, Transaction, Table

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
[temperature.foreign_keys]
city = "city"
"""

with Transaction("sqlite://"):
    load_schema(schema_toml, create_tables=True)

    # Add cities
    cities = [
        ("Brussels","50.8476° N", "4.3572° E"),
        ("Louvain-la-Neuve", "50.6681° N", "4.6118° E"),
    ]
    city = Table.get("city")
    upsert = city.upsert("name", "lat", "long")
    upsert.executemany(cities) # Execute upsert

    # Add temperatures
    temperature = Table.get("temperature")
    upsert = temperature.upsert("city.name", "timestamp", "value")
    upsert.execute("Louvain-la-Neuve", "2023-11-27T16:00", 6)
    upsert.executemany([
        ("Brussels", "2023-11-27T17:00", 7),
        ("Brussels", "2023-11-27T20:00", 8),
        ("Brussels", "2023-11-27T23:00", 5),
        ("Brussels", "2023-11-28T02:00", 3),
    ])

    # Read data back
    select = temperature.select("city.lat", "(avg value)").groupby("city.lat")
    print(select.stm())
    assert sorted(select) == [('50.6681° N', 6.0), ('50.8476° N', 5.75)]

    # Use the one2many
    select = city.select(
        "name",
        "(avg temperatures.value)"
    ).orderby("name")
    assert dict(select) == {'Brussels': 5.75, 'Louvain-la-Neuve': 6.0}
