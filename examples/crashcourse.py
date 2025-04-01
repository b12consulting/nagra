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

with Transaction("sqlite://"):
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

    upsert.execute("Louvain-la-Neuve", "2023-11-27 16:00:00", 6)
    upsert.executemany([
        ("Brussels", "2023-11-27 17:00:00", 7),
        ("Brussels", "2023-11-27 20:00:00", 8),
        ("Brussels", "2023-11-27 23:00:00", 5),
        ("Brussels", "2023-11-28 02:00:00", 3),
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

    # Pandas example: generate df from table
    df = temperature.select().to_pandas()
    print(df)

    # Update df and save it
    df["value"] += 10

    temperature.upsert().from_pandas(df)
    row, = temperature.select("value").where("(= timestamp '2023-11-28 02:00:00')")
    assert row == (13,)
