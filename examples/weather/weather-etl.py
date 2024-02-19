from pathlib import Path

from nagra import Transaction, Table, load_schema
from pandas import read_csv


HERE = Path(__file__).parent
DB = "sqlite://weather.db"


def init():
    db_ok = Path(DB).exists()
    load_schema(HERE / "weather_schema.toml", create_tables=not db_ok)


def load_city():
    # load cities
    df = read_csv(HERE / "city.csv")
    # Create upsert
    city_upsert = Table.get("city").upsert("name")
    # Execute it
    city_upsert.executemany(df.values)


def load_weather():
    # load cities
    df = read_csv(HERE / "weather.csv")
    weather_upsert = Table.get("weather").upsert(
        "city.name",
        "timestamp",
        "temperature",
        "wind_speed",
    )
    weather_upsert.executemany(df.values)


if __name__ == "__main__":
    import sys
    with Transaction(DB):
        init()
        if sys.argv[1] == "city":
            load_city()
        elif sys.argv[1] == "weather":
            load_weather()
        else:
            print("usage: python weather-etl.py [city | weather]")
