from pathlib import Path

from nagra import Transaction, Table, Schema
from pandas import read_csv


HERE = Path(__file__).parent
DB = "sqlite://weather.db"


def init():
    Schema.default.load_toml(HERE / "weather_schema.toml")
    db_ok = Path(DB).exists()
    if not db_ok:
        Schema.default.create_tables()

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
