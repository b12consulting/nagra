"""
Run me with first with `python fastapi-example.py` to populate the
db.

Start the api: `uvicorn fastapi-example:app`

You can also do :

    $ export NAGRA_DB=sqlite://example.db
    $ export NAGRA_SCHEMA=schema.toml
    $ nagra select city name
    ╭──────────╮
    │ name     │
    ├──────────┤
    │ Berlin   │
    │ Brussels │
    │ London   │
    ╰──────────╯
"""

from dataclasses import dataclass
from typing import List

from nagra import Transaction, Schema
from nagra.select import Select

from fastapi import FastAPI

schema_toml = """
[city]
natural_key = ["name"]
[city.columns]
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


app = FastAPI()
DB = "sqlite://example.db"
schema = Schema.default
schema.load_toml(schema_toml)


# Define models

@dataclass
class City:
    name: str
    lat: str | None = None
    long: str | None = None

@dataclass
class CityTemperatures:
    __table__ = "city"
    name: str
    temperature: float


@dataclass
class Temperature:
    city: City
    timestamp: str


# Define endpoints
@app.get("/temperatures/", response_model=List[Temperature])
def temperatures():
    with Transaction(DB):
        select = Select.from_dataclass(Temperature)
        return list(select.to_dict(nest=True))


# Custom endpoint
@app.get("/city/{name}/temperatures", response_model=List[CityTemperatures])
def city_temperatures(name: str):
    with Transaction(DB):
        city = schema.get("city")
        select = city.select(
            "name",
            "temperatures.value"
        ).aliases("name", "temperature")
        return list(select.where("(= name {})").to_dict(name))


# Init create db tables and automate the creation of GET endpoint for
# every table
def init():
    with Transaction(DB):
        schema.create_tables()


init()

if __name__ == "__main__":
    print("LOAD DATA")

    with Transaction(DB):
        schema.create_tables()
        city = schema.get("city")
        city.upsert("name").executemany([
            ("Brussels",),
            ("London",),
            ("Berlin",),
        ])

        temp = schema.get("temperature")
        temp.upsert("city.name", "timestamp", "value").executemany([
            ("Brussels", "2000-01-01T00:00", 0),
            ("London", "2000-01-01T00:00", 1),
            ("Berlin", "2000-01-01T00:00", 3),
            ("Berlin", "2000-01-01T01:00", 2),
        ])
