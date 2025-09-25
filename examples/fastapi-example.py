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
from datetime import datetime
from typing import List

from nagra import Transaction, Schema
from nagra.select import Select

from fastapi import FastAPI

schema_toml = """
[city]
natural_key = ["name"]
[city.columns]
name = "str"
lat = "str"
long = "str"
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
# or
#DB = "postgresql:///demo"

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
    timestamp: datetime


# Model based endpoint
@app.get("/temperatures/", response_model=List[Temperature])
def temperatures():
    with Transaction(DB):
        select = Select.from_dataclass(Temperature)
        return list(select.to_dict(nest=True))


# Custom select endpoint
@app.get("/city/{name}/temperatures", response_model=List[CityTemperatures])
def city_temperatures(name: str):
    with Transaction(DB):
        city = schema.get("city")
        select = city.select(
            "name",
            "temperatures.value"
        ).aliases("name", "temperature")
        return list(select.where("(= name {})").to_dict(name))


# Rely on select to generate response model
avg_select = schema.get("temperature").select(
    "city.name",
    "(avg value)",
).aliases("city", "avg")
avg_class = avg_select.to_dataclass()


@app.get("/avg/{name}/temperatures", response_model=List[avg_class])
def avg_temperature(name: str):
    with Transaction(DB) as trn:
        return list(
            avg_select
            .clone(trn=trn)
            .where("(= city.name {})")
            .to_dict(name)
        )


# Init creates db tables and automate the creation of GET endpoint for
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
        city.upsert("name" ,"lat", "long").executemany([
            ("Brussels", "50.85045", "4.34878"),
            ("London", "51.51279", "-0.09184"),
            ("Berlin", "52.52437", "13.41053"),
        ])

        temp = schema.get("temperature")
        temp.upsert("city.name", "timestamp", "value").executemany([
            ("Brussels", "2000-01-01T00:00", 0),
            ("London", "2000-01-01T00:00", 1),
            ("Berlin", "2000-01-01T00:00", 3),
            ("Berlin", "2000-01-01T01:00", 2),
        ])
