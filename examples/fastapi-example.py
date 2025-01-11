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

from typing import List

from nagra import Transaction, Schema
from fastapi import FastAPI, Depends

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
sch = Schema.from_toml(schema_toml)


def trn():
    transaction = Transaction(DB)
    try:
        yield transaction
    finally:
        transaction.commit()


def endpoint(app, name, select):
    dclass = select.to_dataclass()
    @app.get(f"/{name}", response_model=List[dclass])
    def getter(trn: Transaction = Depends(trn)):
        records = select.clone(trn).to_dict()
        return records


# Init create db tables and automate the creation of GET endpoint for
# every table
def init():
    with Transaction(DB):
        sch.create_tables()

    for name, table in sch.tables.items():
        select = table.select()
        endpoint(app, name, select)

# Custom endpoint
@app.get("/")
async def root():
    return {"message": "Hello World"}

init()


if __name__ == "__main__":
    print("LOAD DATA")

    with Transaction(DB):
        sch.create_tables()
        city = sch.get("city")
        city.upsert("name").executemany([
            ("Brussels",),
            ("London",),
            ("Berlin",),
        ])

        temp = sch.get("temperature")
        temp.upsert("city.name", "timestamp", "value").executemany([
            ("Brussels", "2000-01-01T00:00", 0),
            ("London", "2000-01-01T00:00", 1),
            ("Berlin", "2000-01-01T00:00", 3),
            ("Berlin", "2000-01-01T01:00", 2),
        ])
