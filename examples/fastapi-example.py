"""
Run me with: uvicorn fastapi-example:app
"""

from pathlib import Path
from typing import List

from nagra import Transaction, Schema
from fastapi import FastAPI, Depends

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


app = FastAPI()
DB = "sqlite://example.db"
here = Path(__file__).parent
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
