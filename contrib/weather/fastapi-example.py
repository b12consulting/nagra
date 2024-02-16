"""
Usage: start this script first, it will populate a db:
"sqlite://fastapi_example.sqlite".

You can then run the api like this:

    uvicorn fastapi_example:app

"""

from pathlib import Path
from typing import List

from nagra import Transaction, load_schema, Schema
from fastapi import FastAPI


app = FastAPI()
DB = "sqlite://weather.db"
here = Path(__file__).parent

def endpoint(app, name, select):
    dclass = select.to_dataclass()
    @app.get(f"/{name}", response_model=List[dclass])
    def getter():
        with Transaction(DB):
            records = select.to_dict()
            print(records)
            return records


def init():
    with Transaction(DB):
        load_schema(here / "weather_schema.toml")

    for name, table in Schema.default.tables.items():
        select = table.select()
        endpoint(app, name, select)


@app.get("/")
async def root():
    return {"message": "Hello World"}


init()
