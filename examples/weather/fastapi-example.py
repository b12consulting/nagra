"""
First run the etl to populate the example db

    python weather-etl.py city
    python weather-etl.py city

You can then run the api like this:

    uvicorn fastapi-example:app
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
            return records


# Init automate the creation of GET endpoint for every table
def init():
    with Transaction(DB):
        load_schema(here / "weather_schema.toml")

    for name, table in Schema.default.tables.items():
        select = table.select()
        endpoint(app, name, select)

# Custom endpoint
@app.get("/")
async def root():
    return {"message": "Hello World"}


init()
