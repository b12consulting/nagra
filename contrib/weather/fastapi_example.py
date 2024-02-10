"""
Usage: start this script first, it will populate a db:
"sqlite://fastapi_example.sqlite".

You can then run the api like this:

    uvicorn fastapi_example:app

"""

from typing import List

from nagra import Table, Transaction, Schema
from fastapi import FastAPI


app = FastAPI()
DB = "sqlite://fastapi_example.sqlite"

person = Table(
    "person",
    columns={
        "name": "varchar",
        "parent": "int",
    },
    foreign_keys={
        "parent": "person",
    },
    natural_key=["name"],
)
person_select = person.select("name", "parent.name")
person_class = person_select.to_dataclass()


def init():
    print("init")
    Schema.default.setup()
    person.upsert("name").executemany([
        ("One",),
        ("Two",),
    ])
    person.upsert("name", "parent.name").executemany([
        ("Three", "One"),
        ("Four", "Two"),
    ])


@app.get("/person", response_model=List[person_class])
async def get_person():
    with Transaction(DB):
        return person_select.to_dict()


@app.get("/")
async def root():
    return {"message": "Hello World"}


if __name__ == "__main__":
    with Transaction(DB):
        init()
