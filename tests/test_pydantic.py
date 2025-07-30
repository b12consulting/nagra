from typing import Optional
from datetime import datetime, date

from pydantic import BaseModel


def test_base_select(person):
    select = person.select("id", "name")
    model = select.to_pydantic(model_name="Person")

    class Person(BaseModel):
        id: int
        name: str

    assert model.model_json_schema() == Person.model_json_schema()


# def test_base_select_array(parameter):
#     select = parameter.select("name", "timestamps", "values")
#     model = select.to_pydantic(model_name="Parameter")
#     class Parameter(BaseModel):
#         name: str
#         timestamps: list[datetime]
#         values: list[float]
#     assert model.model_json_schema() == Parameter.model_json_schema()


def test_select_with_fk(person):
    select = person.select("id", "parent.name")
    model = select.to_pydantic(model_name="Person")

    class Person(BaseModel):
        id: int
        parent_name: Optional[str] = None

    assert  model.model_json_schema() == Person.model_json_schema()

    # Double fk
    select = person.select("id", "parent.parent.name")
    model = select.to_pydantic(model_name="Person")

    class Person(BaseModel):
        id: int
        parent_parent_name: Optional[str] = None

    assert model.model_json_schema() == Person.model_json_schema()


def test_kitchensink(kitchensink):
    select = kitchensink.select()
    aliases = kitchensink.columns
    model = select.to_pydantic(*aliases, model_name="KitchenSink")

    class KitchenSink(BaseModel):
        varchar: str
        bigint: Optional[int] = None
        float: Optional[float]
        int: Optional[int]
        timestamp: Optional[datetime] = None
        timestamptz: Optional[datetime] = None
        bool: Optional[bool] = None
        date: Optional[date]
        json: Optional[dict | list]  = None
        uuid: Optional[str] = None
        max: Optional[str] = None
        true: Optional[str] = None
        blob: Optional[bytes]

    # TODO loop on KitchenSink.model_fields and validate annotation and types

