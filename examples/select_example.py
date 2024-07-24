"""
The Select class is also able to return dataclass definition and
pandas dataframes. This example also shows how dataclass can be
combined with pydantic
"""

from dataclasses import fields

from pydantic import TypeAdapter
from nagra import Schema, Transaction


schema_toml = """
[person]
natural_key = ["name"]
[person.columns]
name = "varchar"
birthday = "date"
height = "int"
"""

with Transaction("sqlite://"):
    schema = Schema.from_toml(schema_toml)
    schema.create_tables()

    # Add person
    person = schema.get("person")
    record = ("Hugo","2000-01-01", "180")
    person.upsert("name", "birthday", "height").execute(*record)

    # Create select
    select = person.select()
    dclass = select.to_dataclass()

    print(dclass)
    # -> <class 'types.person'>

    for field in fields(dclass):
        print(field.name, field.type)
    # ->
    # name <class 'str'>
    # birthday typing.Optional[datetime.date]
    # height typing.Optional[int]
    
    record, = select.to_dict()
    print(record)
    # -> {'name': 'Hugo', 'birthday': '2000-01-01', 'height': 180}

    print(select.to_pandas())
    # ->
    #      name   birthday  height
    # 0  Hugo 2000-01-01     180


    adapter = TypeAdapter(dclass)
    obj = adapter.validate_python(record)
    print(obj)
    # ->
    # person(name='Hugo', birthday=datetime.date(2000, 1, 1), height=180)
