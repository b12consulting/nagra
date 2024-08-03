from dataclasses import fields, dataclass
from datetime import datetime, date
from typing import Optional


def equivalent_classes(A, B):
    A_fields = fields(A)
    B_fields = fields(B)
    if not len(A_fields) == len(B_fields):
        return False

    for A_field, B_field in zip(A_fields, B_fields):
        if A_field.name != B_field.name:
            return False
        if A_field.type != B_field.type:
            return False
    return True


def test_base_select(person):
    select = person.select("id", "name")
    dclass = select.to_dataclass()

    @dataclass
    class Person:
        id: int
        name: str

    assert equivalent_classes(dclass, Person)


def test_base_select_array(parameter):
    select = parameter.select("name", "timestamps", "values")
    dclass = select.to_dataclass()

    @dataclass
    class Parameter:
        name: str
        timestamps: list[datetime] | None
        values: list[float] | None

    assert equivalent_classes(dclass, Parameter)


def test_select_with_fk(person):
    select = person.select("id", "parent.name")
    dclass = select.to_dataclass()

    @dataclass
    class Person:
        id: int
        parent_name: Optional[str]

    assert equivalent_classes(dclass, Person)

    # Double fk
    select = person.select("id", "parent.parent.name")
    dclass = select.to_dataclass()

    @dataclass
    class Person:
        id: int
        parent_parent_name: Optional[str]

    assert equivalent_classes(dclass, Person)


def test_select_with_sexp(person):
    select = person.select(
        "name",
        "(= name 'spam')",
        "(+ 1.0 1.0)",
        "(+ 2 2)",
    )
    dclass = select.to_dataclass("str_like", "bool_like", "float_like", "int_like")

    @dataclass
    class Expected:
        str_like: str
        bool_like: Optional[bool]
        float_like: Optional[float]
        int_like: Optional[int]

    assert equivalent_classes(dclass, Expected)


def test_kitchensink(kitchensink):
    select = kitchensink.select()
    dclass = select.to_dataclass()

    @dataclass
    class KitchenSink:
        varchar: str
        bigint: Optional[int]
        float: Optional[float]
        int: Optional[int]
        timestamp: Optional[datetime]
        bool: Optional[bool]
        date: Optional[date]
        json: Optional[dict | list]
        uuid: Optional[str]

    assert equivalent_classes(dclass, KitchenSink)


def test_aggregates(kitchensink):
    select = kitchensink.select(
        "(min varchar)",
        "(sum bigint)",
        "(avg float)",
        "(max int)",
        "(max timestamp)",
        "(count)",
        "(every bool)",
        "(max date)",
    )
    dclass = select.to_dataclass(
        "varchar",
        "bigint",
        "float",
        "int",
        "timestamp",
        "count",
        "bool",
        "date",
    )

    @dataclass
    class KitchenSink:
        varchar: Optional[str]  # FIXME should no be nullable
        bigint: Optional[int]
        float: Optional[float]
        int: Optional[int]
        timestamp: Optional[datetime]
        count: Optional[int]
        bool: Optional[bool]
        date: Optional[date]

    assert equivalent_classes(dclass, KitchenSink)
