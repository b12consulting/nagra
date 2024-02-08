from dataclasses import fields, dataclass
from datetime import datetime


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
    class person:
        id: int
        name: str

    assert equivalent_classes(dclass, person)


def test_select_with_fk(person):
    select = person.select("id", "parent.name")
    dclass = select.to_dataclass()

    @dataclass
    class Person:
        id: int
        parent_name: str

    assert equivalent_classes(dclass, Person)

    # Double fk
    select = person.select("id", "parent.parent.name")
    dclass = select.to_dataclass()

    @dataclass
    class Person:
        id: int
        parent_parent_name: str

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
        bool_like: bool
        float_like: float
        int_like: int
    assert equivalent_classes(dclass, Expected)


def test_kitchensink(kitchensink):
    select = kitchensink.select()
    dclass = select.to_dataclass()

    @dataclass
    class KitchenSink:
        varchar: str
        bigint: int
        float: float
        int: int
        timestamp: datetime
        bool: bool

    assert equivalent_classes(dclass, KitchenSink)


def test_aggregates(kitchensink):
    select = kitchensink.select(
        "(min varchar)",
        "(sum bigint)",
        "(avg float)",
        "(max int)",
        "(max timestamp)",
        "(count)",
        "(every bool)"
    )
    dclass = select.to_dataclass(
        "varchar",
        "bigint",
        "float",
        "int",
        "timestamp",
        "count",
        "bool",
    )

    @dataclass
    class KitchenSink:
        varchar: str
        bigint: int
        float: float
        int: int
        timestamp: datetime
        count: int
        bool: bool

    assert equivalent_classes(dclass, KitchenSink)
