from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional, get_args

from nagra.select import clean_col


def equivalent_classes(A, B):
    # Quickwin !
    if A == B:
        return True

    A_fields = vars(A)["__annotations__"]
    B_fields = vars(B)["__annotations__"]

    for A_name, B_name in zip(A_fields, B_fields):
        # Compare field names
        if A_name != B_name:
            breakpoint()
            return False

        # Compare field types
        A_type = A_fields[A_name]
        B_type = B_fields[B_name]
        if "__annotations__" in dir(A_type):
            return equivalent_classes(A_type, B_type)
        # We recurse on union types
        if get_args(A_type):
            for a_subtype, b_subtype in zip(get_args(A_type), get_args(B_type)):
                if not equivalent_classes(a_subtype, b_subtype):
                    return False
        elif A_type != B_type:
            breakpoint()
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
        bool_like: bool
        float_like: float
        int_like: int

    assert equivalent_classes(dclass, Expected)


def test_kitchensink(kitchensink):
    select = kitchensink.select()
    aliases = kitchensink.columns
    dclass = select.to_dataclass(*aliases)

    @dataclass
    class KitchenSink:
        varchar: str
        bigint: Optional[int]
        float: Optional[float]
        int: Optional[int]
        timestamp: Optional[datetime]
        timestamptz: Optional[datetime]
        bool: Optional[bool]
        date: Optional[date]
        json: Optional[list | dict]
        uuid: Optional[str]
        max: Optional[str]
        true: Optional[str]
        blob: Optional[bytes]

    assert equivalent_classes(dclass, KitchenSink)


def test_aggregates(kitchensink):
    select = kitchensink.select(
        "(min varchar)",
        "(sum bigint)",
        "(avg float)",
        "(max int)",
        "(max timestamp)",
        "(date_bin '5 days' timestamptz)",
        "(now)",
        "(current_date)",
        "(date_part timestamp)",
        "(isfinite timestamp)",
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
        "timestamptz",
        "now",
        "current_date",
        "date_part",
        "isfinite",
        "count",
        "bool",
        "date",
    )

    @dataclass
    class KitchenSink:
        varchar: Optional[str]  # Aggregates can be null
        bigint: Optional[int]
        float: Optional[float]
        int: Optional[int]
        timestamp: Optional[datetime]
        timestamptz: Optional[datetime]
        now: datetime
        current_date: date
        date_part: Optional[float]
        isfinite: Optional[bool]
        count: Optional[int]
        bool: Optional[bool]
        date: Optional[date]

    assert equivalent_classes(dclass, KitchenSink)


def test_clean_col():
    assert clean_col("name") == "name"
    assert clean_col("table.name") == "table_name"
    assert clean_col("(= col 1)") == "___col_1_"


def test_nested_dataclasses(person):
    select = person.select("id", "parent.name")
    dclass = select.to_dataclass(nest=True)

    @dataclass
    class Parent:
        name: str

    @dataclass
    class Person:
        id: int
        parent: Parent | None

    assert equivalent_classes(dclass, Person)

    # Multiple fields for a fk
    select = person.select("id", "parent.name", "parent.id")
    dclass = select.to_dataclass(nest=True)

    @dataclass
    class Parent:
        name: str
        id: int

    @dataclass
    class Person:
        id: int
        parent: Parent | None

    assert equivalent_classes(dclass, Person)
