from datetime import datetime, date
from uuid import UUID

import pytest
from pandas import DataFrame

from nagra import Transaction
from nagra.utils import strip_lines
from nagra.exceptions import UnresolvedFK, ValidationError


def test_simple_upsert_stm(person):
    upsert = person.upsert("name", "parent.name")
    assert list(upsert.resolve_stm) == ["parent"]
    res = list(strip_lines(upsert.resolve_stm["parent"]))
    assert res == [
        "SELECT",
        '"person"."id"',
        'FROM "person"',
        "WHERE",
        '"person"."name" = %s',
        ";",
    ]
    res = list(strip_lines(upsert.stm()))
    assert res == [
        'INSERT INTO "person" (name, parent)',
        "VALUES (",
        "%s,%s",
        ")",
        "ON CONFLICT (",
        '"name"',
        ")",
        "DO UPDATE SET",
        '"parent" = EXCLUDED."parent"',
        "RETURNING id",
    ]


def test_simple_upsert(transaction, person):
    # First upsert
    upsert = person.upsert("name")
    upsert.execute("Big Bob")
    (record,) = list(person.select("name").execute())
    assert record == ("Big Bob",)

    # Second one
    upsert = person.upsert("name", "parent.name")
    upsert.execute("Bob", "Big Bob")
    rows = list(person.select("name", "parent.name").execute())
    assert rows == [("Big Bob", None), ("Bob", "Big Bob")]


def test_insert(transaction, person):
    # First upsert
    upsert = person.upsert("name")
    records = [("Big Bob",), ("Bob",)]
    upsert.executemany(records)

    # Second one (with insert instead of upsert)
    upsert = person.insert("name", "parent.name")
    upsert.execute("Bob", "Big Bob")
    rows = list(person.select("name", "parent.name").execute())
    assert rows == [("Big Bob", None), ("Bob", None)]


def test_upsert_stmt_with_id(person):
    upsert = person.upsert("id", "name", "parent.name")
    res = list(strip_lines(upsert.stm()))
    assert res == [
        'INSERT INTO "person" (id, name, parent)',
        "VALUES (",
        "%s,%s,%s",
        ")",
        "ON CONFLICT (",
        '"id"',
        ")",
        "DO UPDATE SET",
        '"name" = EXCLUDED."name" , "parent" = EXCLUDED."parent"',
        "RETURNING id",
    ]


def test_upsert_exec_with_id(transaction, person):
    # Add parent
    upsert = person.upsert("id", "name")
    upsert.execute(1, "Big Bob")
    (rows,) = list(person.select("id", "name").execute())
    assert rows == (1, "Big Bob")

    # Add child
    upsert = person.upsert("id", "name", "parent.name")
    upsert.execute(2, "Bob", "Big Bob")
    rows = list(person.select("name", "parent.name").execute())
    assert rows == [("Big Bob", None), ("Bob", "Big Bob")]

    # Update child
    upsert = person.upsert("id", "name")
    upsert.execute(2, "BOB")
    cond = "(= id 2)"
    (rows,) = person.select("name").where(cond).execute()
    assert rows == ("BOB",)


def test_many_upsert(transaction, person):
    # First upsert
    upsert = person.upsert("name")
    records = [("Big Alice",), ("Big Bob",)]
    upsert.executemany(records)
    rows = list(person.select("name").execute())
    assert len(rows) == 2

    # Second upsert
    upsert = person.upsert("name", "parent.name")
    records = [
        (
            "Alice",
            "Big Alice",
        ),
        (
            "Bob",
            "Big Bob",
        ),
    ]
    upsert.executemany(records)

    rows = list(person.select("name", "parent.name").execute())
    assert len(rows) == 4


def test_dbl_fk_upsert(transaction, person):
    # GP
    upsert = person.upsert("name")
    records = [("GP Alice",), ("GP Bob",)]
    upsert.executemany(records)

    # Parents
    upsert = person.upsert("name", "parent.name")
    records = [
        (
            "P Alice",
            "GP Alice",
        ),
        (
            "P Bob",
            "GP Bob",
        ),
    ]
    upsert.executemany(records)

    # children
    upsert = person.upsert("name", "parent.parent.name")
    records = [
        (
            "Alice",
            "GP Alice",
        ),
        (
            "Bob",
            "GP Bob",
        ),
    ]
    upsert.executemany(records)

    select = (
        person.select(
            "name",
            "parent.name",
            "parent.parent.name",
        )
        .where("(not (is parent.parent.name null))")
        .orderby("name")
    )
    rows = list(select)
    assert rows == [
        ("Alice", "P Alice", "GP Alice"),
        ("Bob", "P Bob", "GP Bob"),
    ]


def test_missing_fk(transaction, person):
    # If pass None in parent.name, we get None back
    upsert = person.upsert("name", "parent.name")
    records = [("Big Alice", None), ("Big Bob", None)]
    upsert.executemany(records)

    rows = list(person.select("parent").execute())
    assert rows == [(None,), (None,)]

    # If given a non-existing name upsert raises UnresolvedFK exception
    upsert = person.upsert("name", "parent.name")
    records = [("Big Alice", "I do not exist")]
    with pytest.raises(UnresolvedFK):
        upsert.executemany(records)

    # If lenient is given a None is inserted
    for lenient in [True, ["parent"]]:
        upsert = person.upsert("name", "parent.name", lenient=lenient)
        records = [("Big Alice", "I do not exist")]
        upsert.executemany(records)
        rows = list(person.select("parent").where("(= name 'Big Alice')").execute())
        assert rows == [(None,)]


def test_return_ids(transaction, person):
    # Create an "on conflict update" upsert
    upsert = person.upsert("name", "parent.name")
    records = [("Big Alice", None), ("Big Bob", None)]
    insert_ids = upsert.executemany(records)
    update_ids = upsert.executemany(records)
    assert len(insert_ids) == 2
    assert insert_ids == update_ids
    assert insert_ids != [None, None]

    # Create an "on conflict do nothing" upsert
    upsert = person.upsert("name")
    records = [("Papa",), ("Quebec",)]
    insert_ids = upsert.executemany(records)
    assert insert_ids != [None, None]
    update_ids = upsert.executemany(records)
    assert update_ids == [None, None]


def test_from_pandas(transaction, kitchensink):
    df = DataFrame(
        {
            "varchar": ["ham"],
            "bigint": [1],
            "float": [1.0],
            "int": [1],
            "timestamp": ["1970-01-01 00:00:00"],
            "bool": [True],
            "date": ["1970-01-01"],
            "json": ["{}"],
            "uuid": ["F1172BD3-0A1D-422E-8ED6-8DC2D0F8C11C"],
        }
    )
    kitchensink.upsert().from_pandas(df)

    (row,) = kitchensink.select()
    if Transaction.current.flavor == "postgresql":
        assert row == (
            "ham",
            1,
            1.0,
            1,
            datetime(1970, 1, 1, 0, 0),
            True,
            date(1970, 1, 1),
            {},
            UUID("F1172BD3-0A1D-422E-8ED6-8DC2D0F8C11C"),
        )
    else:
        assert row == (
            "ham",
            1,
            1.0,
            1,
            "1970-01-01 00:00:00",
            1,
            "1970-01-01",
            "{}",
            "F1172BD3-0A1D-422E-8ED6-8DC2D0F8C11C",
        )


def test_double_insert(transaction, person):
    """
    Show that 'last write win' when duplicates are given
    """
    upsert = person.upsert("name")
    upsert.execute("Tango")

    upsert = person.upsert("name", "parent.name")
    upsert.executemany(
        [
            ("Charly", "Tango"),
            ("Charly", None),
        ]
    )
    rows = list(person.select())
    assert rows == [("Tango", None), ("Charly", None)]


def test_one2many_ref(transaction, person, org):
    person.upsert("name").execute("Charly")
    person.upsert("name").execute("Juliet")
    org.upsert("name", "person.name").execute("Alpha", "Charly")
    org.upsert("name", "person.name").execute("Bravo", "Juliet")

    # update parent based on org
    upsert = person.upsert("name", "parent.orgs.name")
    upsert.execute("Juliet", "Alpha")

    # Check results
    rows = list(person.select().where("(= name 'Juliet')"))
    assert rows == [("Juliet", "Charly")]


def test_where_cond(transaction, person):
    """
    Shows that an exception is raised when a row infrige a where condition
    """
    upsert = person.upsert("name")
    upsert.execute("Tango")

    cond = "(!= name parent.name)"  # Forbid self-reference
    upsert = person.upsert("name", "parent.name").where(cond)
    with pytest.raises(ValidationError):
        upsert.execute("Tango", "Tango")


def test_default_value(transaction, org):
    """
    Shows that default values are applied on row creation
    """
    upsert = org.upsert("name")
    upsert.execute("Lima")

    (record,) = org.select("name", "status")
    name, status = record
    assert (name, status) == ("Lima", "OK")
