from datetime import datetime

import pytest

from nagra.utils import strip_lines
from nagra.exceptions import UnresolvedFK, ValidationError


def test_simple_upsert_stm(person):
    upsert = person.upsert("name", "parent.name")
    assert list(upsert.resolve_stm) == ["parent"]
    res = strip_lines(upsert.resolve_stm["parent"])
    assert res == [
        "SELECT",
        '"person"."id"',
        'FROM "person"',
        "WHERE",
        '"person"."name" = %s',
        ";",
    ]
    res = strip_lines(upsert.stm())
    assert res == [
        'INSERT INTO "person" ("name", "parent")',
        "VALUES (",
        "%s,%s",
        ")",
        "ON CONFLICT (",
        '"name"',
        ")",
        "DO UPDATE SET",
        '"parent" = EXCLUDED."parent"',
        'RETURNING "id"',
    ]


def test_simple_upsert(cacheable_transaction, person):
    # First upsert
    upsert = person.upsert("name")
    upsert.execute("Big Bob")
    (record,) = list(person.select("name").execute())
    assert record == ("Big Bob",)

    # Second one
    upsert = person.upsert("name", "parent.name")
    upsert.execute("Bob", "Big Bob")
    rows = list(person.select("name", "parent.name").orderby("name").execute())
    assert rows == [("Big Bob", None), ("Bob", "Big Bob")]

    # Empty input
    upsert = person.upsert("name", "parent.name")
    upsert.execute()
    upsert.executemany([])
    rows = list(person.select("name", "parent.name").orderby("name").execute())
    assert rows == [("Big Bob", None), ("Bob", "Big Bob")]


def test_insert(cacheable_transaction, person):
    # First upsert
    upsert = person.upsert("name")
    records = [("Big Bob",), ("Bob",)]
    upsert.executemany(records)

    # Second one (with insert instead of upsert)
    upsert = person.insert("name", "parent.name")
    upsert.execute("Bob", "Big Bob")
    rows = list(person.select("name", "parent.name").orderby("name").execute())
    assert rows == [("Big Bob", None), ("Bob", None)]


def test_upsert_stmt_with_id(cacheable_transaction, person):
    if cacheable_transaction.flavor == "postgresql":
        # Test stmt with all columns
        upsert = person.upsert("id", "name", "parent.name")
        res = list(strip_lines(upsert.stm()))
        assert res == [
            'INSERT INTO "person" ("id", "name", "parent")',
            "VALUES (",
            "%s,%s,%s",
            ")",
            "ON CONFLICT (",
            '"id"',
            ")",
            "DO UPDATE SET",
            '"name" = EXCLUDED."name" , "parent" = EXCLUDED."parent"',
            'RETURNING "id"',
        ]

        # Test stmt with one columns
        upsert = person.upsert("id", "name")
        res = list(strip_lines(upsert.stm()))
        assert res == [
            'INSERT INTO "person" ("id", "name")',
            "VALUES (",
            "%s,%s",
            ")",
            "ON CONFLICT (",
            '"id"',
            ")",
            "DO UPDATE SET",
            '"name" = EXCLUDED."name"',
            'RETURNING "id"',
        ]

    # Insert & update on db
    upsert = person.upsert("name")
    new_id = upsert.execute("Lima")

    upsert = person.upsert("id", "name")
    upsert.execute(new_id, "Lima2")
    (record,) = person.select("id", "name").where("(= id {})").execute(new_id)
    assert record == (new_id, "Lima2")


def test_upsert_exec_with_id(cacheable_transaction, person):
    # Add parent
    upsert = person.upsert("id", "name")
    upsert.execute(1, "Big Bob")
    (rows,) = list(person.select("id", "name").execute())
    assert rows == (1, "Big Bob")

    # Add child
    upsert = person.upsert("id", "name", "parent.name")
    upsert.execute(2, "Bob", "Big Bob")
    rows = list(person.select("name", "parent.name").orderby("name").execute())
    assert rows == [("Big Bob", None), ("Bob", "Big Bob")]

    # Update child
    upsert = person.upsert("id", "name")
    upsert.execute(2, "BOB")
    cond = "(= id 2)"
    (rows,) = person.select("name").where(cond).execute()
    assert rows == ("BOB",)


def test_many_upsert(cacheable_transaction, person):
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


def test_dbl_fk_upsert(cacheable_transaction, person):
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


def test_missing_fk(cacheable_transaction, person):
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


def test_resolve(cacheable_transaction, person):
    # Prepare table
    upsert = person.upsert("name", "parent.name")
    records = [("Big Alice", None), ("Big Bob", None)]
    upsert.executemany(records)

    # Direct call to resolve, without upserting anything
    values = ["Big Alice", "Big Bob"]
    ids = list(upsert.resolve("parent", values))
    assert ids == [1, 2]


def test_return_ids(cacheable_transaction, person):
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


def test_double_insert(cacheable_transaction, person):
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
    rows = list(person.select().orderby(("name", "desc")))
    assert rows == [("Tango", None), ("Charly", None)]


def test_one2many_ref(cacheable_transaction, person, org):
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


def test_where_cond(cacheable_transaction, person):
    """
    Shows that an exception is raised when a row infrige a check condition
    """
    upsert = person.upsert("name")
    upsert.execute("Tango")

    cond = "(!= name parent.name)"  # Forbid self-reference
    upsert = person.upsert("name", "parent.name").check(cond)
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


def test_mixed_cursor(cacheable_transaction, person):
    # First upsert
    upsert = person.upsert("name")
    records = [("Romeo",), ("Sierra",), ("Tango",)]
    upsert.executemany(records)

    # add Tango as parent to other record, execute one stm for each
    select = person.select("name").where("(!= name 'Tango')")
    upsert = person.upsert("name", "parent.name")
    for (name,) in select:
        # In this loop we have two "live" cursor, the one consumed by
        # the select and the one executed by the upsert
        upsert.execute(name, "Tango")

    # assert results
    select = person.select("name", "parent.name").where("(!= name 'Tango')")
    for name, parent in select:
        assert name in ["Romeo", "Sierra"]
        assert parent == "Tango"


def test_arrays(cacheable_transaction, parameter):
    # First upsert
    upsert = parameter.upsert()
    records = [
        ("one", ["2024-08-03T10:42", "2024-08-03T10:43"], [2, 3]),
        ("two", ["2024-08-03T10:44", "2024-08-03T10:45"], [4, 5]),
        ("three", ["2024-08-03T10:46", "2024-08-03T10:47"], [6, 7]),
    ]
    if cacheable_transaction.flavor == "sqlite":
        records = [[str(v) for v in r] for r in records]
    upsert.executemany(records)

    records = list(parameter.select().orderby("id"))

    if cacheable_transaction.flavor == "sqlite":
        assert records == [
            ("one", "['2024-08-03T10:42', '2024-08-03T10:43']", "[2, 3]"),
            ("two", "['2024-08-03T10:44', '2024-08-03T10:45']", "[4, 5]"),
            ("three", "['2024-08-03T10:46', '2024-08-03T10:47']", "[6, 7]"),
        ]
    else:
        assert records == [
            (
                "one",
                [datetime(2024, 8, 3, 10, 42), datetime(2024, 8, 3, 10, 43)],
                [2.0, 3.0],
            ),
            (
                "two",
                [datetime(2024, 8, 3, 10, 44), datetime(2024, 8, 3, 10, 45)],
                [4.0, 5.0],
            ),
            (
                "three",
                [datetime(2024, 8, 3, 10, 46), datetime(2024, 8, 3, 10, 47)],
                [6.0, 7.0],
            ),
        ]


def test_from_dict(transaction, person):
    # Prepare two lines in table
    upsert = person.upsert("name", "parent.name")
    upsert.execute("Big Bob", None)
    upsert.execute("Bob", "Big Bob")
    records = list(person.select().to_dict())
    assert records == [
        {"name": "Big Bob", "parent_name": None},
        {"name": "Bob", "parent_name": "Big Bob"},
    ]

    # Upserting unchanged data is a noop
    person.upsert("name", "parent.name").from_dict(records)
    records_bis = list(person.select().to_dict())
    assert records_bis == records

    # Upserting will also support dotted notation
    new_records = [
        {"name": "Big Bob", "parent.name": None},
        {"name": "Bob", "parent.name": "Big Bob"},
    ]
    person.upsert("name", "parent.name").from_dict(new_records)
    records_bis = list(person.select().to_dict())
    assert records_bis == records
