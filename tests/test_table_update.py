import zoneinfo
from datetime import datetime, date
from uuid import UUID

import pytest
from pandas import DataFrame, to_datetime

from nagra.exceptions import UnresolvedFK, ValidationError


def test_simple_update_by_id(transaction, person):
    # First upsert some values
    upsert = person.upsert("name")
    new_id = upsert.execute("Bob")

    # Update by id
    update = person.update("id", "name")
    new_id_copy = update.execute(new_id, "BOB")
    assert new_id_copy == new_id

    # Test update is successful
    (row,) = person.select("id", "name")
    assert row == (new_id, "BOB")


def test_simple_update_by_nk(transaction, temperature):
    # First upsert some values
    upsert = temperature.upsert("timestamp", "city", "value")
    new_id = upsert.execute("2024-06-27 17:52:00", "Brussels", 27)

    # Update by nk
    update = temperature.update("timestamp", "city", "value")
    new_id_copy = update.execute("2024-06-27 17:52:00", "Brussels", 28)
    assert new_id_copy == new_id

    # Test update is successful
    (row,) = temperature.select("timestamp", "city", "value")
    ts = "2024-06-27 17:52:00"
    if transaction.flavor != "sqlite":
        ts = datetime.fromisoformat(ts)
    assert row == (ts, "Brussels", 28)


def test_return_ids(transaction, person):
    # Add lines
    upsert = person.upsert("name", "parent.name")
    records = [("Alice", None), ("Bob", None), ("Charlie", None)]
    insert_ids = upsert.executemany(records)

    # Fisrt updates - by nk
    update = person.update("name", "parent")
    pid = insert_ids[0]
    records = [("Bob", pid), ("Charlie", pid)]
    update_ids = update.executemany(records)
    assert update_ids == insert_ids[1:]

    # Second update - by id
    update = person.update("name", "id")
    a, b, c = insert_ids
    records = [("BOB", b), ("CHARLIE", c)]
    update_ids = update.executemany(records)
    assert update_ids == insert_ids[1:]

    # Check consistency
    rows = list(person.select("name").where(f"(= parent {a}").orderby("id"))
    assert rows == [("BOB",), ("CHARLIE",)]


def test_from_pandas(transaction, kitchensink):
    # First insert
    df = DataFrame(
        {
            "varchar": ["ham"],
            "bigint": [1],
            "float": [1.0],
            "int": [1],
            "timestamp": to_datetime(["1970-01-01 00:00:00"]),
            "timestamptz": to_datetime(["1970-01-01 00:00:00+00:00"]),
            "bool": [True],
            "date": ["1970-01-01"],
            "json": [{}],
            "uuid": ["F1172BD3-0A1D-422E-8ED6-8DC2D0F8C11C"],
            "max": ["max"],
            "true": ["true"],
            "blob": [b"blob"],
        }
    )
    ids = kitchensink.upsert().from_pandas(df)

    # Update values
    df = DataFrame(
        {
            "id": ids,
            "varchar": ["HAM"],
            "bigint": [2],
            "float": [2.0],
            "int": [2],
            "timestamp": to_datetime(["1970-01-02 00:00:00"]),
            "timestamptz": to_datetime(["1970-01-02 00:00:00+00:00"]),
            "bool": [False],
            "date": ["1970-01-02"],
            "json": '[{"foo": "bar"}]',
            "uuid": ["F1172BD3-0A1D-422E-8ED6-8DC2D0F8C11D"],
            "max": ["max"],
            "true": ["true"],
        }
    )
    kitchensink.update(*df.columns).from_pandas(df)

    (row,) = kitchensink.select()
    BRUTZ = zoneinfo.ZoneInfo(key="Europe/Brussels")
    if transaction.flavor == "postgresql":
        assert row == (
            "HAM",
            2,
            2.0,
            2,
            datetime(1970, 1, 2, 0, 0),
            datetime(1970, 1, 2, 1, 0, tzinfo=BRUTZ),
            False,
            date(1970, 1, 2),
            [{"foo": "bar"}],
            UUID("F1172BD3-0A1D-422E-8ED6-8DC2D0F8C11D"),
            "max",
            "true",
            b"blob",
        )
    else:
        assert row == (
            "HAM",
            2,
            2.0,
            2,
            "1970-01-02",
            "1970-01-02 00:00:00+00:00",
            0,
            "1970-01-02",
            '[{"foo": "bar"}]',
            "F1172BD3-0A1D-422E-8ED6-8DC2D0F8C11D",
            "max",
            "true",
            "blob",
        )


def test_where_cond(transaction, person):
    """
    Shows that an exception is raised when a row infrige a check condition
    """
    upsert = person.upsert("name")
    upsert.execute("Tango")

    cond = "(!= name parent.name)"  # Forbid self-reference
    upsert = person.update("name", "parent.name").check(cond)
    with pytest.raises(ValidationError):
        upsert.execute("Tango", "Tango")


def test_missing_fk(transaction, person):
    # If pass None in parent.name, we get None back
    upsert = person.upsert("name", "parent.name")
    records = [("Big Alice", None), ("Big Bob", None)]
    upsert.executemany(records)

    # If given a non-existing name update raises UnresolvedFK exception
    update = person.update("name", "parent.name")
    records = [("Big Alice", "I do not exist")]
    with pytest.raises(UnresolvedFK):
        update.executemany(records)

    # If lenient is given a None is inserted
    for lenient in [True, ["parent"]]:
        update = person.update("name", "parent.name", lenient=lenient)
        records = [("Big Alice", "I do not exist")]
        update.executemany(records)
        rows = list(person.select("parent").where("(= name 'Big Alice')").execute())
        assert rows == [(None,)]
