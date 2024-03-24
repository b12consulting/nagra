from  pathlib import Path

import pytest

from nagra import load_schema, Table, Schema


HERE = Path(__file__).parent
def test_toml_loader():
    test_schema = Schema()

    # With a path
    src = HERE / "assets" / "sample_schema.toml"
    load_schema(src.open(), schema=test_schema)
    table = Table.get("user", schema=test_schema)
    assert table is not None

    # with a string
    load_schema(src.open().read(), reset=True, schema=test_schema)
    table = Table.get("user", schema=test_schema)
    assert table is not None

    # with an io base
    load_schema(open(src), reset=True, schema=test_schema)
    table = Table.get("user", schema=test_schema)
    assert table is not None

    # Must fail on double loading (without reset)
    with pytest.raises(RuntimeError):
        load_schema(open(src), reset=False, schema=test_schema)

    # Reset
    test_schema.reset()
    assert test_schema.tables == {}


def test_setup():
    pass # TODO test generated sql


def test_create_tables(empty_transaction):
    # Make sure we start from empty db
    assert not Schema.default._db_columns()
    Schema.default.setup()
    post = Schema.default._db_columns()
    # Test person table is properly created
    assert "person" in post
    assert sorted(post["person"]) ==  ["id", "name", "parent"]

    # Add a column to existing table
    person = Table.get("person")
    person.columns["email"] = "varchar"
    Schema.default.setup()
    post = Schema.default._db_columns()
    assert "person" in post
    assert sorted(post["person"]) ==  ["email", "id", "name", "parent"]

    # Needed to not polute other tests
    person.columns.pop("email")

