from  pathlib import Path

import pytest

from nagra import Table, Schema


HERE = Path(__file__).parent
def test_toml_loader():

    # With a Path
    src = HERE / "assets" / "sample_schema.toml"
    test_schema = Schema.from_toml(src)
    table = Table.get("user", schema=test_schema)
    assert table is not None

    # with a string
    test_schema = Schema.from_toml(src.open().read())
    table = Table.get("user", schema=test_schema)
    assert table is not None

    # with an io base
    test_schema = Schema.from_toml(src.open())
    table = Table.get("user", schema=test_schema)
    assert table is not None

    # Must fail when a duplicate table is added
    with pytest.raises(RuntimeError):
        Table("user", columns=["name"], natural_key=["name"], schema=test_schema)

    # Test reset
    test_schema.reset()
    assert test_schema.tables == {}


def test_setup():
    pass # TODO test generated sql


def test_create_tables(empty_transaction):
    # Associate schema with the transaction
    schema = Schema.default

    # Make sure we start from empty db
    assert not schema._db_columns(trn=empty_transaction)
    schema.create_tables(trn=empty_transaction)
    post = schema._db_columns(trn=empty_transaction)

    # Test person table is properly created
    assert "person" in post
    assert sorted(post["person"]) ==  ["id", "name", "parent"]

    # Add a column to existing table
    person = Table.get("person")
    person.columns["email"] = "varchar"
    schema.create_tables(trn=empty_transaction)
    post = schema._db_columns(trn=empty_transaction)
    assert "person" in post
    assert sorted(post["person"]) ==  ["email", "id", "name", "parent"]

    # Needed to not polute other tests
    person.columns.pop("email")
