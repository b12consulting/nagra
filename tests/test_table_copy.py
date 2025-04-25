import pytest
from psycopg.errors import UniqueViolation, ForeignKeyViolation


def test_simple_copy(transaction, person):
    if transaction.flavor != "postgresql":
        pytest.skip("COPY FROM not available for sqlite")

    # First copy
    records = [(1, "Big Bob", None), (2, "Bob", None)]
    person.copy_from(records)

    # Check table content
    assert len(list(person.select())) == 2

    # Second copy - must fail on unique index
    records = [(1, "Big Bob", None), (2, "Bob", None)]
    with pytest.raises(UniqueViolation):
        person.copy_from(records)


def test_incorrect_fk(transaction, person):
    if transaction.flavor != "postgresql":
        pytest.skip("COPY FROM not available for sqlite")
    # copy - must fail on fk
    records = [(1, "Trudy", 42)]
    with pytest.raises(ForeignKeyViolation):
        person.copy_from(records)
