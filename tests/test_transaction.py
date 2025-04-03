import pytest

from nagra.transaction import dummy_transaction, Transaction
from nagra.exceptions import NoActiveTransaction
from nagra.schema import Schema


def test_dummy_transaction():

    with pytest.raises(NoActiveTransaction):
        dummy_transaction.execute("SELECT 1")

    with pytest.raises(NoActiveTransaction):
        dummy_transaction.executemany("SELECT 1")


def test_concurrent_transaction(person):
    uri = "postgresql:///nagra"

    with Transaction(uri):
        Schema.default.create_tables()
        # Cleanup
        person.delete()

    trn_a = Transaction(uri)
    trn_b = Transaction(uri)

    person.upsert("name", trn=trn_a).execute("Romeo")
    person.upsert("name", trn=trn_b).execute("Sierra")

    trn_a.commit()
    trn_b.rollback()

    with Transaction(uri) as tr:
        records = list(person.select("name"))
        assert records == [("Romeo",)]

        # Cleanup
        for tbl in Schema.default.tables.values():
            if tbl.is_view:
                continue
            tr.execute(f"DROP TABLE {tbl.name} CASCADE")
