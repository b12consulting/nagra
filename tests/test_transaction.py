import pytest

from nagra.transaction import dummy_transaction, Transaction
from nagra.exceptions import NoActiveTransaction, TransactionReenterError
from nagra.schema import Schema


POSTGRESQL_DSN = "postgresql:///nagra"


def test_dummy_transaction():
    with pytest.raises(NoActiveTransaction):
        dummy_transaction.execute("SELECT 1")

    with pytest.raises(NoActiveTransaction):
        dummy_transaction.executemany("SELECT 1")


def test_transaction_reuse():
    with pytest.raises(TransactionReenterError):
        trn = Transaction(POSTGRESQL_DSN)
        with trn:
            with trn:
                pass


def test_concurrent_transaction(person, schema: Schema):
    with Transaction(POSTGRESQL_DSN):
        schema.create_tables()
        # Cleanup
        person.delete()

    trn_a = Transaction(POSTGRESQL_DSN)
    trn_b = Transaction(POSTGRESQL_DSN)

    person.upsert("name", trn=trn_a).execute("Romeo")
    person.upsert("name", trn=trn_b).execute("Sierra")

    trn_a.commit()
    trn_b.rollback()

    with Transaction(POSTGRESQL_DSN) as tr:
        records = list(person.select("name"))
        assert records == [("Romeo",)]

        # Cleanup
        for tbl in schema.tables.values():
            if tbl.is_view:
                continue
            tr.execute(f"DROP TABLE {tbl.name} CASCADE")


def test_postgresql_connection_pool_is_cached_by_dsn():
    Transaction.shutdown_pools()
    try:
        trn_a = Transaction(POSTGRESQL_DSN)
        trn_b = Transaction(POSTGRESQL_DSN)

        assert trn_a._pool is trn_b._pool
        assert Transaction._pool_cache[POSTGRESQL_DSN] is trn_a._pool
        assert trn_a._connection is None
        assert trn_b._connection is None
    finally:
        Transaction.shutdown_pools()


def test_postgresql_connection_is_borrowed_lazily_and_returned_on_close():
    Transaction.shutdown_pools()
    try:
        trn = Transaction(POSTGRESQL_DSN)
        assert trn._connection is None

        connection = trn.connection
        assert connection is not None
        assert trn.connection is connection

        pool = trn._pool
        trn.close()

        assert trn._connection is None

        reused_trn = Transaction(POSTGRESQL_DSN)
        assert reused_trn._pool is pool
        assert reused_trn.connection is connection
        reused_trn.close()
    finally:
        Transaction.shutdown_pools()


def test_postgresql_close_without_borrowed_connection_does_not_touch_pool():
    Transaction.shutdown_pools()
    try:
        trn = Transaction(POSTGRESQL_DSN)
        pool = trn._pool

        trn.close()

        assert trn._connection is None
        assert Transaction._pool_cache[POSTGRESQL_DSN] is pool
    finally:
        Transaction.shutdown_pools()


def test_shutdown_pools_closes_and_clears_postgresql_pools():
    Transaction.shutdown_pools()
    Transaction(POSTGRESQL_DSN)

    assert POSTGRESQL_DSN in Transaction._pool_cache

    Transaction.shutdown_pools()

    assert Transaction._pool_cache == {}
