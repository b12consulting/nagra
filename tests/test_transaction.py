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
        pool_key = Transaction._pool_key(POSTGRESQL_DSN)

        assert trn_a._pool is trn_b._pool
        assert Transaction._pool_cache[pool_key] is trn_a._pool
        assert trn_a._connection is None
        assert trn_b._connection is None
    finally:
        Transaction.shutdown_pools()


def test_postgresql_connection_pool_key_normalizes_query_param_order():
    dsn_a = f"{POSTGRESQL_DSN}?connect_timeout=3&application_name=nagra"
    dsn_b = f"{POSTGRESQL_DSN}?application_name=nagra&connect_timeout=3"

    Transaction.shutdown_pools()
    try:
        trn_a = Transaction(dsn_a)
        trn_b = Transaction(dsn_b)

        assert trn_a._pool is trn_b._pool
        assert len(Transaction._pool_cache) == 1
    finally:
        Transaction.shutdown_pools()


def test_postgresql_connection_pool_key_preserves_semantic_differences():
    dsn_a = f"{POSTGRESQL_DSN}?application_name=nagra"
    dsn_b = f"{POSTGRESQL_DSN}?application_name=other"

    Transaction.shutdown_pools()
    try:
        trn_a = Transaction(dsn_a)
        trn_b = Transaction(dsn_b)

        assert trn_a._pool is not trn_b._pool
        assert len(Transaction._pool_cache) == 2
    finally:
        Transaction.shutdown_pools()


def test_postgresql_connection_is_borrowed_lazily():
    Transaction.shutdown_pools()
    try:
        trn = Transaction(POSTGRESQL_DSN)
        assert trn._connection is None

        connection = trn.connection
        assert connection is not None
        assert trn.connection is connection

        trn.return_connection()
    finally:
        Transaction.shutdown_pools()


def test_postgresql_empty_context_does_not_borrow_connection():
    Transaction.shutdown_pools()
    try:
        trn = Transaction(POSTGRESQL_DSN)

        with trn:
            pass

        assert trn._connection is None
    finally:
        Transaction.shutdown_pools()


def test_sqlite_empty_context_does_not_open_connection(tmp_path):
    trn = Transaction(f"sqlite://{tmp_path / 'empty.db'}")

    with trn:
        pass

    assert trn._connection is None


def test_postgresql_close_does_not_return_connection():
    Transaction.shutdown_pools()
    try:
        trn = Transaction(POSTGRESQL_DSN)
        connection = trn.connection

        trn.close()

        assert trn._connection is connection

        trn.return_connection()
    finally:
        Transaction.shutdown_pools()


def test_postgresql_return_connection_returns_connection_to_pool():
    Transaction.shutdown_pools()
    try:
        trn = Transaction(POSTGRESQL_DSN)
        connection = trn.connection

        trn.return_connection()

        assert trn._connection is None

        reused_trn = Transaction(POSTGRESQL_DSN)
        assert reused_trn.connection is connection
        reused_trn.return_connection()
    finally:
        Transaction.shutdown_pools()


def test_postgresql_connection_is_returned_on_commit():
    Transaction.shutdown_pools()
    try:
        trn = Transaction(POSTGRESQL_DSN)
        connection = trn.connection

        trn.commit()

        assert trn._connection is None

        reused_trn = Transaction(POSTGRESQL_DSN)
        assert reused_trn.connection is connection
        reused_trn.return_connection()
    finally:
        Transaction.shutdown_pools()


def test_postgresql_connection_is_returned_on_rollback():
    Transaction.shutdown_pools()
    try:
        trn = Transaction(POSTGRESQL_DSN)
        connection = trn.connection

        trn.rollback()

        assert trn._connection is None

        reused_trn = Transaction(POSTGRESQL_DSN)
        assert reused_trn.connection is connection
        reused_trn.return_connection()
    finally:
        Transaction.shutdown_pools()


def test_postgresql_connection_is_returned_when_context_commit_raises(monkeypatch):
    Transaction.shutdown_pools()
    try:
        trn = Transaction(POSTGRESQL_DSN)

        def broken_commit():
            raise RuntimeError("commit failed")

        with pytest.raises(RuntimeError, match="commit failed"):
            with trn:
                connection = trn.connection
                monkeypatch.setattr(trn, "commit", broken_commit)

        assert trn._connection is None

        reused_trn = Transaction(POSTGRESQL_DSN)
        assert reused_trn.connection is connection
        reused_trn.return_connection()
    finally:
        Transaction.shutdown_pools()


def test_postgresql_connection_is_returned_when_context_rollback_raises(
    monkeypatch,
):
    Transaction.shutdown_pools()
    try:
        trn = Transaction(POSTGRESQL_DSN)

        def broken_rollback():
            raise RuntimeError("rollback failed")

        with pytest.raises(RuntimeError, match="rollback failed"):
            with trn:
                connection = trn.connection
                monkeypatch.setattr(trn, "rollback", broken_rollback)
                raise ValueError("body failed")

        assert trn._connection is None

        reused_trn = Transaction(POSTGRESQL_DSN)
        assert reused_trn.connection is connection
        reused_trn.return_connection()
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


def test_sqlite_connection_is_borrowed_lazily(tmp_path):
    trn = Transaction(f"sqlite://{tmp_path / 'lazy.db'}")

    assert trn._connection is None

    connection = trn.connection

    assert connection is not None
    assert trn.connection is connection

    trn.close()

    assert trn._connection is None


def test_sqlite_commit_creates_connection(tmp_path):
    trn = Transaction(f"sqlite://{tmp_path / 'commit.db'}")

    assert trn._connection is None

    trn.commit()

    assert trn._connection is not None

    trn.close()


def test_sqlite_rollback_creates_connection(tmp_path):
    trn = Transaction(f"sqlite://{tmp_path / 'rollback.db'}")

    assert trn._connection is None

    trn.rollback()

    assert trn._connection is not None

    trn.close()


def test_shutdown_pools_closes_and_clears_postgresql_pools():
    Transaction.shutdown_pools()
    Transaction(POSTGRESQL_DSN)

    assert Transaction._pool_key(POSTGRESQL_DSN) in Transaction._pool_cache

    Transaction.shutdown_pools()

    assert Transaction._pool_cache == {}
