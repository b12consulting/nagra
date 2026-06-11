import sqlite3
import threading
from itertools import islice
from typing import Callable

from nagra.utils import logger, UNSET, mssql_connection_string
from nagra.exceptions import NoActiveTransaction, TransactionReenterError


class LRUGenerator:
    """
    LRUGenerator wraps generator calls and implement LRU caching
    """

    def __init__(self, generator, size=1000):
        self.size = size
        self.generator = generator
        self.recent = {}
        self.older = {}

    def run(self, keys):
        # Capture existing known values in `cached`
        cached = {}
        fresh = []
        for k in keys:
            v = self.get(k)
            if v is UNSET:
                fresh.append(k)
            else:
                cached[k] = v

        # Update internal state with generator results
        for fresh_k, fresh_v in zip(fresh, self.generator(fresh)):
            self.set(fresh_k, fresh_v)

        # Yield results
        for k in keys:
            if k in cached:
                yield cached[k]
            else:
                yield self.get(k)

        # Vaccum
        self.vaccum()

    def __contains__(self, key):
        return key in self.recent or key in self.older

    def get(self, key, default=UNSET):
        if key in self.recent:
            return self.recent[key]

        if key in self.older:
            value = self.older[key]
            self.recent[key] = value
            return value

        return default

    def set(self, key, value):
        self.recent[key] = value

    def update(self, values):
        self.recent.update(values)
        self.vaccum()

    def vaccum(self):
        if len(self.older) + len(self.recent) > self.size:
            self.older = self.recent
            self.recent = {}


class Transaction:
    _pool_cache = {}
    _pool_lock = threading.Lock()
    _stack_lock = threading.Lock()
    # _local_stack: ContextVar[list["Transaction"]] = ContextVar('_local_stack', default=[])
    _local = threading.local()
    _local.stack = []

    def __init__(self, dsn, rollback=False, fk_cache=False):
        self.auto_rollback = rollback
        self._fk_cache = {} if fk_cache else None
        self._connection = None
        self._pool = None

        if dsn.startswith("postgresql://"):
            self._init_pg(dsn)

        elif dsn.startswith("sqlite://"):
            self._init_sqlite(dsn)

        elif dsn.startswith("mssql://"):
            self._init_mssql(dsn)

        elif dsn.startswith("duckdb://"):
            self._init_duckdb(dsn)

        else:
            raise ValueError(f"Invalid dsn string: {dsn}")

    def _init_pg(self, dsn):
        try:
            from psycopg_pool import ConnectionPool
        except ImportError as exc:  # pragma: no cover - optional dependency
            msg = "Postgresql support requires the 'psycopg' package. Install nagra[pg]."
            raise ImportError(msg) from exc

        self.flavor = "postgresql"

        with self._pool_lock:
            if dsn not in Transaction._pool_cache:
                Transaction._pool_cache[dsn] = ConnectionPool(
                    dsn,
                    min_size=0,
                    max_size=10,  # TODO should be configurable
                )
            self._pool = Transaction._pool_cache[dsn]

    def _init_sqlite(self, dsn):
        self.flavor = "sqlite"
        filename = dsn[9:]
        self._connection = sqlite3.connect(filename)
        self.connection.execute("PRAGMA foreign_keys = 1")

    def _init_mssql(self, dsn):
        try:
            import pyodbc
        except ImportError as exc:  # pragma: no cover - optional dependency
            msg = "SQL Server support requires the 'pyodbc' package. Install nagra[mssql]."
            raise ImportError(msg) from exc

        self.flavor = "mssql"
        conn_str = mssql_connection_string(dsn)
        self._connection = pyodbc.connect(conn_str, autocommit=False)
        cursor = self._connection.cursor()
        cursor.execute("SET QUOTED_IDENTIFIER ON")
        cursor.execute("SET XACT_ABORT ON")  # Enforce atomicity
        cursor.close()

    def _init_duckdb(self, dsn):
        import duckdb

        self.flavor = "duckdb"
        filename = dsn[9:]
        self._connection = duckdb.connect(filename)
        self.connection.begin()

    @property
    def connection(self):
        """
        Unified accessor for all backends.
        """
        if self._connection is None and self.flavor == "postgresql":
            self._connection = self._pool.getconn()
        return self._connection

    def execute(self, stmt, args=tuple()) -> "ResultCursor":
        logger.debug(stmt)
        cursor = self.connection.cursor()
        cursor.execute(stmt, args)
        match self.flavor:
            case "postgresql" | "sqlite":
                return ResultCursor(cursor)
            case "mssql":
                return MSSQLCursor(cursor)
            case _:
                msg = f"Unsupported flavor for execute: {self.flavor}"
                raise RuntimeError(msg)

    def executemany(
        self, stmt, args=None, returning=False
    ) -> "ResultCursor | RowCursor":
        logger.debug(stmt)
        cursor = self.connection.cursor()
        args = args or []

        match self.flavor:
            case "postgresql":
                cursor.executemany(stmt, args, returning=returning)
                return ResultCursor(cursor, returning=returning)
            case "sqlite":
                cursor.executemany(stmt, args)
                return ResultCursor(cursor)
            case "mssql":
                if not returning:
                    cursor.fast_executemany = True
                    cursor.executemany(stmt, args)
                    return ResultCursor(cursor, returning=returning)
                else:
                    rows = self._executemany_mssql(cursor, stmt, args)
                    return RowCursor(rows)
            case _:
                msg = f"Unsupported flavor for executemany: {self.flavor}"
                raise RuntimeError(msg)

    def _executemany_mssql(self, cursor, stmt, args):
        import pyodbc

        for params in args:
            cursor.execute(stmt, params)
            try:
                row = cursor.fetchone()
            except pyodbc.Error:
                row = None
            yield row

    def rollback(self):
        self.connection.rollback()

    def commit(self):
        self.connection.commit()

    def __enter__(self):
        Transaction.push(self)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        Transaction.pop(self)
        if self.auto_rollback or exc_type is not None:
            self.rollback()
        else:
            self.commit()
        self.close()

    def close(self):
        match self.flavor:
            case "postgresql":
                # Return the borrowed connection to pg pool
                if self._connection is not None:
                    self._pool.putconn(self._connection)
                    self._connection = None
            case _:
                # Close direct connections for other backends
                self._connection.close()

    @classmethod
    def push(cls, transaction):
        with cls._stack_lock:
            if not hasattr(cls._local, "stack"):
                cls._local.stack = []
            if transaction in cls._local.stack:
                raise TransactionReenterError(
                    "Transaction already in stack. Are you entering a context with the same transaction twice?"
                )
            cls._local.stack.append(transaction)

    @classmethod
    def pop(cls, expected_trn):
        with cls._stack_lock:
            trn = cls._local.stack.pop()
            assert trn is expected_trn, "Unexpected Transaction when leaving context"

    @classmethod
    def current(cls) -> "Transaction | DummyTransaction":
        try:
            with cls._stack_lock:
                return cls._local.stack[-1]
                # return cls._local_stack.get()[-1]
        except (IndexError, AttributeError):
            return dummy_transaction

    def __repr__(self):
        return f"<Transaction {self.flavor}>"

    def get_fk_cache(
        self, cache_key: tuple[str, ...], fn: Callable
    ) -> LRUGenerator | None:
        """
        Instanciate an LRUGenerator for the given function
        `fn`. Use `cache_key` to identify them. Will return
        `None` if `fk_cache` is False.
        """
        if self._fk_cache is None:
            return None

        if lru := self._fk_cache.get(cache_key):
            return lru
        lru = LRUGenerator(fn)
        self._fk_cache[cache_key] = lru
        return lru

    @classmethod
    def shutdown_pools(cls):
        """Gracefully close all managed PostgreSQL pools."""
        with cls._pool_lock:
            for pool in cls._pool_cache.values():
                pool.close()
            cls._pool_cache.clear()


def yield_from_cursor(cursor):
    while rows := cursor.fetchmany(1000):
        yield from rows


class CursorMixin:
    """
    Provide extra properties (one, all, scalar, scalars) and
    usefull methods for Cursor classes.
    """

    def fetchone(self):
        return next(self, None)

    def fetchmany(self, size=1000):
        return list(islice(self, size))

    def fetchall(self):
        return list(self)

    @property
    def one(self):
        return self.fetchone()

    @property
    def all(self):
        return self.fetchall()

    @property
    def scalar(self):
        (res,) = self.fetchone()
        return res

    @property
    def scalars(self):
        for (res,) in self:
            yield res


class ResultCursor(CursorMixin):
    def __init__(self, native_cursor, returning=False):
        self.native_cursor = native_cursor
        self.returning = returning

    def __iter__(self):
        if not self.returning:
            return iter(self.native_cursor)
        return self.iter_returning()

    def iter_returning(self):
        # Insert/update queries returning data must be iterated in a
        # different fashion
        while True:
            row = self.native_cursor.fetchone()
            yield row
            if not self.native_cursor.nextset():
                break

    def __next__(self):
        return next(iter(self))

    def close(self):
        self.native_cursor.close()


class MSSQLCursor(ResultCursor):
    def __iter__(self):
        return (r and tuple(r) for r in super().__iter__())

    def __next__(self):
        return next(self.native_cursor)


class RowCursor:
    """
    Wrapper around a collection of rows that mimicks ResultCursor,
    needed for mssql support.
    """

    def __init__(self, rows):
        self.rows = iter(rows)

    def __iter__(self):
        yield from (r and tuple(r) for r in self.rows)


class ExecMany:
    """
    Helper class that can consume an iterator and feed the values
    yielded to a (returning) executemany statement.
    """

    def __init__(self, stm, values, trn):
        self.stm = stm
        self.values = values
        self.trn = trn

    def __iter__(self):
        # Use a dedicated cursor to allow concurrent execution
        logger.debug(self.stm)
        match self.trn.flavor:
            case "sqlite":
                cursor = self.trn.connection.cursor()
                for vals in self.values:
                    cursor.execute(self.stm, vals)
                    yield cursor.fetchone()
            case "postgresql" | "mssql":
                cursor = self.trn.executemany(
                    self.stm,
                    self.values,
                    returning=True,
                )
                yield from cursor


class DummyTransaction(Transaction):
    """
    Postgresql flavored transaction look-alike
    """

    flavor = "postgresql"

    def __init__(self):
        pass

    def execute(self, stmt, args=tuple()):
        raise NoActiveTransaction()

    def executemany(self, stmt, args=None, returning=True):
        raise NoActiveTransaction()


dummy_transaction = DummyTransaction()
