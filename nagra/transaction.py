import sqlite3
import threading
from contextvars import ContextVar
from typing import Callable
from urllib.parse import parse_qs, unquote_plus, urlparse

from nagra.utils import logger, UNSET
from nagra.exceptions import NoActiveTransaction


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


def _mssql_connection_string(dsn: str) -> str:
    """
    Build an ODBC connection string from a mssql:// style DSN.
    Supports typical URL components as well as a raw odbc_connect query parameter.
    """
    parsed = urlparse(dsn)
    query = parse_qs(parsed.query, keep_blank_values=True)

    if "odbc_connect" in query:
        return unquote_plus(query["odbc_connect"][-1])

    driver = unquote_plus(query.pop("driver", ["ODBC Driver 18 for SQL Server"])[-1])
    parts = [
        f"DRIVER={{{driver}}}",
        "TrustServerCertificate=yes", # FIXME should come from dsn
    ]

    host = parsed.hostname or "localhost"
    if parsed.port:
        host = f"{host},{parsed.port}"
    parts.append(f"SERVER={host}")

    database = parsed.path.lstrip("/")
    if database:
        parts.append(f"DATABASE={unquote_plus(database)}")

    if parsed.username:
        parts.append(f"UID={unquote_plus(parsed.username)}")
    if parsed.password:
        parts.append(f"PWD={unquote_plus(parsed.password)}")

    trusted_values = query.pop("trusted_connection", query.pop("Trusted_Connection", [None]))
    trusted = trusted_values[-1]
    if trusted is not None:
        parts.append(f"Trusted_Connection={unquote_plus(trusted)}")

    for key, values in query.items():
        if not values:
            continue
        parts.append(f"{key}={unquote_plus(values[-1])}")

    return ";".join(parts)


class Transaction:

    _stack_lock = threading.Lock()
    # _local_stack: ContextVar[list["Transaction"]] = ContextVar('_local_stack', default=[])
    _local = threading.local()
    _local.stack = []

    def __init__(self, dsn, rollback=False, fk_cache=False):
        self.auto_rollback = rollback
        self._fk_cache = {} if fk_cache else None

        if dsn.startswith("postgresql://"):
            try:
                import psycopg
            except ImportError as exc:  # pragma: no cover - optional dependency
                msg = "Postgresql support requires the 'psycopg' package. Install nagra[pg]."
                raise ImportError(msg) from exc

            # TODO use Connection Pool
            self.flavor = "postgresql"
            self.connection = psycopg.connect(dsn)
        elif dsn.startswith("sqlite://"):
            self.flavor = "sqlite"
            filename = dsn[9:]
            self.connection = sqlite3.connect(filename)
            self.connection.execute("PRAGMA foreign_keys = 1")
        elif dsn.startswith("mssql://"):
            try:
                import pyodbc
            except ImportError as exc:  # pragma: no cover - optional dependency
                msg = "SQL Server support requires the 'pyodbc' package. Install nagra[mssql]."
                raise ImportError(msg) from exc

            self.flavor = "mssql"
            conn_str = _mssql_connection_string(dsn)
            self.connection = pyodbc.connect(conn_str, autocommit=False)
            cursor = self.connection.cursor()
            cursor.execute("SET QUOTED_IDENTIFIER ON")
            cursor.execute("SET XACT_ABORT ON")  # Enforce atomicity
            cursor.close()
        elif dsn.startswith("duckdb://"):
            import duckdb

            self.flavor = "duckdb"
            filename = dsn[9:]
            self.connection = duckdb.connect(filename)
            self.connection.begin()
        else:
            raise ValueError(f"Invalid dsn string: {dsn}")

    def execute(self, stmt, args=tuple()):
        logger.debug(stmt)
        cursor = self.connection.cursor()
        cursor.execute(stmt, args)
        if self.flavor == "duckdb":
            return yield_from_cursor(cursor)
        else:
            return cursor

    def executemany(self, stmt, args=None, returning=True):
        logger.debug(stmt)
        cursor = self.connection.cursor()
        args = args or []

        if self.flavor == "sqlite":
            cursor.executemany(stmt, args)
            return cursor

        if self.flavor == "duckdb":
            cursor.executemany(stmt, args, returning=returning)
            return yield_from_cursor(cursor)

        if self.flavor == "mssql":
            # cursor.executemany(stmt, args)
            # return cursor
            return self._executemany_mssql(cursor, stmt, args, returning)

        cursor.executemany(stmt, args, returning=returning)
        return cursor

    def _executemany_mssql(self, cursor, stmt, args, returning):
        import pyodbc  # Local import to keep optional dependency scoped
        print(f"{returning=}", f"{args=}", stmt)

        sequence = list(args)
        if not sequence:
            return SequenceCursor([]) if returning else cursor
        if returning:
            rows = []
            for params in sequence:
                cursor.execute(stmt, params)
                try:
                    row = cursor.fetchone()
                except pyodbc.Error:  # pragma: no cover - driver specific
                    row = None
                rows.append(tuple(row))
            return SequenceCursor(rows)

        try:
            cursor.fast_executemany = True
        except AttributeError:
            pass
        cursor.executemany(stmt, sequence)
        return (tuple(r) for r in cursor)

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

    @classmethod
    def push(cls, transaction):
        with cls._stack_lock:
            if not hasattr(cls._local, "stack"):
                cls._local.stack = []
            cls._local.stack.append(transaction)
            # stack = cls._local_stack.get()
            # stack.append(transaction)
            # cls._local_stack.set(stack)

    @classmethod
    def pop(cls, expected_trn):
        with cls._stack_lock:
            trn = cls._local.stack.pop()
            assert trn is expected_trn, "Unexpected Transaction when leaving context"

            # stack = cls._local_stack.get()
            # trn = stack.pop()
            # cls._local_stack.set(stack)
            # assert id(trn) == id(expected_trn), "Unexpected Transaction when leaving context"

    @classmethod
    def current(cls):
        try:
            with cls._stack_lock:
                return cls._local.stack[-1]
                # return cls._local_stack.get()[-1]
        except (IndexError, AttributeError):
            return dummy_transaction

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.flavor}>"

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


def yield_from_cursor(cursor):
    while rows := cursor.fetchmany(1000):
        yield from rows


class SequenceCursor:
    """
    Minimal cursor-like object used to return rows collected during manual executemany calls.
    """

    def __init__(self, rows):
        self._rows = rows
        self._index = 0

    def fetchone(self):
        if self._index < len(self._rows):
            return self._rows[self._index]
        return None

    def nextset(self):
        if self._index < len(self._rows) - 1:
            self._index += 1
            return True
        if self._index == len(self._rows) - 1:
            self._index += 1
        return False


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
        # Create a dedicated cursor
        if self.trn.flavor == "sqlite":
            cursor = self.trn.connection.cursor()
            for vals in self.values:
                logger.debug(self.stm)
                cursor.execute(self.stm, vals)
                res = cursor.fetchone()
                yield res
        else:
            cursor = self.trn.executemany(self.stm, self.values, returning=True)
            while True:
                vals = cursor.fetchone()
                yield vals
                if not cursor.nextset():
                    break


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
