import sqlite3
import threading
from typing import Callable

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


class Transaction:

    _local = threading.local()
    _local.stack = []

    def __init__(self, dsn, rollback=False, fk_cache=False):
        self.auto_rollback = rollback
        self._fk_cache = {} if fk_cache else None

        if dsn.startswith("postgresql://"):
            import psycopg

            # TODO use Connection Pool
            self.flavor = "postgresql"
            self.connection = psycopg.connect(dsn)
        elif dsn.startswith("sqlite://"):
            self.flavor = "sqlite"
            filename = dsn[9:]
            self.connection = sqlite3.connect(filename)
            self.connection.execute("PRAGMA foreign_keys = 1")
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
        if self.flavor == "sqlite":
            cursor.executemany(stmt, args)
        else:
            cursor.executemany(stmt, args, returning=returning)

        if self.flavor == "duckdb":
            return yield_from_cursor(cursor)
        else:
            return cursor

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
        if not hasattr(cls._local, "stack"):
            cls._local.stack = []
        cls._local.stack.append(transaction)

    @classmethod
    def pop(cls, expected_trn):
        trn = cls._local.stack.pop()
        assert trn is expected_trn, "Unexpected Transaction when leaving context"

    @classmethod
    def current(cls):
        try:
            return cls._local.stack[-1]
        except IndexError:
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
        cursor = self.trn.connection.cursor()
        if self.trn.flavor == "sqlite":
            for vals in self.values:
                logger.debug(self.stm)
                cursor.execute(self.stm, vals)
                res = cursor.fetchone()
                yield res
        else:
            logger.debug(self.stm)
            cursor.executemany(self.stm, self.values, returning=True)
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
