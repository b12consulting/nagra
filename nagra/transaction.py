import sqlite3
import threading

from nagra.utils import logger
from nagra.exceptions import NoActiveTransaction


class Transaction:

    _local = threading.local()
    _local.stack = []

    def __init__(self, dsn, rollback=False):
        if dsn.startswith("postgresql://"):
            import psycopg
            # TODO use Connection Pool
            self.flavor = "postgresql"
            self.connection = psycopg.connect(dsn)
        elif dsn.startswith("sqlite://"):
            self.flavor = "sqlite"
            filename = dsn[9:]
            self.connection = sqlite3.connect(filename)
        elif dsn.startswith("duckdb://"):
            import duckdb
            self.flavor = "duckdb"
            filename = dsn[9:]
            self.connection = duckdb.connect(filename)
            self.connection.begin()
        else:
            raise ValueError(f"Invalid dsn string: {dsn}")
        self.cursor = self.connection.cursor()
        self.auto_rollback = rollback

    def execute(self, stmt, args=tuple()):
        logger.debug(stmt)
        cursor = self.cursor
        cursor.execute(stmt, args)
        if self.flavor == "duckdb":
            return yield_from_cursor(cursor)
        else:
            return cursor

    def executemany(self, stmt, args=None):
        logger.debug(stmt)
        cursor = self.cursor
        cursor.executemany(stmt, args, returning=True)
        if self.flavor == "duckdb":
            return yield_from_cursor(cursor)
        else:
            return cursor

    def rollback(self):
        self.connection.rollback()

    def commit(self):
        self.cursor.connection.commit()

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
        assert trn is expected_trn

    @classmethod
    @property
    def current(cls):
        try:
            return cls._local.stack[-1]
        except IndexError:
            return None
            # raise NoActiveTransaction()


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


class DummyTransaction:
    """
    Postgresql flavored transaction look-alike
    """
    flavor = "postgresql"


    def execute(self, stmt, args=tuple()):
        raise NoActiveTransaction()

    def executemany(self, stmt, args=None):
        raise NoActiveTransaction()


dummy_transaction = DummyTransaction()
