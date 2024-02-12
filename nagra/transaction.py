import sqlite3
import threading

from nagra.utils import logger, DEFAULT_FLAVOR
from nagra.exceptions import NoActiveTransaction


class Transaction:

    _local = threading.local()
    _local.current_transaction = []

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
        self.rollback = rollback

    @classmethod
    @property
    def current(cls):
        try:
            return cls._local.current_transaction[-1]
        except IndexError:
            raise NoActiveTransaction()

    @classmethod
    @property
    def flavor(cls):
        try:
            return Transaction.current and Transaction.current.flavor or DEFAULT_FLAVOR
        except NoActiveTransaction:
            return DEFAULT_FLAVOR

    @classmethod
    def execute(cls, stmt, args=tuple()):
        logger.debug(stmt)
        transaction = cls.current
        cursor = transaction.cursor
        cursor.execute(stmt, args)
        if transaction.flavor == "duckdb":
            return yield_from_cursor(cursor)
        else:
            return cursor


    @classmethod
    def executemany(cls, stmt, args=None):
        logger.debug(stmt)
        transaction = cls.current
        cursor = transaction.cursor
        cursor.executemany(stmt, args)
        if transaction.flavor == "duckdb":
            return yield_from_cursor(cursor)
        else:
            return cursor

    def __enter__(self):
        if not hasattr(self._local, "current_transaction"):
            self._local.current_transaction = []
        self._local.current_transaction.append(self)
        return self.cursor

    def __exit__(self, exc_type, exc_value, traceback):
        self._local.current_transaction.pop()
        if self.rollback or exc_type is not None:
            self.connection.rollback()
        else:
            self.cursor.connection.commit()


execute = Transaction.execute
executemany = Transaction.executemany


def yield_from_cursor(cursor):
    while rows := cursor.fetchmany(1000):
        yield from rows


class ExecMany:
    """
    Helper class that can consume an iterator and feed the values
    yielded to a (returning) executemany statement.
    """

    def __init__(self, stm, values):
        self.stm = stm
        self.values = values

    def __iter__(self):
        # Create a dedicated cursor
        cursor = Transaction.current.connection.cursor()
        if Transaction.flavor == "sqlite":
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
