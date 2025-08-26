"""

Examples of table definitions

``` python
from nagra import Table

city = Table(
    "city",
    columns={
        "name": "varchar",
        "lat": "varchar",
        "long": "varchar",
    },
    natural_key=["name"],
    one2many = {
        "temperatures": "temperature.city",
    }
)

temperature = Table(
    "temperature",
    columns={
        "timestamp": "timestamp",
        "city": "int",
        "value": "float",
    },
    natural_key=["city", "timestamp"],
    foreign_keys={
        "city": "city",
    },

)
```


"""

import warnings
from datetime import date, datetime
from functools import lru_cache
from typing import Iterable, Optional, Union, TYPE_CHECKING

from nagra.delete import Delete
from nagra.exceptions import IncorrectSchema
from nagra.schema import Schema
from nagra.select import Select
from nagra.sexpr import AST
from nagra.statement import Statement
from nagra.transaction import Transaction
from nagra.update import Update
from nagra.copy import copy_from
from nagra.upsert import Upsert

if TYPE_CHECKING:
    from pandas import DataFrame


# Intentionally sorted by reverse length to help type hint detection,
# see Schema._db_columns
_TYPE_ALIAS = {
    "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamptz",
    "character varying": "str",
    "double precision": "float",
    "timestamptz": "timestamptz",
    "timestamp": "timestamp",
    "datetime": "timestamp",
    "boolean": "bool",
    "integer": "int",
    "numeric": "float",
    "varchar": "str",
    "bigint": "bigint",
    "vector": "float []",
    "bytea": "blob",
    "bytes": "blob",
    "float": "float",
    "jsonb": "json",
    "blob": "blob",
    "cidr": "str",
    "inet": "str",
    "real": "float",
    "bool": "bool",
    "date": "date",
    "json": "json",
    "text": "str",
    "uuid": "uuid",
    "int": "int",
    "str": "str",
    "": "str",
}

_DB_TYPE = {
    "postgresql": {
        "str": "TEXT",
        "int": "INTEGER",
        "bigint": "BIGINT",
        "float": "FLOAT",
        "timestamp": "TIMESTAMP",
        "timestamptz": "TIMESTAMPTZ",
        "date": "DATE",
        "bool": "BOOL",
        "uuid": "UUID",
        "json": "JSON",
        "blob": "BYTEA",
    },
    "sqlite": {
        "str": "TEXT",
        "int": "INTEGER",
        "bigint": "INTEGER",
        "float": "FLOAT",
        "timestamp": "DATETIME",
        "timestamptz": "DATETIME",
        "date": "DATE",
        "bool": "BOOL",
        "uuid": "TEXT",
        "json": "JSON",
        "blob": "BLOB",
    },
}


class Column:
    __slots__ = ["name", "dtype", "dims"]

    def __init__(self, name: str, dtype: str):
        self.name = name.strip()
        if "[" in dtype:
            dtype, dims = dtype.split("[", 1)
            self.dtype = dtype.strip()
            self.dims = "[" + dims.strip()
        else:
            self.dtype = dtype.strip()
            self.dims = ""
        try:
            self.dtype = _TYPE_ALIAS[dtype.strip().lower()]
        except KeyError:
            self.dtype = "str"
            warnings.warn(f"Type '{dtype}' not supported (for column '{name}'), falling back to string type.")

    def python_type(self):
        res = None
        match self.dtype:
            case "int" | "bigint":
                res = int
            case "str":
                res = str
            case "float":
                res = float
            case "timestamp" | "timestamptz":
                res = datetime
            case "bool":
                res = bool
            case "json":
                res = list | dict
            case "date":
                res = date
            case "uuid":
                res = str
            case "blob":
                res = bytes
            case _:
                raise RuntimeError("Unexpected error")

        for c in self.dims:
            if c == "[":
                res = list[res]

        return res


class Table:
    def __init__(
        self,
        name: str,
        columns: dict,
        natural_key: Optional[list[str]] = None,
        foreign_keys: Optional[dict] = None,
        not_null: Optional[list[str]] = None,
        one2many: Optional[dict] = None,
        default: Optional[dict] = None,
        primary_key: Optional[str] = "id",
        schema: Schema = Schema.default,
        is_view: Optional[bool] = False,
    ):
        self.name = name
        self.columns = {name: Column(name, dtype) for name, dtype in columns.items()}
        self.natural_key = natural_key or list(columns)
        self.foreign_keys = foreign_keys or {}
        self.not_null = (
            set(self.natural_key)
            | set(not_null or [])
            | set([primary_key])
        )
        self.one2many = one2many or {}
        self.default = default or {}
        self.primary_key = primary_key
        self.schema = schema
        self.is_view = is_view

        # Detect malformed fk definitions
        if len(self.natural_key) == 1:
            (nk,) = self.natural_key
            for fk, fk_table in self.foreign_keys.items():
                if fk != nk or fk_table != name:
                    continue
                msg = f"Table '{name}': Foreign key '{fk}' refers to table natural key"
                raise IncorrectSchema(msg)

        # Detect incorrect nk
        for nk_name in self.natural_key:
            if nk_name not in self.columns:
                raise IncorrectSchema(
                    f"Table '{name}': unknown column name '{nk_name}'"
                    " referenced in natural key"
                )

        # Add table to schema
        self.schema.add_table(self.name, self)

    @classmethod
    def get(self, name, schema=Schema.default):
        """
        Shortcut method to Schema.default().get()
        """
        return schema.tables.get(name)

    def select(self, *columns, trn=None):
        trn = trn or Transaction.current()
        if not columns:
            columns = self.default_columns()
        slct = Select(self, *columns, trn=trn, env=Env(self))
        return slct

    def delete(self, where=None, trn=None):
        trn = trn or Transaction.current()
        delete = Delete(self, trn=trn, env=Env(self))
        if where:
            return delete.where(where)
        return delete

    def upsert(
        self,
        *columns,
        trn: Optional[Transaction] = None,
        lenient: Union[bool, list[str]] = False,
    ):
        """
        Create an upsert object based on the given columns, if
        lenient is set, foreign keys wont be enforced on the given
        columns even if a value is passed on the subsequent execute or
        executemany. Example:

        >>> upsert = Table.get("comment").upsert("body", "blog_post.title", lenient=["blog_post"])
        >>> upsert.execute(("Nice post!", "A post title that will change soon."))

        If lenient is set to True all foreign keys will be treated as such.
        """
        if not columns:
            columns = self.default_columns()
        trn = trn or Transaction.current()
        return Upsert(self, *columns, trn=trn, env=Env(self), lenient=lenient)

    def update(
        self,
        *columns,
        trn: Optional[Transaction] = None,
        lenient: Union[bool, list[str]] = False,
    ):
        if not columns:
            columns = self.default_columns()
        trn = trn or Transaction.current()
        return Update(self, *columns, trn=trn, lenient=lenient)

    def insert(
        self,
        *columns,
        trn: Optional[Transaction] = None,
        lenient: Union[bool, list[str]] = False,
    ):
        """
        Provide an insert-only statement (won't raise error if
        record already exists). See `Table.upsert` for `lenient` role.
        """
        trn = trn or Transaction.current()
        return self.upsert(*columns, trn=trn, lenient=lenient).insert_only()

    def copy_from(
        self,
        rows: Iterable[tuple] | "DataFrame",
        trn: Optional[Transaction] = None,
        lenient: Union[bool, list[str]] = False,
    ):
        """
        Execute a COPY <table> FROM STDIN (only supported with
        postgresql). See `Table.upsert` for `lenient` role.
        """
        trn = trn or Transaction.current()
        return copy_from(self, rows=rows, trn=trn, lenient=lenient)

    def drop(self, trn: Optional[Transaction] = None):
        trn = trn or Transaction.current()
        stmt = Statement("drop_table", trn.flavor, name=self.name)
        trn.execute(stmt())

    def required(self, col_name):
        return (
            col_name in self.natural_key
            or col_name in self.not_null
            or col_name == self.primary_key
        )

    def default_columns(self, nk_only: bool = False):
        """
        Return the list of default column for the current
        table. Used by `Table.select` and `Table.upsert` when no
        columns are provided.
        """
        columns = self.natural_key if nk_only else self.columns
        for column in columns:
            # Escape literals (nul, true, false)
            if column in AST.literals:
                yield f".{column}"
                continue
            # Handle non foreign keys
            if column not in self.foreign_keys or nk_only:
                yield column
                continue
            # FK
            ftable = self.schema.get(self.foreign_keys[column])
            yield from (f"{column}.{k}" for k in ftable.default_columns(nk_only=True))

    def join(self, env: "Env"):
        for prefix, alias in env.refs.items():
            # Find alias of previous join in the chain
            *head, tail = prefix
            prev_table = env.refs[tuple(head)] if head else self.name
            # Identify last table & column of the chain
            ftable, alias_col, join_col = self.join_on(prefix, env)
            yield (ftable.name, alias, prev_table, alias_col, join_col)

    @lru_cache
    def join_on(self, path: tuple[str, ...], env: "Env") -> tuple["Table", str, str]:
        """
        `path` is a tuple containing names of column, each of
        which is a foreign key to another table.

        Returns the next table to join and the column to join on.

        """
        if len(path) == 1:
            head = path[0]
            if alias := self.one2many.get(head):
                # An alias is a string containing "table_name.fk_name"
                table_name, alias_col = alias.split(".")
                ftable = self.schema.get(table_name)
                join_col = ftable.primary_key
            else:
                # not an alias we implictly join on self, based on the
                # given column
                join_col = head
                fname = self.foreign_keys[join_col]
                ftable = self.schema.get(fname)
                alias_col = ftable.primary_key
            return ftable, alias_col, join_col

        # Recurse to find the previous table in the chain
        prev_table, *_ = self.join_on(path[:-1], env)
        # Resolve last step
        return prev_table.join_on(path[-1:], env)

    def ctypes(self, flavor: str, column_names: Iterable[str]):
        # detect arrays
        db_type = _DB_TYPE[flavor]
        res = {}
        for name in column_names:
            col = self.columns[name]
            if flavor == "sqlite" and col.dims:
                # sqlite does not support arrays
                res[name] = "json"
            else:
                res[name] = db_type[col.dtype] + col.dims
        return res

    def __iter__(self):
        return iter(self.select())

    def __repr__(self):
        return f"<Table {self.name}>"


class Env:
    def __init__(self, table: "Table", refs: Optional[dict] = None):
        self.table = table
        self.refs = refs or {}

    def add_ref(self, path):
        *head, name, tail = path
        prefix = tuple([*head, name])
        table_alias = self.refs.get(prefix)
        if not table_alias:
            if len(prefix) >= 2:
                self.add_ref(prefix)
            table_alias = f"{name}_{len(self.refs)}"
            self.refs[prefix] = table_alias
        return f'"{table_alias}"."{tail}"'

    def __repr__(self):
        content = repr(self.refs)
        return f"<Env {self.table.name} {content}>"

    def clone(self):
        return Env(self.table, self.refs.copy())
