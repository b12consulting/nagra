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
from functools import lru_cache
from typing import Optional, Union

from nagra.schema import Schema
from nagra.delete import Delete
from nagra.select import Select
from nagra.upsert import Upsert
from nagra.transaction import Transaction, dummy_transaction
from nagra.exceptions import IncorrectTable


_TYPE_MAP = {
    "postgresql": {
        "varchar": "VARCHAR",
        "str": "VARCHAR",
        "text": "VARCHAR",
        "int": "INTEGER",
        "bigint": "BIGINT",
        "float": "FLOAT",
        "timestamp": "TIMESTAMP",
        "date": "DATE",
        "bool": "BOOL",
        "uuid": "UUID",
        "json": "JSON",
    },
    "sqlite": {
        "varchar": "TEXT",
        "text": "TEXT",
        "str": "TEXT",
        "int": "INTEGER",
        "bigint": "INTEGER",
        "float": "FLOAT",
        "timestamp": "DATETIME",
        "date": "DATE",
        "bool": "BOOL",
        "uuid": "TEXT",
        "json": "JSON",
    },
}

class Table:
    def __init__(
        self,
        name: str,
        columns: dict,
        natural_key: Optional[list] = None,
        foreign_keys: Optional[dict] = None,
        not_null: Optional[list] = None,
        one2many: Optional[dict] = None,
        default: Optional[dict] = None,
        schema: Schema = Schema.default,
    ):
        self.name = name
        self.columns = columns
        self.natural_key = natural_key or list(columns)
        self.foreign_keys = foreign_keys or {}
        self.not_null = set(self.natural_key) | set(not_null or [])
        self.one2many = one2many or {}
        self.default = default or {}
        self.schema = schema

        # Detect malformed fk definitions
        if len(self.natural_key) == 1:
            (nk,) = self.natural_key
            for fk, fk_table in self.foreign_keys.items():
                if fk != nk or fk_table != name:
                    continue
                msg = f"Table '{name}': Foreign key '{fk}' refers to table natural key"
                raise IncorrectTable(msg)

        # Add table to schema
        self.schema.add(self.name, self)

    @classmethod
    def get(self, name, schema=Schema.default):
        """
        Shortcut method to Schema.default().get()
        """
        return schema.get(name)

    def select(self, *columns, trn=None):
        trn = trn or Transaction.current or dummy_transaction
        if not columns:
            columns = self.default_columns()
        slct = Select(self, *columns, trn=trn, env=Env(self))
        return slct

    def delete(self, where=None, trn=None):
        trn = trn or Transaction.current or dummy_transaction
        delete = Delete(self, trn=trn, env=Env(self))
        if where:
            delete.where(where)
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
        trn = trn or Transaction.current or dummy_transaction
        return Upsert(self, *columns, trn=trn, lenient=lenient)

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
        return self.upsert(*columns, trn=trn, lenient=lenient).insert_only()

    def default_columns(self, nk_only: bool = False):
        """
        Return the list of default column for the current
        table. Used by `Table.select` and `Table.upsert` when no
        columns are provided.
        """
        columns = self.natural_key if nk_only else self.columns
        for column in columns:
            if column not in self.foreign_keys:
                yield column
                continue

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
    def join_on(self, path: tuple[str, ...], env: "Env"):
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
                join_col = "id"
                ftable = self.schema.get(table_name)
            else:
                # not an alias we implictly join on self, based on the
                # given column
                join_col = head
                alias_col = "id"
                fname = self.foreign_keys[join_col]
                ftable = self.schema.get(fname)
            return ftable, alias_col, join_col

        # Recurse to find the previous table in the chain
        prev_table, *_ = self.join_on(path[:-1], env)
        # Resolve last step
        return prev_table.join_on(path[-1:], env)

    def ctypes(self, trn):
        type_map = _TYPE_MAP[trn.flavor]
        return {c: type_map[d] for c, d in self.columns.items()}

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
