from functools import lru_cache
from typing import Dict, List

from nagra import execute
from nagra.schema import Schema
from nagra.delete import Delete
from nagra.select import Select
from nagra.upsert import Upsert
from nagra.transaction import Transaction


_SQLITE_TYPE_MAP = {
    "varchar": "TEXT",
    "int": "INTEGER",
    "float": "FLOAT",
    "timestamp": "DATETIME",
}



class Table:
    def __init__(
        self,
        name: str,
        columns: Dict,
        natural_key: List = None,
        foreign_keys: Dict = None,
        not_null: List = None,
        one2many: Dict = None,
        schema: Schema = Schema.default,
    ):
        self.name = name
        self.columns = columns
        self.natural_key = natural_key or list(columns)
        self.foreign_keys = foreign_keys or {}
        self.not_null = set(self.natural_key) | set(not_null or [])
        self.one2many = one2many or {}
        self.schema = schema
        self.schema.add(self.name, self)

    @classmethod
    def get(self, name, schema=Schema.default):
        """
        Shortcut method to Schema.default().get()
        """
        return schema.get(name)

    def select(self, *columns):
        if not columns:
            columns = self.default_columns()
        slct = Select(self, *columns, env=Env(self))
        return slct

    def default_columns(self, nk_only=False):
        columns = self.natural_key if nk_only else self.columns
        for column in columns:
            if column not in self.foreign_keys:
                yield column
                continue

            ftable = self.schema.get(self.foreign_keys[column])
            yield from (f"{column}.{k}" for k in ftable.default_columns(nk_only=True))

    def join(self, env):
        for prefix, alias in env.refs.items():
            # Find alias of previous join in the chain
            *head, tail = prefix
            prev_table = env.refs[tuple(head)] if head else self.name
            # Identify last table & column of the chain
            ftable, alias_col, join_col = self.join_on(prefix, env)
            yield (ftable.name, alias, prev_table, alias_col, join_col)

    @lru_cache
    def join_on(self, path, env):
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
                fname= self.foreign_keys[join_col]
                ftable = self.schema.get(fname)
            return ftable, alias_col, join_col

        # Recurse to find the previous table in the chain
        prev_table, *_  = self.join_on(path[:-1], env)
        # Resolve last step
        return prev_table.join_on(path[-1:], env)

    def delete(self, where=None):
        delete = Delete(self, env=Env(self))
        if where:
            delete.where(where)
        return delete

    def upsert(self, *columns, lenient=None):
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
        return Upsert(self, *columns, lenient=lenient)

    def insert(self, *columns, lenient=None):
        """
        Provide an insert-only statement (won't raise error if
        record already exists). See `Table.upsert` for `lenient` role.
        """
        return Upsert(self, *columns, lenient=None).insert_only()

    def suggest(self, column, like=None):
        """
        Return a iterator over the possible values of columns. Use
        like in a where condition if given.
        """
        if "." not in column:
            raise NotImplementedError("TODO")
        local_col, remote_col = column.split(".", 1)
        cond = []
        if like:
            cond.append("(like " + remote_col + " {})")

        ftable = self.schema.get(self.foreign_keys[local_col])
        select = ftable.select(remote_col).where(*cond).groupby(remote_col).orderby(remote_col)
        if like:
            cur = execute(select.stm(), (like,))
            return (x for x, in cur)
        return (x for x, in select)

    def ctypes(self):
        if Transaction.flavor == "sqlite":
            return {
                c: _SQLITE_TYPE_MAP.get(d, d) for c, d in self.columns.items()
            }
        return self.columns

    def __repr__(self):
        return f"<Table {self.name}>"


class Env:
    def __init__(self, table, refs=None):
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
