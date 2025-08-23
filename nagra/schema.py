from itertools import chain, groupby
from contextlib import contextmanager
from collections import defaultdict
from pathlib import Path
from io import IOBase
from typing import Optional, TYPE_CHECKING

import toml
from nagra.statement import Statement
from nagra.transaction import Transaction
from nagra.utils import logger, snake_to_pascal, template


if TYPE_CHECKING:
    from nagra.table import Table, View


class Schema:
    def __init__(self, tables=None, views=None):
        self.tables = tables or {}
        self.views = views or {}

    @classmethod
    def from_toml(self, toml_src: IOBase | Path | str) -> "Schema":
        schema = Schema()
        schema.load_toml(toml_src)
        return schema

    @property
    def empty(self) -> bool:
        return not self.tables

    def load_toml(self, toml_src: IOBase | Path | str):
        # Late import to avoid import loops
        from nagra.table import Table
        from nagra.view import View

        # load table definitions
        match toml_src:
            case IOBase():
                content = toml_src.read()
            case Path():
                content = toml_src.open().read()
            case _:
                content = toml_src
        tables = toml.loads(content)
        # Instanciate tables
        for name, info in tables.items():
            logger.debug("Instanciate '%s' table from toml", name)
            if "primary_key" in info:
                if info["primary_key"].strip() == "":
                    info["primary_key"] = None

            # Handle view info
            view_params = ("view_columns", "as_select", "view_select")
            if any(info.get(c) for c in view_params):
                View(name, **info, schema=self)
            else:
                Table(name, **info, schema=self)

    def add_table(self, name: str, table: "Table"):
        if name in self.tables:
            raise RuntimeError(f"Table {name} already in schema!")
        self.tables[name] = table

    def add_view(self, name: str, view: "View"):
        if name in self.views:
            raise RuntimeError(f"View {name} already in schema!")
        self.views[name] = view

    def reset(self):
        self.tables = {}
        self.views = {}

    def get(self, name: str) -> "Table | View":
        """
        Return the view or the table with name `name`
        """
        res = self.views.get(name) or self.tables[name]
        if not res:
            raise KeyError(f"No view or table named {name}")
        return res

    @classmethod
    def _db_columns(cls, trn=None, pg_schema="public"):
        from nagra.table import _TYPE_ALIAS

        trn = trn or Transaction.current()
        res = defaultdict(dict)
        stmt = Statement("find_columns", trn.flavor, pg_schema=pg_schema)
        for tbl, col_name, col_type, *hints in trn.execute(stmt()):
            # handle array types
            if col_type.upper() == "ARRAY" and hints:
                # Try to find type of elements, rely on the fact that
                # _TYPE_ALIAS keys are sorted by reverse length.
                hint = hints[0].lower()
                for candidate in _TYPE_ALIAS:
                    if candidate in hint:
                        # XXX We can not detect array dimensions,
                        # fallback to simple array
                        col_type = f"{candidate} []"
                        break
                else:
                    msg = f"Unable to detect type of column: {col_name} in table {tbl}"
                    raise RuntimeError(msg)
            # handle pg extension types
            if col_type.upper() == "USER-DEFINED" and hints:
                for name in hints:
                    if col_type := _TYPE_ALIAS.get(name.lower()):
                        break
                else:
                    msg = f"Unable to detect type of column: {col_name} in table {tbl}"
                    raise RuntimeError(msg)

            res[tbl][col_name] = col_type
        return res

    def _db_indexes(cls, trn=None, pg_schema="public"):
        trn = trn or Transaction.current()
        stmt = Statement("find_indexes", trn.flavor, pg_schema=pg_schema)
        res = [n for n, in trn.execute(stmt())]
        return res

    def _db_views(cls, trn=None, pg_schema="public") -> dict[str, str]:
        trn = trn or Transaction.current()
        stmt = Statement("find_views", trn.flavor, pg_schema=pg_schema)
        # The statement returns tuples of (name, view_def)
        res = dict(trn.execute(stmt()))
        return res

    @classmethod
    def _db_fk(cls, *whitelist, trn=None, pg_schema="public"):
        trn = trn or Transaction.current()
        res = defaultdict(dict)
        stmt = Statement("find_foreign_keys", trn.flavor, pg_schema=pg_schema)
        for name, tbl, col, ftable, fcol in trn.execute(stmt()):
            if whitelist and tbl not in whitelist:
                continue
            if name in res[tbl]:
                raise RuntimeError("Unexpected multi-columns foreign key")
            res[tbl][name] = FKConstraint(name, tbl, col, ftable, fcol)
        return res

    @classmethod
    def _db_pk(cls, trn=None, pg_schema="public"):
        trn = trn or Transaction.current()
        res = {}
        stmt = Statement("find_primary_keys", trn.flavor, pg_schema=pg_schema)
        for tbl, pk_col in trn.execute(stmt()):
            if tbl in res:
                raise RuntimeError("Unexpected multi-columns primary key")
            res[tbl] = pk_col
        return res

    @classmethod
    def _db_unique(cls, db_pk, trn=None, pg_schema="public") -> dict[str, list[str]]:
        trn = trn or Transaction.current()
        by_constraint = defaultdict(list)

        if trn.flavor == "sqlite":
            stmt = Statement("find_unique_constraint", trn.flavor)
            constraints = trn.execute(stmt()).fetchall()
            for (tbl, idx_name), rows in groupby(constraints, key=lambda x: x[:2]):
                rows = list(rows)
                by_constraint[tbl].append([col_name for _, _, col_name in rows])

        else:
            stmt = Statement("find_unique_constraint", trn.flavor, pg_schema=pg_schema)
            constraints = trn.execute(stmt()).fetchall()
            for tbl, constraint_name in constraints:
                col_stmt = Statement(
                    "find_index_columns",
                    trn.flavor,
                    pg_schema=pg_schema,
                    name=constraint_name,
                )
                columns = [c for c, in trn.execute(col_stmt())]
                # Postgresql will wrap columns names with quotes for
                # reserved words
                columns = [c.strip('"') for c in columns]
                by_constraint[tbl].append(columns)

        # Keep the unique constraint with the lowest number of columns for
        # each table
        res = {}
        for table, constraints in by_constraint.items():
            candidates = sorted(constraints, key=lambda item: len(item))
            for candidate in candidates:
                if len(candidate) == 1 and candidate[0] == db_pk.get(table):
                    # unique key is the primary key, skip
                    continue
                res[table] = candidate
                break
        return res

    def _create_views(self, trn):
        # Create tables
        for name, view in self.views.items():
            if trn.flavor == "sqlite":
                # SQLite does not support OR REPLACE
                stmt = Statement(
                    "drop_view",
                    trn.flavor,
                    name=name,
                )
                yield stmt()

            stmt = Statement(
                "create_view",
                trn.flavor,
                name=name,
                view_def=view.view_def(),
            )
            yield stmt()

    def _create_tables(self, db_columns, trn):
        # Create tables
        for name, table in self.tables.items():
            if table.is_view:
                continue

            if name in db_columns:
                continue
            ctypes = table.ctypes(trn.flavor, table.columns)

            # TODO use "KEY GENERATED ALWAYS AS IDENTITY" instead of
            # serials (see https://stackoverflow.com/a/55300741) ?
            if table.primary_key is None:
                ctypes = table.ctypes(trn.flavor, table.columns)
                # Create the list of natural key columns, respecting
                # table definition order:
                nk_cols = [c for c in table.columns if c in table.natural_key]

                # Create tuples of (name, type, foreign_table, default)
                natural_key = [
                    (
                        c,
                        ctypes[c],
                        table.foreign_keys.get(c),
                        table.default.get(c),
                    )
                    for c in nk_cols
                ]

                fk_tables = {}
                for nk_col, *_ in natural_key:
                    if fk_table_name := table.foreign_keys.get(nk_col):
                        fk_tables[nk_col] = self.get(fk_table_name)

                stmt = Statement(
                    "create_table_nk",
                    trn.flavor,
                    table=table,
                    natural_key=natural_key,
                    fk_tables=fk_tables,
                )
            else:
                if fk_table_name := table.foreign_keys.get(table.primary_key):
                    fk_table = self.get(fk_table_name)
                else:
                    fk_table = None
                stmt = Statement(
                    "create_table",
                    trn.flavor,
                    table=table,
                    pk_type=ctypes.get(table.primary_key),
                    fk_table=fk_table,
                )
            yield stmt()

    def _add_columns(self, db_columns, trn):
        # Add columns
        for table in self.tables.values():
            if table.is_view:
                continue

            ctypes = table.ctypes(trn.flavor, table.columns)
            for column in table.columns:
                # The base table can contain either the pk either the nk
                if column == table.primary_key:
                    continue
                if table.primary_key is None and column in table.natural_key:
                    continue
                if column in db_columns.get(table.name, []):
                    continue
                if fk_table_name := table.foreign_keys.get(column):
                    fk_table = self.get(fk_table_name)
                else:
                    fk_table = None

                stmt = Statement(
                    "add_column",
                    flavor=trn.flavor,
                    table=table.name,
                    column=column,
                    col_def=ctypes[column],
                    not_null=column in table.not_null,
                    fk_table=fk_table,
                    default=table.default.get(column),
                )
                yield stmt()

    def _create_indexes(self, db_indexes, trn):
        # Add index on natural keys
        for name, table in self.tables.items():
            if table.is_view or f"{name}_idx" in db_indexes:
                continue
            stmt = Statement(
                "create_unique_index",
                trn.flavor,
                table=name,
                natural_key=table.natural_key,
            )
            yield stmt()

    def setup_statements(self, trn=None):
        trn = trn or Transaction.current()
        # Find existing tables and columns
        db_columns = self._db_columns(trn)
        db_indexes = self._db_indexes(trn)

        yield from self._create_tables(db_columns, trn)
        yield from self._add_columns(db_columns, trn)
        yield from self._create_indexes(db_indexes, trn)
        yield from self._create_views(trn)

    def create_tables(self, trn=None):
        """
        Create tables, indexes and foreign keys
        """
        trn = trn or Transaction.current()
        # Loop on setup statements and execute them
        for stm in self.setup_statements(trn=trn):
            trn.execute(stm)

    @classmethod
    def from_db(cls, trn: Optional[Transaction] = None) -> "Schema":
        """
        Instanciate a nagra Schema (and Tables) based on database
        schema
        """
        trn = trn or Transaction.current()
        schema = Schema()
        schema.introspect_db(trn=trn)
        return schema

    def introspect_db(self, *tables: str, trn: Optional[Transaction] = None):
        """
        Instanciate Table instances based on database content. If
        `tables` is non-empty, it is used as a whitelist and all other
        tables are ignored
        """
        from nagra.table import Table
        from nagra.view import View

        trn = trn or Transaction.current()
        db_fk = self._db_fk(*tables, trn=trn)
        db_pk = self._db_pk(trn=trn)
        db_unique = self._db_unique(db_pk, trn=trn)
        db_columns = self._db_columns(trn=trn)
        db_views = self._db_views(trn=trn)

        for table_name, cols in db_columns.items():
            if tables and table_name not in tables:
                continue
            fks = {fk.column: fk.foreign_table for fk in db_fk[table_name].values()}
            if view_def := db_views.get(table_name):
                # Instanciate view
                View(
                    table_name,
                    columns=cols,
                    natural_key=db_unique.get(table_name),
                    foreign_keys=fks,
                    schema=self,
                    as_select=view_def,
                )
            else:
                # Instanciate table
                Table(
                    table_name,
                    columns=cols,
                    natural_key=db_unique.get(table_name),
                    foreign_keys=fks,
                    primary_key=db_pk.get(table_name),
                    schema=self,
                )

    def drop(self, trn=None):
        trn = trn or Transaction.current()
        for table in self.tables.values():
            table.drop(trn)

    def generate_d2(self):
        tpl = template("misc/schema.d2")
        tables = self.tables.values()
        res = "\n".join(tpl.render(table=t) for t in tables)
        return res

    def generate_pydantic_models(self, base_class:str="BaseModel", table_names:list[str] | None= None):
        tpl = template("misc/pydantic-schema.py")
        if not table_names:
            tables = self.tables.values()
        else:
            tables = [self.tables[t] for t in table_names]

        res = "\n".join(
            tpl.render(
                table=t,
                class_name=snake_to_pascal(t.name),
                base_class=base_class,
                snake_to_pascal=snake_to_pascal,
            )
            for t in tables
        )
        return res

    def generate_toml(self):
        tpl = template("misc/schema.toml")
        tables = self.tables.values()
        res = "\n".join(tpl.render(table=t) for t in tables)
        return res

    @contextmanager
    def suspend_fk(self, trn: Optional[Transaction] = None):
        """
        Temporarily drop all foreign keys and re-add them when
        exiting.  The db is introspected each time `suspend_fk` is
        called and the content of Schema is ignored, so the code may drop
        and re-add more foreign keys.
        """
        trn = trn or Transaction.current()
        if trn.flavor == "sqlite":
            trn.execute("PRAGMA foreign_keys = 0")
            yield
            trn.execute("PRAGMA foreign_keys = 1")
            return

        all_fks = list(
            chain.from_iterable(fks.values() for fks in self._db_fk(trn=trn).values())
        )
        for fk in all_fks:
            fk.drop()
        yield

        for fk in all_fks:
            fk.add()

    default: "Schema" = None


# Define default schema
Schema.default = Schema()


class FKConstraint:
    def __init__(self, name, table, column, foreign_table, foreign_column):
        self.name = name
        self.table = table
        self.column = column
        self.foreign_table = foreign_table
        self.foreign_column = foreign_column

    def drop(self, trn=None):
        trn = trn or Transaction.current()
        stmt = Statement(
            "drop_fk",
            trn.flavor,
            table=self.table,
            name=self.name,
        )
        trn.execute(stmt())

    def add(self, trn=None):
        trn = trn or Transaction.current()
        stmt = Statement(
            "add_foreign_key",
            trn.flavor,
            table=self.table,
            column=self.column,
            name=self.name,
            foreign_table=self.foreign_table,
            foreign_column=self.foreign_column,
        )
        trn.execute(stmt())
