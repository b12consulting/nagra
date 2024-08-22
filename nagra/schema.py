from collections import defaultdict
from jinja2 import Template
from pathlib import Path
from io import IOBase
from typing import Optional, TYPE_CHECKING

import toml
from nagra.statement import Statement
from nagra.transaction import Transaction

if TYPE_CHECKING:
    from nagra.table import Table


D2_TPL = """
{{table.name}}_: "{{table.name}}" {
  shape: sql_table
  {%- for name, col in table.columns.items() %}
  {{name}}: {{col.python_type()}}
  {%- endfor %}
}
{%- for col, f_table in table.foreign_keys.items() %}
{{table.name}}.{{col}} -> {{f_table}}.id : {{col}}
{%- endfor -%}
"""


class Schema:
    _default = None

    def __init__(self, tables=None):
        self.tables = tables or {}

    @classmethod
    @property
    def default(cls):
        if not cls._default:
            cls._default = Schema()
        return cls._default

    @classmethod
    def from_toml(self, toml_src: IOBase | Path | str) -> "Schema":
        schema = Schema()
        schema.load_toml(toml_src)
        return schema

    def load_toml(self, toml_src: IOBase | Path | str):
        # Late import to avoid import loops
        from nagra.table import Table

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
            Table(name, **info, schema=self)

    def add(self, name: str, table: "Table"):
        if name in self.tables:
            raise RuntimeError(f"Table {name} already in schema!")
        self.tables[name] = table

    def reset(self):
        self.tables = {}

    def get(self, name: str) -> "Table":
        """
        Return the table with name `name`
        """
        return self.tables[name]

    @classmethod
    def _db_columns(cls, trn=None, pg_schema="public"):
        trn = trn or Transaction.current
        res = defaultdict(dict)
        stmt = Statement("find_columns", trn.flavor, pg_schema=pg_schema)
        for tbl, col_name, col_type in trn.execute(stmt()):
            res[tbl][col_name] = col_type
        return res

    @classmethod
    def _db_fk(cls, trn=None, pg_schema="public"):
        trn = trn or Transaction.current
        res = defaultdict(dict)
        stmt = Statement("find_foreign_keys", trn.flavor, pg_schema=pg_schema)
        for tbl, col, ftable in trn.execute(stmt()):
            res[tbl][col] = ftable
        return res

    @classmethod
    def _db_pk(cls, trn=None, pg_schema="public"):
        trn = trn or Transaction.current
        res = {}
        stmt = Statement("find_primary_keys", trn.flavor, pg_schema=pg_schema)
        for tbl, pk_col in trn.execute(stmt()):
            res[tbl] = pk_col
        return res

    @classmethod
    def _db_unique(cls, trn=None, pg_schema="public"):
        trn = trn or Transaction.current
        by_constraint = defaultdict(lambda: defaultdict(list))
        stmt = Statement("find_unique_constraint", trn.flavor, pg_schema=pg_schema)
        for tbl, constraint, col in trn.execute(stmt()):
            by_constraint[tbl][constraint].append(col)

        # Keep the unique constraint with the lowest number of columns for
        # each table
        res = {}
        for table, constraints in by_constraint.items():
            key_fn = lambda name: len(constraints[name])
            first, *_ = sorted(constraints, key=key_fn)
            res[table] = constraints[first]
        return res

    def setup_statements(self, db_columns, flavor):
        # Create tables
        for name, table in self.tables.items():
            if name in db_columns:
                continue
            ctypes = table.ctypes(flavor, table.columns)
            # TODO use KEY GENERATED ALWAYS AS IDENTITY) instead of
            # serials (see https://stackoverflow.com/a/55300741) ?
            stmt = Statement(
                "create_table", flavor, table=table, pk_type=ctypes.get(table.primary_key)
            )
            yield stmt()

        # Add columns
        for table in self.tables.values():
            ctypes = table.ctypes(flavor, table.columns)
            for column in table.columns:
                if column == table.primary_key:
                    continue
                if column in db_columns.get(table.name, []):
                    continue
                stmt = Statement(
                    "add_column",
                    flavor=flavor,
                    table=table.name,
                    column=column,
                    col_def=ctypes[column],
                    not_null=column in table.not_null,
                    fk_table=table.foreign_keys.get(column),
                    default=table.default.get(column),
                )
                yield stmt()

        # Add index on natural keys
        for name, table in self.tables.items():
            stmt = Statement(
                "create_unique_index",
                flavor,
                table=name,
                natural_key=table.natural_key,
            )
            yield stmt()

    def create_tables(self, trn=None):
        """
        Create tables, indexes and foreign keys
        """
        trn = trn or Transaction.current
        # Find existing tables and columns
        db_columns = self._db_columns(trn)
        # Loop on setup statements and execute them
        for stm in self.setup_statements(db_columns, trn.flavor):
            trn.execute(stm)

    @classmethod
    def from_db(cls, trn: Optional[Transaction] = None) -> "Schema":
        """"
        Instanciate a nagra Schema (and Tables) based on database
        schema
        """
        schema = Schema()
        schema.introspect_db(trn=trn)
        return schema

    def introspect_db(self, trn: Optional[Transaction] = None):
        from nagra.table import Table

        trn = trn or Transaction.current
        db_fk = self._db_fk(trn)
        db_pk = self._db_pk(trn)
        db_unique = self._db_unique(trn)
        db_columns = self._db_columns(trn=trn)
        for table_name, cols in db_columns.items():
            # Instanciate table
            Table(
                table_name,
                columns=cols,
                natural_key=db_unique.get(table_name),
                foreign_keys=db_fk[table_name],
                primary_key=db_pk.get(table_name, None),
                schema=self)

    def drop(self, trn=None):
        trn = trn or Transaction.current
        for table in self.tables.values():
            table.drop(trn)

    def generate_d2(self):
        tpl = Template(D2_TPL)
        tables = self.tables.values()
        res = "\n".join(tpl.render(table=t) for t in tables)
        return res
