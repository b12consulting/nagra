from collections import defaultdict
from jinja2 import Template
from pathlib import Path
from io import IOBase

import toml

from nagra.statement import Statement
from nagra.transaction import Transaction


D2_TPL = """
{{table.name}}_: "{{table.name}}" {
  shape: sql_table
  {%- for col, dt in table.columns.items() %}
  {{col}}: {{dt}}
  {%- endfor %}
}
{%- for col, f_table in table.foreign_keys.items() %}
{{table.name}}.{{col}} -> {{f_table}}.id : {{col}}
{%- endfor -%}
"""



class Schema:
    _default = None

    def __init__(self, trn=None, tables=None):
        self.tables = tables or {}

    @classmethod
    @property
    def default(cls):
        if not cls._default:
            cls._default = Schema()
        return cls._default

    @classmethod
    def from_toml(self, toml_src):
        schema = Schema()
        schema.load(toml_src)
        return schema

    def load(self, toml_src):
        # Late import to avoid import loops
        from nagra.table import Table

        # load table definitions
        match toml_src:
            case IOBase():
                content = toml_src.read()
            case Path():
                content =  toml_src.open().read()
            case _:
                content = toml_src
        tables = toml.loads(content)
        # Instanciate tables
        for name, info in tables.items():
            Table(name, **info, schema=self)

    def add(self, name, table):
        if name in self.tables:
            raise RuntimeError(f"Table {name} already in schema!")
        self.tables[name] = table

    def reset(self):
        self.tables = {}

    def get(self, name):
        """
        Return the table with name `name`
        """
        return self.tables[name]

    def _db_columns(self, trn=None, pg_schema="public"):
        trn = trn or Transaction.current
        res = defaultdict(list)
        stmt = Statement("find_columns", trn.flavor, pg_schema=pg_schema)
        for tbl, col in trn.execute(stmt()):
            res[tbl].append(col)
        return res

    def setup_statements(self, trn):
        # Find existing tables and columns
        db_columns = self._db_columns(trn)

        # Create tables
        for name, table in self.tables.items():
            if name in db_columns:
                continue
            stmt = Statement(
                "create_table",
                trn.flavor,
                table=name,
            )
            yield stmt()

        # Add columns
        for table in self.tables.values():
            ctypes = table.ctypes(trn)
            for column in table.columns:
                if column in db_columns[table.name]:
                    continue
                stmt = Statement(
                    "add_column",
                    flavor=trn.flavor,
                    table=table.name,
                    column=column,
                    col_def=ctypes[column],
                    not_null=column in table.not_null,
                    fk_table=table.foreign_keys.get(column)
                )
                yield stmt()

        # Add index on natural keys
        for name, table in self.tables.items():
            stmt = Statement(
                "create_unique_index",
                trn.flavor,
                table=name,
                natural_key=table.natural_key,
            )
            yield stmt()

    def create_tables(self, trn=None):
        """
        Create tables, indexes and foreign keys
        """
        trn = trn or Transaction.current
        for stm in self.setup_statements(trn):
            trn.execute(stm)

    def drop(self, trn):
        trn = trn or Transaction.current
        for name in self.tables:
            stmt = Statement("drop_table", trn.flavor, name=name)
            trn.execute(stmt())

    def generate_d2(self):
        tpl = Template(D2_TPL)
        tables = self.tables.values()
        res = "\n".join(tpl.render(table=t) for t in tables)
        return res
