from collections import defaultdict
from pathlib import Path
from io import IOBase

import toml
from jinja2 import Template

from nagra import Statement, Transaction


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

    def __init__(self, transaction=None):
        self.tables = {}
        self.transaction = transaction

    def add(self, name, table):
        if name in self.tables:
            raise RuntimeError(f"Table {name} already in schema!")
        self.tables[name] = table

    def reset(self):
        self.tables = {}

    def get(self, name):
        return self.tables[name]

    @classmethod
    @property
    def default(cls):
        if not cls._default:
            cls._default = Schema()
        return cls._default

    def _db_columns(self, pg_schema="public"):
        res = defaultdict(list)
        stmt = Statement("find_columns", pg_schema=pg_schema)
        for tbl, col in self.execute(stmt()):
            res[tbl].append(col)
        return res

    def setup_statements(self):
        # Find existing tables and columns
        db_columns = self._db_columns()

        # Create tables
        for name, table in self.tables.items():
            if name in db_columns:
                continue
            stmt = Statement(
                "create_table",
                table=name,
            )
            yield stmt()

        # Add columns
        for table in self.tables.values():
            ctypes = table.ctypes()
            for column in table.columns:
                if column in db_columns[table.name]:
                    continue
                stmt = Statement(
                    "add_column",
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
                table=name,
                natural_key=table.natural_key,
            )
            yield stmt()

    def setup(self):
        for stm in self.setup_statements():
            self.execute(stm)

    def drop(self):
        for name in self.tables:
            stmt = Statement("drop_table", name=name)
            self.execute(stmt())

    def execute(self, stm):
         transaction = self.transaction or Transaction.current
         return transaction.execute(stm)

    def generate_d2(self):
        tpl = Template(D2_TPL)
        tables = self.tables.values()
        res = "\n".join(tpl.render(table=t) for t in tables)
        return res


def load_schema(toml_src, create_tables=False, reset=True, schema=Schema.default):
    # Late import to avoid circual deps (should put this code in a
    # "misc" submodule)
    from nagra.table import  Table

    if reset:
        schema.reset()
    # load table definitions
    match toml_src:
        case IOBase():
            content = toml_src.read()
        case Path():
            content =  toml_src.open().read()
        case _:
            content = toml_src
    tables = toml.loads(content)
    for name, info in tables.items():
        Table(name, **info, schema=schema)
    if create_tables:
        schema.setup()
