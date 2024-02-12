import toml
from jinja2 import Template

from nagra import Statement, execute, Transaction


D2_TPL = """
{{table.name}}: {
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

    def __init__(self):
        self.tables = {}

    def add(self, name, table):
        # if name in self.tables: TODO
        #     raise RuntimeError(f"Table {name} already in schema!")
        self.tables[name] = table

    @classmethod
    def get(cls, name):
        return cls.default.tables[name]

    @classmethod
    @property
    def default(cls):
        if not cls._default:
            cls._default = Schema()
        return cls._default

    def setup_statements(self):
        # Create tables
        for name, table in self.tables.items():
            ctypes = {c: t for c, t in table.ctypes().items() if c not in table.foreign_keys}
            stmt = Statement(
                "create_table",
                table=name,
                columns=ctypes,
                not_null=table.not_null,
            )
            yield stmt()

        # Add foreign keys, we need to find existing ones first
        if Transaction.flavor == "sqlite":
            fks = set()
            for name in self.tables:
                stmt = Statement("select_foreign_keys").name(name)
                rows = execute(stmt())
                fks.update(
                    (name, column, other_table)
                    for _, _, other_table, column, *_ in rows
                )
        else:
            stmt = Statement("select_foreign_keys")
            fks = set(execute(stmt()))

        for name, table in self.tables.items():
            ctypes = table.ctypes()
            for column, other_table in table.foreign_keys.items():
                if (name, column, other_table) in fks:
                    continue

                # XXX this loop will fail on composite keys (but do we need it?)
                stmt = Statement(
                    "add_foreign_key",
                    table=name,
                    column=column,
                    col_def=ctypes[column],
                    other_table=other_table,
                    not_nulld=column in table.not_null,
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
            execute(stm)

    def drop(self):
        for name in self.tables:
            stmt = Statement("drop_table", name=name)
            execute(stmt())

    def generate_d2(self):
        tpl = Template(D2_TPL)
        tables = self.tables.values()
        res = "\n".join(tpl.render(table=t) for t in tables)
        return res


def load_schema(toml_file, create_tables=False):
    # Late import to avoid circual deps (should put this code in a
    # "misc" sumodule)
    from nagra.table import  Table

    # load table definitions
    tables = toml.loads(open(toml_file).read())
    for name, info in tables.items():
        Table(name, **info)
    if create_tables:
        Schema.default.setup()
