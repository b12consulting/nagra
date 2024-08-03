from collections import defaultdict

from nagra import Statement
from nagra.utils import strip_lines
from nagra.schema import Schema
from nagra.table import Table

def test_create_table(empty_transaction):
    flavor = empty_transaction.flavor
    schema = Schema()
    Table("my_table",
          columns={
              "name": "varchar",
              "score": "int",
          },
          natural_key=["name"],
          not_null=["score"],
          default={
              "score": "0",
          },
          primary_key="custom_id",
          schema=schema,
          )
    db_columns = defaultdict(list)
    lines = list(schema.setup_statements(db_columns, flavor))
    create_table, add_name, add_score, create_idx = map(strip_lines, lines)

    if flavor == "postgresql":
        assert create_table == [
            'CREATE TABLE  "my_table" (',
            '"custom_id" BIGSERIAL PRIMARY KEY',
            ');'
        ]
        assert add_name == [
            'ALTER TABLE my_table',
            'ADD COLUMN "name" VARCHAR NOT NULL'
        ]
    else:
        assert create_table == [
            'CREATE TABLE  "my_table" (',
            '"custom_id"  INTEGER PRIMARY KEY',
            ');'
        ]
        assert add_name == [
            'ALTER TABLE my_table',
            'ADD COLUMN "name" TEXT NOT NULL'
        ]

    assert add_score == [
        'ALTER TABLE my_table',
        'ADD COLUMN "score" INTEGER NOT NULL',
        'DEFAULT 0'
    ]
    assert create_idx == [
        'CREATE UNIQUE INDEX IF NOT EXISTS my_table_idx ON "my_table" (',
        '"name"',
        ');'
    ]


def test_create_unique_index():
    stmt = Statement("create_unique_index").table("my_table").natural_key(["name"])
    doc = stmt()
    lines = strip_lines(doc)
    assert lines == [
        'CREATE UNIQUE INDEX IF NOT EXISTS my_table_idx ON "my_table" (',
        '"name"',
        ");",
    ]
