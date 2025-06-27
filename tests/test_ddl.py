from nagra import Statement
from nagra.utils import strip_lines
from nagra.schema import Schema
from nagra.table import Table


def test_create_table(empty_transaction):
    flavor = empty_transaction.flavor
    schema = Schema()
    Table(
        "my_table",
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
    lines = list(schema.setup_statements(trn=empty_transaction))
    create_table, add_name, add_score, create_idx = map(strip_lines, lines)

    if flavor == "postgresql":
        assert create_table == [
            'CREATE TABLE  "my_table" (',
            '"custom_id" BIGSERIAL PRIMARY KEY',
            ");",
        ]
    else:
        assert create_table == [
            'CREATE TABLE  "my_table" (',
            '"custom_id"  INTEGER PRIMARY KEY',
            ");",
        ]
    assert add_name == ['ALTER TABLE "my_table"', 'ADD COLUMN "name" TEXT NOT NULL']

    assert add_score == [
        'ALTER TABLE "my_table"',
        'ADD COLUMN "score" INTEGER NOT NULL',
        "DEFAULT 0",
    ]
    assert create_idx == [
        'CREATE UNIQUE INDEX my_table_idx ON "my_table" (',
        '"name"',
        ");",
    ]


def test_create_table_pk_is_fk(empty_transaction):
    flavor = empty_transaction.flavor
    schema = Schema()
    Table(  # Regular table
        "concept",
        columns={
            "name": "varchar",
        },
        natural_key=["name"],
        primary_key="concept_id",
        schema=schema,
    )
    Table(  # Table with no primary key and a fk in the nk
        "score",
        columns={
            "concept": "bigint",
            "score": "int",
        },
        primary_key="concept",
        foreign_keys={
            "concept": "concept",
        },
        schema=schema,
    )
    lines = list(schema.setup_statements(trn=empty_transaction))
    if flavor == "postgresql":
        assert lines == [
            'CREATE TABLE  "concept" (\n  "concept_id" BIGSERIAL PRIMARY KEY\n);',
            'CREATE TABLE  "score" (\n'
            '  "concept" BIGINT PRIMARY KEY\n'
            '   CONSTRAINT fk_concept REFERENCES "concept"("concept_id")\n'
            ");",
            'ALTER TABLE "concept"\n ADD COLUMN "name" TEXT NOT NULL',
            'ALTER TABLE "score"\n ADD COLUMN "score" INTEGER NOT NULL',
            'CREATE UNIQUE INDEX concept_idx ON "concept" (\n  "name"\n);',
            'CREATE UNIQUE INDEX score_idx ON "score" (\n  "concept", "score"\n);',
        ]
    else:
        assert lines == [
            'CREATE TABLE  "concept" (\n  "concept_id"  INTEGER PRIMARY KEY\n);',
            'CREATE TABLE  "score" (\n'
            '  "concept"  INTEGER PRIMARY KEY\n'
            '   CONSTRAINT fk_concept REFERENCES "concept"("concept_id")\n'
            ");",
            'ALTER TABLE "concept"\n ADD COLUMN "name" TEXT NOT NULL\n',
            'ALTER TABLE "score"\n ADD COLUMN "score" INTEGER NOT NULL\n',
            'CREATE UNIQUE INDEX concept_idx ON "concept" (\n  "name"\n);',
            'CREATE UNIQUE INDEX score_idx ON "score" (\n  "concept", "score"\n);',
        ]


def test_create_table_no_pk(empty_transaction):
    flavor = empty_transaction.flavor
    schema = Schema()
    Table(  # Regular table
        "concept",
        columns={
            "name": "varchar",
        },
        natural_key=["name"],
        schema=schema,
    )
    Table(  # Table with no primary key and a fk in the nk
        "score",
        columns={
            "concept": "bigint",
            "score": "int",
        },
        natural_key=["concept"],
        primary_key=None,
        foreign_keys={
            "concept": "concept",
        },
        schema=schema,
    )
    lines = list(schema.setup_statements(trn=empty_transaction))
    (
        create_concept,
        create_score_table,
        add_concept_name,
        add_score,
        create_concept_idx,
        create_score_idx,
    ) = map(strip_lines, lines)

    if flavor == "postgresql":
        assert create_concept == [
            'CREATE TABLE  "concept" (',
            '"id" BIGSERIAL PRIMARY KEY',
            ");",
        ]

        assert create_score_table == [
            'CREATE TABLE  "score" (',
            '"concept"  BIGINT NOT NULL',
            'CONSTRAINT fk_concept REFERENCES "concept"("id")',
            ");",
        ]
    else:
        assert create_concept == [
            'CREATE TABLE  "concept" (',
            '"id"  INTEGER PRIMARY KEY',
            ");",
        ]

        assert create_score_table == [
            'CREATE TABLE  "score" (',
            '"concept"  INTEGER NOT NULL',
            'CONSTRAINT fk_concept REFERENCES "concept"("id")',
            ");",
        ]
    assert add_concept_name == [
        'ALTER TABLE "concept"',
        'ADD COLUMN "name" TEXT NOT NULL',
    ]
    assert add_score == ['ALTER TABLE "score"', 'ADD COLUMN "score" INTEGER']
    assert create_concept_idx == [
        'CREATE UNIQUE INDEX concept_idx ON "concept" (',
        '"name"',
        ");",
    ]
    assert create_score_idx == [
        'CREATE UNIQUE INDEX score_idx ON "score" (',
        '"concept"',
        ");",
    ]


def test_create_unique_index():
    stmt = Statement("create_unique_index").table("my_table").natural_key(["name"])
    doc = stmt()
    lines = strip_lines(doc)
    assert lines == [
        'CREATE UNIQUE INDEX my_table_idx ON "my_table" (',
        '"name"',
        ");",
    ]
